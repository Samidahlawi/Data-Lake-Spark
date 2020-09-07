import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # The function of process_song_data is loads 'song_data' from S3 and then processes it to extracting the songs & artist tables and then will load it to S3
    # So, The parameters: 
    # spark: just a spark session
    # input_data: the location of data where the file is load to process, 
    # The last params output: the locations where will load it after the processing. 

    # get filepath to song data file
    song_data = input_data+"song_data"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # I will use sql so let make a song view 
    df.createOrReplaceTempView("song_data")
    
    # extract columns to create songs table
    # I will use sql cause is more easy to extract columns
    songs_table = spark.sql('''
                            SELECT 
                            DISTINCT
                            sd.song_id,
                            sd.artist_id,
                            sd.sd.title,
                            sd.year,
                            sd.duration
                            FROM song_data st
                            WHERE st.song_id IS NOT NULL
                            ''')
    

    songs_table.write.partitionBy('year','artist').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql('''
                            SELECT 
                            DISTINCT 
                            at.artist_id
                            at.artist_name,
                            at.artist_latitude,
                            at.artist_location,
                            at.artist_longitude
                            FROM song_data at
                            WHERE at.artist_id IS NOT NULL
                            ''')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    # write songs table to parquet files partitioned by year and artist to extracting the songs and artist tables and take it back to load it to S3.
    # So, The parameters: 
    # spark: just a spark session
    # input_data: the location of data where the file is load to process, 
    # The last params output: the locations where will load it after the processing. 
    
    # get filepath to log data file
    log_data =  input_data + 'log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # I will use sql so let make a  view 
    df.createOrReplaceTempView('log_data')

    # extract columns for users table    
    users_table = spark.sql('''
                            SELECT 
                            ut.userId ad user_id
                            ut.firstName as first_name,
                            ut.lastName as last_name,
                            ut.level as level,
                            ut.gender as gender
                            FROM log_data ut
                            WHERE ut.user_id IS NOT NULL
                            ''')
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0))
    # add column called 'timestamp' to df
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    time_table = df.select(
            df.timestamp,
            hour(df.timestamp).alias('hour'),
            dayofmonth(df.timestamp).alias('day'),
            week(df.timestamp).alias('week'),
            month(df.timestamp).alias('month'),
            year(df.timestamp).alias('year'),
            weekofyear(df.timestamp).alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data + "time_tables/")


    # read in song data to use for songplays table
    song_df = spark.read.parquet(input_data+"song_data/*/*/*/*.json")

    # I will use sql so let make a view 
    df.createOrReplaceTempView('song_data')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                                SELECT 
                                row_number() over (ORDER BY sd.artist_id, ld.userId ) songplay_id,
                                to_timestamp(logT.ts/1000) as start_time,
                                month(to_timestamp(logT.ts/1000)) as month,
                                year(to_timestamp(logT.ts/1000)) as year,
                                ld.userId as user_id,
                                ld.level as level,
                                sd.song_id as song_id,
                                sd.artist_id as artist_id,
                                ld.sessionId as session_id,
                                ld.location as location,
                                ld.userAgent as user_agent
                                FROM log_data ld
                                JOIN song_data sd on sd.artist_name = ld.artist and ld.song = sd.title
                                ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays/")


def main():
    # the main function to create spark_session, extract data from S3 and store it back to S3 by using these funciton process_song_data & process_log_data
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dendspark-s3/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

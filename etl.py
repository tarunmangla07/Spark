import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a Spark Session and returns the spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    It reads the song data store as JSON and transforms into dimension table song and artist table and write to S3 as parquet files
    
    Input Parameters:
        spark: Active spark session
        input_data: song_data location
        output_data: S3 bucket location where parquet files are stored
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()
    df.createOrReplaceTempView('song_data')
    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    song_table_output = output_data + "song_table/"
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(song_table_output)

    # extract columns to create artists table
    artists_table =  df.selectExpr(["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artist_table_output = output_data + "artist_table/"
    artists_table.write.parquet(artist_table_output)


def process_log_data(spark, input_data, output_data):
    """
    It reads the log data store as JSON and transforms into Fact table songplays table and
    dimension table users and time table and write to S3 as parquet files
    
    Input Parameters:
        spark: Active spark session
        input_data: song_data location
        output_data: S3 bucket location where parquet files are stored
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    df.printSchema()
    
    # filter by actions for song plays
    logs_dataFrame = df.filter(df.page == 'NextSong')
    logs_dataFrame.createOrReplaceTempView('log_data')

    # extract columns for users table    
    users_table = logs_dataFrame.selectExpr(["userId AS user_id", "firstName AS first_name", "lastName AS last_name", "gender", "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table_output = output_data + "users_table/"
    users_table.write.parquet(users_table_output)

    # create timestamp column from original timestamp column
    df = logs_dataFrame.withColumn('timestamp_new', F.to_timestamp(df.ts/1000))
    
    # create datetime column from original timestamp column
    df_logs = df.withColumn('date_time', F.to_date(df.timestamp_new))
    
    df = df_logs.withColumn("hour", hour("date_time")) \
            .withColumn("day", dayofmonth("date_time")) \
            .withColumn("week", weekofyear("date_time")) \
            .withColumn("month", month("date_time")) \
            .withColumn("year", year("date_time")) \
            .withColumn("weekday", dayofweek("date_time"))
    
    # extract columns to create time table
    time_table_fields = ["date_time as start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_table = df.selectExpr(time_table_fields)
    
    # write time table to parquet files partitioned by year and month
    time_output_table = output_data + "time_table/"
    time_table.write.partitionBy("year", "month").parquet(time_output_table)

    # read in song data to use for songplays table
    song_table_input = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_table_input)
    song_df.printSchema()
    
    df_songplays = df_logs.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title))
    df_songplays.printSchema()

    # extract columns from joined song and log datasets to create songplays table
    df_songplays = df_songplays.withColumn("songplay_id", monotonically_increasing_id()) \
                               .withColumn("month", month("date_time"))
    songplays_table = df_songplays.select(
            col('songplay_id'),
            col('date_time').alias('start_time'),
            col('userId').alias('user_id'),
            col('level'),
            col('song_id'),
            col('artist_id'),
            col('sessionId').alias('session_id'),
            col('location'),
            col('userAgent').alias('user_agent'),
            col('month'),
            col('year')
        ).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.printSchema()
    songplay_output_data = output_data + "songplay_table/"
    songplays_table.write.partitionBy("year", "month").parquet(songplay_output_data)


def main():
    """
    It creates a spark session and loads the data from S3 bucket and processes using spark and
    transforms into set of Facts and dimension tables and finally loads into S3 as parquet files.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

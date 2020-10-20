import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,to_date
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''This function creates and set a sparksession object and return it
        ARGUMENTS: NONE
        RETUNS:
            spark: the spark session object.'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()    
    return spark

def create_spark_context(spark):
    ''' This function creates and sets  the sparkconfiguration.
        ARGUMENTS:
            spark: the spark session object to which the sparkconfiguration will be set
        RETURNS:
            sc: the spark configuration instance.
    '''
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return sc

def process_song_data(spark, input_data, output_data):
    ''' This function reads the song data set, processes the data and write it back
        ARGUMEMNTS:
            spark      : the sparksession object
            input_data : the source of the data to process
            output_data: the location where the processed data should be written back.
        RETURNS:
            NONE
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    print('Schema of the song data frame: ')
    df.printSchema()    

    # extract columns to create songs table
    df.createOrReplaceTempView('data_song_table')
    songs_table=spark.sql(""" SELECT DISTINCT (song_id), title, artist_id, year, duration 
                          FROM data_song_table """)
    
    # write songs table to parquet files partitioned by year and artist    
    print('Start writing songs parquet')
    songs_table= songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs/')
    print('End writing songs parquet')
    
    # extract columns to create artists table
    artists_table=spark.sql("""SELECT DISTINCT (artist_id), artist_name AS name, artist_location AS location,    
                                        artist_latitude AS lattitude, artist_longitude AS longitude
                               FROM data_song_table""")
    
    # write artists table to parquet files
    print('Start writing artists parquet')
    artists_table.write.mode('overwrite').parquet(output_data +'artists/')
    print('End writing artists parquet')

def process_log_data(spark, input_data, output_data):
    ''' This function reads the log data set, processes the data and write it back.
        ARGUMEMNTS:
            spark      : the sparksession object
            input_data : the source of the data to process
            output_data: the location where the processed data should be written back.
        RETURNS:
            NONE
    '''
    # get filepath to log data file    
    #log_data = os.path.join(input_data, 'log_data', '*.json')
    log_data = os.path.join(input_data, 'log_data', '*', '*', '*.json')
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    df.createOrReplaceTempView('dat_log_table')
    users_table=spark.sql("""SELECT DISTINCT(userId) AS user_id, firstName AS first_name, 
                                            lastName as last_name, gender, level
                              FROM dat_log_table""")
       
    # write users table to parquet files
    print("Start writting users parquet")
    users_table.write.mode('overwrite').parquet(output_data + 'users/')
    print("End writting users parquet")
    
    # create timestamp column from original timestamp column
    get_timestamp=udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime=udf(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("datetime", get_datetime(df.timestamp)) \
           .withColumn("hour", hour('timestamp')) \
           .withColumn("day", dayofmonth('timestamp')) \
           .withColumn("week", weekofyear('timestamp')) \
           .withColumn("month", month('timestamp')) \
           .withColumn("year", year('timestamp')) \
           .withColumn("Weekday", date_format('timestamp', 'E'))
    print('Schema of log data with new columns')
    df.printSchema()
    
    # extract columns to create time table
    df.createOrReplaceTempView('date_log_table')
    time_table =spark.sql("""SELECT DISTINCT(datetime) AS start_time, hour, day, month,year, weekday
                        FROM date_log_table""")
        
    # write time table to parquet files partitioned by year and month
    print('Start writting time parquet')
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time/')
    print('End writting time parquet')
    
    # read in song data to use for songplays table
    song_data=os.path.join(input_data, 'song_data', '*', '*', '*', '*.json')
    song_df=spark.read.json(song_data)    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table= df.join(song_df, (df.song==song_df.title)) \
                                    .select( monotonically_increasing_id().alias('songplay_id'), col('userId').alias('user_id'), col('level'), \
                                     col('song_id'), col('artist_id'), col('sessionId').alias('session_id'), (song_df.artist_location).alias('location'), \
                                     col('userAgent').alias('user_agent'), col('datetime').alias('start_time'),col('month'), df.year).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    print('Start writting songplays parquet')
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays/')
    print('End writting songplays parquet')

def main():
    spark = create_spark_session()
    sc=create_spark_context(spark)   
    #input_data ="data/"
    #output_data = "spark-warehouse/"
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://bitibotoaparkdatalake/"
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()

if __name__ == "__main__":
    main()

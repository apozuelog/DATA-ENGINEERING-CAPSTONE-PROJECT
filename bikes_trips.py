# Import libraries
import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    print('Creating spark session...')
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

spark = create_spark_session()
print('... DONE!!!')
print('Load *csv ...')
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3a://aws-logs-004583112324-us-west-2/fp/csv/*.csv")
print('... DONE!!!')

# drop duplicates
bike_trip = df.dropDuplicates()

# --------------------------------
# ------ CREATE TIME TABLE -------
# --------------------------------

# extract columns to create TIME_TABLE
time_table = bike_trip.select('start_time')\
        .withColumn('hour', hour('start_time'))\
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time')) \
        .dropDuplicates()

# --------------------------------
# ----- CREATE STATION TABLE -----
# --------------------------------

s_start = bike_trip.select('start_station_id', 'start_station_name', 'start_station_latitude', 'start_station_longitude').dropDuplicates()
s_end = bike_trip.select('end_station_id', 'end_station_name', 'end_station_latitude', 'end_station_longitude').dropDuplicates()
a = s_start.alias('a')
b = s_end.alias('b')

# JOIN TO ONLY ONE STATION_ID
stations_table = a.join(b, (a.start_station_id == b.end_station_id) & (a.start_station_name == b.end_station_name) & \
                           (a.start_station_latitude == b.end_station_latitude), 'inner')

# extract columns for STATIONS_TABLE
stations_table = stations_table.select('start_station_id', 'start_station_name', 'start_station_latitude', 'start_station_longitude') \
                    .dropDuplicates() \
                    .withColumnRenamed('start_station_id', 'station_id') \
                    .withColumnRenamed('start_station_name', 'station_name') \
                    .withColumnRenamed('start_station_latitude', 'station_lat') \
                    .withColumnRenamed('start_station_longitude', 'station_lon') \
                    .na.drop()

# DROP station_id nulls
stations_table=stations_table.where(col("station_id")!='NULL')

# station_id cast to int
from pyspark.sql.types import IntegerType
stations_table = stations_table.withColumn("station_id", stations_table["station_id"].cast(IntegerType()))

# ------------------------------
# ----- CREATE USERS TABLE -----
# ------------------------------

users_table = bike_trip.select('user_type', 'member_birth_year', 'member_gender', 'bike_share_for_all_trip') \
                    .dropDuplicates() \
                    .withColumnRenamed('user_type', 'user_type') \
                    .withColumnRenamed('member_birth_year', 'birth_year') \
                    .withColumnRenamed('member_gender', 'gender') \
                    .withColumnRenamed('bike_share_for_all_trip', 'share') \
                    .na.drop()

# ------------------------------
# ----- CREATE BIKES TABLE -----
# ------------------------------

bikes_table = bike_trip.select("bike_id")
bikes_table = bikes_table.withColumn("bike_id", bikes_table["bike_id"].cast(IntegerType())).dropDuplicates()

# ----------------------------
# ---- CREATE TRIPS TABLE ----
# ----------------------------

# Create trip_id
from pyspark.sql.functions import monotonically_increasing_id
trips_table = trips_table.select("*").withColumn("trip_id", monotonically_increasing_id())
staging_trips_table = trips_table.select('trip_id', 'start_time', 'end_time', 'duration_sec', 'start_station_id', 'start_station_name', 'start_station_latitude', 'start_station_longitude', \
                                'end_station_id', 'end_station_name', 'end_station_latitude', 'end_station_longitude', 'bike_id', 'user_type', 'member_birth_year', 'member_gender')
staging_trips_table = staging_trips_table.withColumn("end_station_id", staging_trips_table["end_station_id"].cast(IntegerType()))

print('ALL OOOK MAN!!!...')


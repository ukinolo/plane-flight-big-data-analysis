import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, lit, round

miles_to_km = 1.61

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "plane_flight_analysis"
MONGO_COLLECTION = "b2"

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
CLEAN_DATA_PATH = HDFS_NAMENODE + "/clean/"

spark = SparkSession.builder \
    .appName("Get airline statistics") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

df = spark.read.parquet(CLEAN_DATA_PATH)

result = df.groupBy("Airline") \
    .agg(sum("AirTime").alias("TotalAirTimeMinutes"), \
        round(sum("Distance") * lit(miles_to_km), 2).alias("TotalDistanceKilometers"), \
        count("*").alias("NumberOfFlights"))

result.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()
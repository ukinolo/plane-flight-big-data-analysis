import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "plane_flight_analysis"
MONGO_COLLECTION = "b3"

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
CLEAN_DATA_PATH = HDFS_NAMENODE + "/clean/"

spark = SparkSession.builder \
    .appName("Get airport statistics") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

df = spark.read.parquet(CLEAN_DATA_PATH)

destinations = df.groupBy("Dest") \
    .agg(count("*").alias("NumberOfArrivingFlights"))

origins = df.groupBy("Origin") \
    .agg(count("*").alias("NumberOfDepartureFlights"))

airport_stats = destinations.join(origins, destinations["Dest"] == origins["Origin"], how="outer") \
    .select(col("Dest").alias("Airport"), col("NumberOfArrivingFlights"), col("NumberOfDepartureFlights"))

airport_stats.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()
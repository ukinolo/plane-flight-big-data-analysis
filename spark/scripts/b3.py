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

destinations = df.groupBy("Dest", "Year") \
    .agg(count("*").alias("NumberOfArrivingFlights"))

origins = df.groupBy("Origin", "Year") \
    .agg(count("*").alias("NumberOfDepartureFlights"))

d = destinations.alias("d")
o = origins.alias("o")

airport_stats = d.join(
    o,
    (col("d.Dest") == col("o.Origin")) &
    (col("d.Year") == col("o.Year")),
    "outer"
).select(
    col("d.Dest").alias("Airport"),
    col("d.NumberOfArrivingFlights"),
    col("o.NumberOfDepartureFlights"),
    col("d.Year")
)

airport_stats.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, coalesce, sum, lit, to_date, concat_ws

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "plane_flight_analysis"
MONGO_COLLECTION = "b6"

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
CLEAN_DATA_PATH = HDFS_NAMENODE + "/clean/"

spark = SparkSession.builder \
    .appName("Get airport taxi statistics") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

df = spark.read.parquet(CLEAN_DATA_PATH)

destinations = df.groupBy("Dest", "Year", "Month", "DayofMonth") \
    .agg(sum("TaxiOut").alias("TaxiOutTotal")) \
    .withColumn( "FlightDate", to_date(concat_ws("-", "Year", "Month", "DayofMonth")))

origins = df.groupBy("Origin", "Year", "Month", "DayofMonth") \
    .agg(sum("TaxiIn").alias("TaxiInTotal"))

d = destinations.alias("d")
o = origins.alias("o")

result = d.join(
    o,
    (col("d.Dest") == col("o.Origin")) &
    (col("d.Year") == col("o.Year")) & 
    (col("d.Month") == col("o.Month")) &
    (col("d.DayofMonth") == col("o.DayofMonth")),
    "outer"
).select(
    col("d.Dest").alias("Airport"),
    col("d.Year"),
    col("d.FlightDate"),
    col("d.TaxiOutTotal"),
    col("o.TaxiInTotal")
).withColumn(
    "TotalTaxiTime",
    coalesce(col("TaxiOutTotal"), lit(0)) +
    coalesce(col("TaxiInTotal"), lit(0))
)

result.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()
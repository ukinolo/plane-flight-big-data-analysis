import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, sum, concat_ws, to_date, round
from pyspark.sql.window import Window

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "plane_flight_analysis"
MONGO_COLLECTION = "b5"

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
CLEAN_DATA_PATH = HDFS_NAMENODE + "/clean/"

spark = SparkSession.builder \
    .appName("Get airline delay statistics") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

df = spark.read.parquet(CLEAN_DATA_PATH)

daily_delay = df.groupBy("Airline", "Year", "Month", "DayofMonth") \
    .agg(sum("ArrDelayMinutes").alias("TotalArrDelayMinutes"))

daily_delay = daily_delay.withColumn(
    "FlightDate",
    to_date(concat_ws("-", "Year", "Month", "DayofMonth"))
)

# Some airlines from the dataset
# Mesa Airlines Inc.
# Air Wisconsin Airlines Corp
# Allegiant Air
# Comair Inc.
# Hawaiian Airlines Inc.
# United Air Lines Inc.
# Envoy Air
# Trans States Airlines

w = Window.partitionBy("Airline") \
          .orderBy("FlightDate") \
          .rowsBetween(-6, 0)

flights_with_maad = daily_delay.withColumn(
    "MovingAvgArrDelay",
    round(avg("TotalArrDelayMinutes").over(w), 2)
)

result = flights_with_maad.select(
    col("Airline"),
    col("FlightDate"),
    col("Year"),
    col("MovingAvgArrDelay")
)

result.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, when, lit
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_date, concat_ws, date_add
from pyspark.sql.functions import year, month, dayofmonth, hour, unix_timestamp, from_unixtime
from pyspark.sql.functions import concat_ws, lpad
from pyspark.sql.window import Window

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "plane_flight_analysis"
MONGO_COLLECTION = "b7"

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
CLEAN_DATA_PATH = HDFS_NAMENODE + "/clean/"

spark = SparkSession.builder \
    .appName("Get hourly plane numbers") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

df = spark.read.parquet(CLEAN_DATA_PATH)

df = df.withColumn("DepHour", floor(col("DepTime") / 100)) \
       .withColumn("ArrHour", ((floor(col("ArrTime") / 100) + 1) % 24))

df = df.withColumn(
    "IsOvernight",
    when(col("ArrHour") < col("DepHour"), 1).otherwise(0)
)

df = df.withColumn(
    "NewFlightDate",
    to_date(concat_ws("-", "Year", "Month", "DayofMonth"))
)

df = df.withColumn(
    "ArrivalDate",
    when(col("IsOvernight") == 1,
         date_add(col("NewFlightDate"), 1)
    ).otherwise(col("NewFlightDate"))
)

dep_events = df.select(
    col("NewFlightDate").alias("EventDate"),
    col("DepHour").alias("Hour")
).withColumn("delta", lit(1))

arr_events = df.select(
    col("ArrivalDate").alias("EventDate"),
    col("ArrHour").alias("Hour")
).withColumn("delta", lit(-1))

events = dep_events.union(arr_events)

events = events.withColumn(
    "EventTimestamp",
    from_unixtime(
        unix_timestamp("EventDate") + col("Hour") * 3600
    ).cast("timestamp")
)

hourly_events = events.groupBy(
    "EventTimestamp"
).agg(
    spark_sum("delta").alias("delta")
)

w = Window.orderBy("EventTimestamp") \
          .rowsBetween(Window.unboundedPreceding, 0)

result = hourly_events.withColumn(
    "NumberOfPlanes",
    spark_sum("delta").over(w)
)

result = result.select(
    "EventTimestamp",
    "NumberOfPlanes"
)

result.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()
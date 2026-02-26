import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    from_unixtime,
    col,
    when,
    approx_count_distinct
)
from pyspark.sql.types import DoubleType, BooleanType, LongType


spark = SparkSession.builder \
    .appName("Stream 5") \
    .getOrCreate()

CLEAN_DATA_STREAM = "stream-flights-cleaned"
KAFKA_BOOTSTRAP = "kafka:9092"

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "plane_flight_analysis"
MONGO_COLLECTION = "s5"

schema = StructType() \
    .add("callsign", StringType()) \
    .add("origin_country", StringType()) \
    .add("time_position", LongType()) \
    .add("last_contact", LongType()) \
    .add("longitude", DoubleType()) \
    .add("latitude", DoubleType()) \
    .add("altitude", DoubleType()) \
    .add("velocity", DoubleType()) \
    .add("vertical_rate", DoubleType()) \
    .add("heading", DoubleType()) \
    .add("on_ground", BooleanType()) \
    .add("time", LongType())

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", CLEAN_DATA_STREAM) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = (
    df_raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

df_in_air = df_parsed.filter(col("on_ground") == False)

df_with_speed_level = df_in_air.withColumn(
    "speed_level",
    when(col("velocity") <= 50, "slow")
    .when((col("velocity") > 50) & (col("velocity") <= 150), "medium")
    .when((col("velocity") > 150) & (col("velocity") <= 250), "fast")
    .otherwise("very_fast")
)

df_with_time = df_with_speed_level.withColumn(
    "event_time",
    from_unixtime(col("time")).cast("timestamp")
)

result = df_with_time \
    .withWatermark("event_time", "3 minutes") \
    .groupBy(window("event_time", "2 minutes"), col("speed_level")) \
    .agg(
        approx_count_distinct("callsign").alias("num_planes")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("speed_level"),
        col("num_planes")
    )

query = (
    result.writeStream 
        .format("mongodb") 
        .option("checkpointLocation", "/home/checkpoints/flights_stream/s5")
        .option("spark.mongodb.connection.uri", MONGO_URI) \
        .option("spark.mongodb.database", MONGO_DATABASE) \
        .option("spark.mongodb.collection", MONGO_COLLECTION) \
        .outputMode("complete")
        .start()
)

query.awaitTermination()
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    from_unixtime,
    approx_count_distinct
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, LongType

spark = SparkSession.builder \
    .appName("Airplane s1") \
    .getOrCreate()

CLEAN_DATA_STREAM = "stream-flights-cleaned"
KAFKA_BOOTSTRAP = "kafka:9092"

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "plane_flight_analysis"
MONGO_COLLECTION = "s1"

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

df_with_time = df_in_air.withColumn(
    "event_time",
    from_unixtime(col("time")).cast("timestamp")
)

result = df_with_time \
    .withWatermark("event_time", "3 minutes") \
    .groupBy(window("event_time", "2 minutes")) \
    .agg(approx_count_distinct("callsign").alias("planes_in_air")) \
    .select(
        col("window.start").alias("window_start"),
        col("planes_in_air")
    )

query = (
    result.writeStream 
        .format("mongodb") 
        .option("checkpointLocation", "/home/checkpoints/flights_stream/s1")
        .option("spark.mongodb.connection.uri", MONGO_URI) \
        .option("spark.mongodb.database", MONGO_DATABASE) \
        .option("spark.mongodb.collection", MONGO_COLLECTION) \
        .outputMode("complete")
        .start()
)

query.awaitTermination()
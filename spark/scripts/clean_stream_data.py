import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, to_json, struct
from pyspark.sql.functions import col, lower, when
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, LongType

spark = SparkSession.builder \
    .appName("Airplane stream data cleaner") \
    .getOrCreate()

RAW_DATA_STREAM = "stream-flights-raw"
CLEAN_DATA_STREAM = "stream-flights-cleaned"
KAFKA_BOOTSTRAP = "kafka:9092"

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
    .option("subscribe", RAW_DATA_STREAM) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = (
    df_raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

df_us_flights = df_parsed.filter(col("origin_country") == "United States")

df_output = df_us_flights.select(
    to_json(struct("*")).alias("value")
)

query = (
    df_output
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", CLEAN_DATA_STREAM)
    .option("checkpointLocation", "/tmp/checkpoints/flights-us-clean")
    .outputMode("append")
    .option("failOnDataLoss", "false")  
    .start()
)

query.awaitTermination()
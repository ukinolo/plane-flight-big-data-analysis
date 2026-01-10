import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, LongType


spark = SparkSession.builder \
    .appName("Airplane data cleaner") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
RAW_DATA_PATH = HDFS_NAMENODE + "/data/raw/"
DESTINATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

df_raw = spark.read.option("header", True).parquet(RAW_DATA_PATH)

select_columns = [
    # Time Period
    'FlightDate',
    'Year',
    'Quarter',
    'Month',
    'DayofMonth',
    'DayOfWeek',

    # Airline
    'Airline',
    'Tail_Number',
    'DOT_ID_Operating_Airline',
    'IATA_Code_Operating_Airline',

    # Departure performance
    'DepTime',
    'DepDelayMinutes',
    'DepDelay',
    'DepDel15',
    'DepartureDelayGroups',
    'DepTimeBlk',
    'TaxiOut',
    'WheelsOff',

    # Arriving performance
    'ArrTime',
    'ArrDelayMinutes',
    'ArrDelay',
    'ArrDel15',
    'ArrivalDelayGroups',
    'ArrTimeBlk',
    'WheelsOn',
    'TaxiIn',

    # Origin
    'Origin',
    'OriginAirportID',
    'OriginAirportSeqID',
    'OriginCityMarketID',
    'OriginCityName',
    'OriginState',
    'OriginStateFips',
    'OriginStateName',
    'OriginWac',

    # Destination
    'Dest',
    'DestAirportID',
    'DestAirportSeqID',
    'DestCityMarketID',
    'DestCityName',
    'DestState',
    'DestStateFips',
    'DestStateName',
    'DestWac',

    # Cancellations and diversions
    'Cancelled',
    'Diverted',

    # Flight summaries
    'AirTime',
    'Distance',
    'ActualElapsedTime',
    'DistanceGroup',
]

bool_columns = [
    'Cancelled',
    'Diverted',
]

numeric_columns = {
    'DepTime': DoubleType(),
    'DepDelayMinutes': DoubleType(),
    'DepDelay': DoubleType(),
    'ArrTime': DoubleType(),
    'ArrDelayMinutes': DoubleType(),
    'AirTime': DoubleType(),
    'ActualElapsedTime': DoubleType(),
    'Distance': DoubleType(),
    'Year': IntegerType(),
    'Quarter': IntegerType(),
    'Month': IntegerType(),
    'DayofMonth': IntegerType(),
    'DayOfWeek': IntegerType(),
    'DOT_ID_Operating_Airline': IntegerType(),
    'OriginAirportID': IntegerType(),
    'OriginAirportSeqID': IntegerType(),
    'OriginCityMarketID': IntegerType(),
    'OriginStateFips': IntegerType(),
    'OriginWac': IntegerType(),
    'DestAirportID': IntegerType(),
    'DestAirportSeqID': IntegerType(),
    'DestCityMarketID': IntegerType(),
    'DestStateFips': IntegerType(),
    'DestWac': IntegerType(),
    'DepDel15': DoubleType(),
    'DepartureDelayGroups': DoubleType(),
    'TaxiOut': DoubleType(),
    'WheelsOff': DoubleType(),
    'WheelsOn': DoubleType(),
    'TaxiIn': DoubleType(),
    'ArrDelay': DoubleType(),
    'ArrDel15': DoubleType(),
    'ArrivalDelayGroups': DoubleType(),
    'DistanceGroup': IntegerType(),
}

df_cleaned = df_raw.select(select_columns)

df_cleaned = df_cleaned.dropna()


for name, dtype in numeric_columns.items():
    df_cleaned = df_cleaned.withColumn(name, col(name).cast(dtype))

for name in bool_columns:
    df_cleaned = df_cleaned.withColumn(
        name,
        when(
            lower(col(name).cast("string")).isin("true", "1", "t", "yes", "y"),
            True
        ).otherwise(False).cast(BooleanType())
    )

df_cleaned.repartition(100)
df_cleaned.write.mode("overwrite").parquet(DESTINATION_DATA_PATH)

spark.stop()
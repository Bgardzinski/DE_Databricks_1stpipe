from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, DateType

from src.config.config_file import BRONZE_INPUT_PATH_PRIC, BRONZE_SCHEMA_LOC_PRIC, BRONZE_CHECKPOINT_LOC_PRIC, BRONZE_OUTPUT_PATH_PRIC


def read_prices_stream():
    schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("day_high", DoubleType(), True),
    StructField("day_low", DoubleType(), True),
    StructField("previous_close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("extract_time", TimestampType(), True),
    StructField("extract_date", DateType(), True),
    StructField("year", IntegerType(), True),  
    StructField("month", IntegerType(), True), 
    StructField("day", IntegerType(), True) 
])

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", BRONZE_SCHEMA_LOC_PRIC)
        .option("header", "true")
        .schema(schema)
        .load(BRONZE_INPUT_PATH_PRIC)
    )

    return df

def write_prices(df):
    query = (
        df.writeStream \
    .format("delta") \
    .option("checkpointLocation", BRONZE_CHECKPOINT_LOC_PRIC) \
    .trigger(availableNow=True) \
    .toTable(BRONZE_OUTPUT_PATH_PRIC) \
    .awaitTermination() )


df = read_prices_stream()
write_prices(df)
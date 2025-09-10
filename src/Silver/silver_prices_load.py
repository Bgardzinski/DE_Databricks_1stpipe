# Databricks notebook source
from src.utils.common_functions import (
    filter_not_null, cast_ints, 
    drop_duplicates_on_columns
)
from src.config.config_file import (
    BRONZE_OUTPUT_CATALOG_PRIC, 
    SILVER_CHECKPOINT_LOC_PRIC, 
    SILVER_OUTPUT_CATALOG_PRIC
)

fundamentals_int_columns = [
    "market_cap"]

non_null_columns = [
    "symbol",
    "open",
    "extract_time"]


def read_prices():
    return spark.readStream.table(BRONZE_OUTPUT_CATALOG_PRIC)

def write_prices(df):
    df.writeStream.format("delta") \
    .option("checkpointLocation", SILVER_CHECKPOINT_LOC_PRIC) \
    .trigger(availableNow=True) \
    .toTable(SILVER_OUTPUT_CATALOG_PRIC) \
    .awaitTermination()


df = read_prices()

#Cleaning and Type correction

df = filter_not_null(df, non_null_columns)
df = cast_ints(df, fundamentals_int_columns)
#watermark
df = df.withWatermark("extract_time", "2 days")
#drop duplicates
df = drop_duplicates_on_columns(df, non_null_columns)

write_prices(df)

# COMMAND ----------

df = spark.sql("SELECT * FROM hive_metastore.dev_silver.prices")
pandas_df = df.limit(100).toPandas()
display(pandas_df)

# COMMAND ----------


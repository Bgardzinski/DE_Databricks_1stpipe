# Databricks notebook source
from src.utils.common_functions import (
    filter_not_null, cast_ints, 
    drop_duplicates_on_columns
)
from src.utils.expected_rows import expected_rows
from src.config.config_file import (
    BRONZE_OUTPUT_CATALOG_PRIC, 
    SILVER_CHECKPOINT_LOC_PRIC, 
    SILVER_OUTPUT_CATALOG_PRIC,
    KEY_COLUMNS_PRIC,
    LOGGING_OUTPUT_CATALOG_PRIC,
    VALUE_CONSTRAINS_PRIC
)
from src.DQ_checks.value_constrains_check import column_non_negative_check_log
from src.DQ_checks.row_count_check import row_count_exact_check_log
from src.DQ_checks.null_check import null_check_log
from src.DQ_checks.duplicate_rows_check import multi_column_uniqueness_check_log
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


fundamentals_int_columns = [
    "market_cap"]


def read_prices():
    return spark.readStream.table(BRONZE_OUTPUT_CATALOG_PRIC)

def write_prices(df):
        #watermark
        df = df.withWatermark("extract_time", "2 days")
    def process_batch(batch_df, batch_id):
       
        df = batch_df
        column_non_negative_check_log(df, SILVER_OUTPUT_CATALOG_PRIC, "Silver", LOGGING_OUTPUT_CATALOG_PRIC, VALUE_CONSTRAINS_PRIC)
        nb_expected_rows = expected_rows(df)
        row_count_exact_check_log(df, SILVER_OUTPUT_CATALOG_PRIC, "Silver", LOGGING_OUTPUT_CATALOG_PRIC, nb_expected_rows)
        multi_column_uniqueness_check_log(df, SILVER_OUTPUT_CATALOG_PRIC, "Silver", LOGGING_OUTPUT_CATALOG_PRIC, KEY_COLUMNS_PRIC)
        null_check_log(df, SILVER_OUTPUT_CATALOG_PRIC, "Silver", LOGGING_OUTPUT_CATALOG_PRIC, KEY_COLUMNS_PRIC)

        #Cleaning and Type correction
        df = filter_not_null(df, KEY_COLUMNS_PRIC)
        df = cast_ints(df, fundamentals_int_columns)

        #drop duplicates
        df = drop_duplicates_on_columns(df, KEY_COLUMNS_PRIC)

        # Write the batch to Silver table
        df.write.format("delta").mode("append").saveAsTable(SILVER_OUTPUT_CATALOG_PRIC)

    # Start streaming with foreachBatch
    query = df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", SILVER_CHECKPOINT_LOC_PRIC) \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()


df = read_prices()

write_prices(df)

# COMMAND ----------

df = spark.sql("SELECT * FROM hive_metastore.dev_silver.prices")
pandas_df = df.limit(100).toPandas()
display(pandas_df)

# COMMAND ----------

dbutils.fs.mv(
    "/FileStore/tables/Symbols-1.txt",
    "/FileStore/tables/Symbols.txt",
    True  # overwrite if it exists
)

# COMMAND ----------


# Databricks notebook source
from src.utils.common_functions import (
    filter_not_null, cast_ints, 
    drop_duplicates_on_columns
)

from pyspark.sql.functions import when, col

from src.config.config_file import (
    BRONZE_OUTPUT_CATALOG_FUND, 
    SILVER_CHECKPOINT_LOC_FUND, 
    SILVER_OUTPUT_CATALOG_FUND
)

fundamentals_int_columns = [
    "fullTimeEmployees",
    "marketCap",
    "enterpriseValue",
    "totalRevenue",
    "netIncomeToCommon",
    "ebitda"]

non_null_columns = [
    "symbol",
    "shortName",
    "extract_time"]

def read_silver():
    return spark.readStream.table(BRONZE_OUTPUT_CATALOG_FUND)

def write_silver(df):
    df.writeStream.format("delta") \
    .option("checkpointLocation", SILVER_CHECKPOINT_LOC_FUND) \
    .trigger(availableNow=True) \
    .toTable(SILVER_OUTPUT_CATALOG_FUND) \
    .awaitTermination()

def fundamentals_enrichments(df):
    df = df.withColumn("marketCapCategory",
        when(col("marketCap") >= 200_000_000_000, "Mega Cap")
        .when(col("marketCap") >= 10_000_000_000, "Large Cap")
        .when(col("marketCap") >= 2_000_000_000, "Mid Cap")
        .otherwise("Small Cap")
    )

    df = df.withColumn("employeeCountCategory",
        when(col("fullTimeEmployees") >= 10000, "Large Workforce")
        .when(col("fullTimeEmployees") >= 1000, "Medium Workforce")
        .otherwise("Small Workforce")
    )

    df = df.withColumn("profitMarginCategory",
        when(col("profitMargins") >= 0.2, "High Margin")
        .when(col("profitMargins") >= 0.05, "Moderate Margin")
        .otherwise("Low Margin")
    )

    df = df.withColumn("revenueGrowthCategory",
        when(col("revenueGrowth") >= 0.15, "High Growth")
        .when(col("revenueGrowth") >= 0.05, "Moderate Growth")
        .otherwise("Low or Negative Growth")
    )
    return df


df = read_silver()

#Cleaning and Type correction

df = filter_not_null(df, non_null_columns)
df = cast_ints(df, fundamentals_int_columns)
#setting Watermark
df = df.withWatermark("extract_time", "2 days")
#dropping duplicates
df = drop_duplicates_on_columns(df, non_null_columns)
#new derived columns
df = fundamentals_enrichments(df)


write_silver(df)



# COMMAND ----------

df = spark.sql("SELECT * FROM hive_metastore.dev_silver.daily_price_aggregates")
pandas_df = df.limit(100).toPandas()
display(pandas_df)

# COMMAND ----------

spark.read.table(f"{env}_bronze.fundamentals").show()

# COMMAND ----------


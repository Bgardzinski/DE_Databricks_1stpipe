# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    LongType, TimestampType, DateType
)
from src.config.config_file import (
    BRONZE_INPUT_PATH_FUND,
    BRONZE_SCHEMA_LOC_FUND,
    BRONZE_CHECKPOINT_LOC_FUND,
    BRONZE_OUTPUT_CATALOG_FUND
)
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ------------------------------
# Read fundamentals stream
# ------------------------------
def read_fundamentals_stream():
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("country", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("fullTimeEmployees", StringType(), True),
        StructField("marketCap", StringType(), True),
        StructField("enterpriseValue", StringType(), True),
        StructField("totalRevenue", StringType(), True),
        StructField("netIncomeToCommon", StringType(), True),
        StructField("profitMargins", DoubleType(), True),
        StructField("revenueGrowth", DoubleType(), True),
        StructField("ebitda", StringType(), True),
        StructField("enterpriseToRevenue", DoubleType(), True),
        StructField("enterpriseToEbitda", DoubleType(), True),
        StructField("bookValue", DoubleType(), True),
        StructField("priceToBook", DoubleType(), True),
        StructField("trailingPE", DoubleType(), True),
        StructField("forwardPE", DoubleType(), True),
        StructField("trailingEps", DoubleType(), True),
        StructField("forwardEps", DoubleType(), True),
        StructField("returnOnAssets", DoubleType(), True),
        StructField("returnOnEquity", DoubleType(), True),
        StructField("earningsQuarterlyGrowth", DoubleType(), True),
        StructField("ipoExpectedDate", StringType(), True),
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
        .option("cloudFiles.schemaLocation", BRONZE_SCHEMA_LOC_FUND)  # imported from config
        .option('header', 'true')
        .schema(schema)
        .load(BRONZE_INPUT_PATH_FUND)  # imported from config
    )
    return df


# ------------------------------
# Write fundamentals stream
# ------------------------------
def write_fundamentals(df):
    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", BRONZE_CHECKPOINT_LOC_FUND)  # imported from config
        .trigger(availableNow=True)
        .toTable(BRONZE_OUTPUT_CATALOG_FUND)  # imported from config
        .awaitTermination()
    )
    return query


# ------------------------------
# Execute stream
# ------------------------------

df = read_fundamentals_stream()
write_fundamentals(df)


# COMMAND ----------

df = spark.sql("SELECT * FROM hive_metastore.dev_bronze.fundamentals")
pandas_df = df.limit(100).toPandas()
display(pandas_df)

# COMMAND ----------

from src.config.config_file import (
    env,
    BRONZE_INPUT_PATH_FUND,
    BRONZE_SCHEMA_LOC_FUND,
    BRONZE_CHECKPOINT_LOC_FUND,
    BRONZE_OUTPUT_CATALOG_FUND
)


df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load(BRONZE_INPUT_PATH_FUND)
)

df.show(5)

# COMMAND ----------

display(dbutils.fs.ls(BRONZE_INPUT_PATH_FUND))

# COMMAND ----------

print(BRONZE_INPUT_PATH_PRIC)
print(BRONZE_INPUT_PATH_FUND)

# COMMAND ----------

daily-fundamentals
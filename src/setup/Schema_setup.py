# Databricks notebook source
from pyspark.sql import SparkSession
from src.config.config_file import (
    env, CATALOG,
    BRONZE_OUTPUT_CATALOG_FUND, BRONZE_OUTPUT_CATALOG_PRIC,
    BRONZE_OUTPUT_PATH_FUND, BRONZE_OUTPUT_PATH_PRIC,
    SILVER_OUTPUT_CATALOG_FUND, SILVER_OUTPUT_CATALOG_PRIC, SILVER_OUTPUT_CATALOG_AGG,
    SILVER_OUTPUT_PATH_FUND, SILVER_OUTPUT_PATH_PRIC, SILVER_OUTPUT_PATH_AGG,
    GOLD_OUTPUT_CATALOG_IND, GOLD_OUTPUT_PATH_IND,
    LOGGING_OUTPUT_PATH_PRIC, LOGGING_OUTPUT_CATALOG_PRIC
)

spark = SparkSession.builder.getOrCreate()


# ------------------------------
# SCHEMAS
# ------------------------------
def create_bronze_schema():
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env}_bronze")


def create_silver_schema():
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env}_silver")


def create_gold_schema():
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env}_gold")

def create_logging_schema():
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env}_logging")

# ------------------------------
# LOG TABLES
# ------------------------------

def create_dq_log_table_partitioned():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOGGING_OUTPUT_CATALOG_PRIC} (
        table_path STRING,
        layer STRING,
        batch_id STRING,
        check_name STRING,
        check_type STRING,
        expected_value STRING,
        observed_value STRING,
        status STRING,
        error_message STRING,
        check_timestamp TIMESTAMP,
        year INT,
        month INT,
        day INT
    )
    USING DELTA
    PARTITIONED BY (year, month, day)
    LOCATION '{LOGGING_OUTPUT_PATH_PRIC}'
    """)

# ------------------------------
# BRONZE TABLES
# ------------------------------
def create_fundamental_table():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_OUTPUT_CATALOG_FUND} (
        symbol STRING,
        shortName STRING,
        sector STRING,
        industry STRING,
        country STRING,
        currency STRING,
        fullTimeEmployees DOUBLE,
        marketCap DOUBLE,
        enterpriseValue DOUBLE,
        totalRevenue DOUBLE,
        netIncomeToCommon DOUBLE,
        profitMargins DOUBLE,
        revenueGrowth DOUBLE,
        ebitda DOUBLE,
        enterpriseToRevenue DOUBLE,
        enterpriseToEbitda DOUBLE,
        bookValue DOUBLE,
        priceToBook DOUBLE,
        trailingPE DOUBLE,
        forwardPE DOUBLE,
        trailingEps DOUBLE,
        forwardEps DOUBLE,
        returnOnAssets DOUBLE,
        returnOnEquity DOUBLE,
        earningsQuarterlyGrowth DOUBLE,
        ipoExpectedDate STRING,
        extract_time TIMESTAMP,
        extract_date DATE,
        year INT,
        month INT,
        day INT
    )
    USING DELTA
    PARTITIONED BY (year, month, day)
    LOCATION '{BRONZE_OUTPUT_PATH_FUND}'
    """)


def create_prices_table():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_OUTPUT_CATALOG_PRIC} (
        symbol STRING,
        current_price DOUBLE,
        open DOUBLE,
        day_high DOUBLE,
        day_low DOUBLE,
        previous_close DOUBLE,
        volume DOUBLE,
        market_cap DOUBLE,
        extract_time TIMESTAMP,
        extract_date DATE,
        year INT,
        month INT,
        day INT
    )
    USING DELTA
    PARTITIONED BY (year, month, day)
    LOCATION '{BRONZE_OUTPUT_PATH_PRIC}'
    """)


# ------------------------------
# SILVER TABLES
# ------------------------------
def create_silver_fundamentals():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_OUTPUT_CATALOG_FUND} (
        symbol STRING,
        shortName STRING,
        sector STRING,
        industry STRING,
        country STRING,
        currency STRING,
        fullTimeEmployees BIGINT,
        marketCap BIGINT,
        enterpriseValue BIGINT,
        totalRevenue BIGINT,
        netIncomeToCommon BIGINT,
        profitMargins DOUBLE,
        revenueGrowth DOUBLE,
        ebitda BIGINT,
        enterpriseToRevenue DOUBLE,
        enterpriseToEbitda DOUBLE,
        bookValue DOUBLE,
        priceToBook DOUBLE,
        trailingPE DOUBLE,
        forwardPE DOUBLE,
        trailingEps DOUBLE,
        forwardEps DOUBLE,
        returnOnAssets DOUBLE,
        returnOnEquity DOUBLE,
        earningsQuarterlyGrowth DOUBLE,
        ipoExpectedDate STRING,
        extract_time TIMESTAMP,
        extract_date DATE,
        year INT,
        month INT,
        day INT,
        marketCapCategory STRING,
        employeeCountCategory STRING,
        profitMarginCategory STRING,
        revenueGrowthCategory STRING
    )
    USING DELTA
    PARTITIONED BY (year, month, day)
    LOCATION '{SILVER_OUTPUT_PATH_FUND}'
    """)


def create_silver_prices():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_OUTPUT_CATALOG_PRIC} (
        symbol STRING,
        current_price DOUBLE,
        open DOUBLE,
        day_high DOUBLE,
        day_low DOUBLE,
        previous_close DOUBLE,
        volume DOUBLE,
        market_cap BIGINT,
        extract_time TIMESTAMP,
        extract_date DATE,
        year INT,
        month INT,
        day INT
    )
    USING DELTA
    PARTITIONED BY (year, month, day)
    LOCATION '{SILVER_OUTPUT_PATH_PRIC}'
    """)


def create_daily_aggregates():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_OUTPUT_CATALOG_AGG} (
        symbol STRING,
        date DATE,
        open DOUBLE,
        day_high DOUBLE,
        day_low DOUBLE,
        avg_price DOUBLE,
        volume DOUBLE,
        year INT,
        month INT
    )
    USING DELTA
    PARTITIONED BY (year, month)
    LOCATION '{SILVER_OUTPUT_PATH_AGG}'
    """)


# ------------------------------
# GOLD TABLES
# ------------------------------
def create_daily_indicators():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_OUTPUT_CATALOG_IND} (
        symbol STRING,
        date DATE,
        open DOUBLE,
        day_high DOUBLE,
        day_low DOUBLE,
        avg_price DOUBLE,
        volume DOUBLE,
        year INT,
        month INT,

        -- Moving Averages
        ma_12 DOUBLE,
        ma_26 DOUBLE,
        ma_60 DOUBLE,

        -- Exponential Moving Averages
        ema_12 DOUBLE,
        ema_26 DOUBLE,
        ema_60 DOUBLE,

        -- RSI
        rsi_12 DOUBLE,
        rsi_26 DOUBLE,
        rsi_60 DOUBLE,

        -- Bollinger Bands 12
        bb_upper_12 DOUBLE,
        bb_lower_12 DOUBLE,
        bb_width_12 DOUBLE,

        -- Bollinger Bands 26
        bb_upper_26 DOUBLE,
        bb_lower_26 DOUBLE,
        bb_width_26 DOUBLE,

        -- Bollinger Bands 60
        bb_upper_60 DOUBLE,
        bb_lower_60 DOUBLE,
        bb_width_60 DOUBLE,

        -- MACD and related
        macd DOUBLE,
        signal DOUBLE,
        macd_hist DOUBLE,

        -- OBV
        obv DOUBLE
    )
    USING DELTA
    PARTITIONED BY (year, month)
    LOCATION '{GOLD_OUTPUT_PATH_IND}'
    """)


# ------------------------------
# EXECUTION
# ------------------------------
create_logging_schema()
create_dq_log_table_partitioned()

create_bronze_schema()
create_silver_schema()
create_gold_schema()

create_fundamental_table()
create_prices_table()

create_silver_fundamentals()
create_silver_prices()
create_daily_aggregates()

create_daily_indicators()


# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS hive_metastore.dev_logging.dq_log;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS hive_metastore.dev_silver.prices;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS hive_metastore.dev_silver.fundamentals;
# MAGIC

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS hive_metastore.dev_bronze.prices;
# MAGIC

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS hive_metastore.dev_bronze.fundamentals;
# MAGIC

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS hive_metastore.dev_silver.daily_price_aggregates;

# COMMAND ----------

spark.conf.set("fs.azure.account.key.stockstoragebg1.dfs.core.windows.net", "q1HDzmQpreCTkJN9r3MMSGNUy9uKPv1PG6vn32FPt/KvoYfYcGH3s1XvQMUwaoT5L+PDmAMMKFG4+AStKjJ7jw==")

# COMMAND ----------


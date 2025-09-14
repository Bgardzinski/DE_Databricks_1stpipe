# ------------------------------
# Config file for environment, paths, and catalog/table references
# ------------------------------

# ENVIRONMENT
env = "dev"   

# CATALOG
CATALOG = "hive_metastore"

# GENERAL PATH
GENERAL_PATH = f"abfss://stockstoragebg1@stockstoragebg1.dfs.core.windows.net/{env}"

#CONTAINER 
CONTAINER_URL = "stockstoragebg1"

#CONNECTION_STRING
CONNECTION_STRING = 0

#KEY COLUMNS
KEY_COLUMNS_PRIC = ["symbol", "current_price", "extract_time"]

VALUE_CONSTRAINS_PRIC = ["current_price"]

# ------------------------------
# RAW
# ------------------------------

RAW_INPUT_PATH_PRIC = "dev/raw-data/hourly_prices"
RAW_INPUT_PATH_FUND = "dev/raw-data/daily_fundamentals"

# ------------------------------
# LOGGING
# ------------------------------

LOGGING_OUTPUT_PATH_PRIC = f"{GENERAL_PATH}/logging/dq_log"
LOGGING_OUTPUT_CATALOG_PRIC = f"{CATALOG}.{env}_logging.dq_log"

# ------------------------------
# BRONZE
# ------------------------------

# INPUT
BRONZE_INPUT_PATH_PRIC = f"{GENERAL_PATH}/raw-data/hourly_prices"
BRONZE_INPUT_PATH_FUND = f"{GENERAL_PATH}/raw-data/daily_fundamentals"

# SCHEMA & CHECKPOINT
BRONZE_SCHEMA_LOC_PRIC = f"{GENERAL_PATH}/raw-data/_schema_autoloader/bronze_prices/"
BRONZE_SCHEMA_LOC_FUND = f"{GENERAL_PATH}/raw-data/_schema_autoloader/bronze_fundamentals/"
BRONZE_CHECKPOINT_LOC_PRIC = f"{GENERAL_PATH}/checkpoints/bronze_prices_stream/"
BRONZE_CHECKPOINT_LOC_FUND = f"{GENERAL_PATH}/checkpoints/bronze_fundamentals_stream/"

# OUTPUT - CATALOG
BRONZE_OUTPUT_CATALOG_PRIC = f"{CATALOG}.{env}_bronze.prices"
BRONZE_OUTPUT_CATALOG_FUND = f"{CATALOG}.{env}_bronze.fundamentals"

# OUTPUT - PATH
BRONZE_OUTPUT_PATH_PRIC = f"{GENERAL_PATH}/bronze/prices/"
BRONZE_OUTPUT_PATH_FUND = f"{GENERAL_PATH}/bronze/fundamentals/"


# ------------------------------
# SILVER
# ------------------------------

# SCHEMA
SILVER_CHECKPOINT_LOC_PRIC = f"{GENERAL_PATH}/checkpoints/silver_prices_stream/"
SILVER_CHECKPOINT_LOC_FUND = f"{GENERAL_PATH}/checkpoints/silver_fundamentals_stream/"
SILVER_CHECKPOINT_LOC_AGG = f"{GENERAL_PATH}/checkpoints/silver_dailyagg_stream/"

# OUTPUT - CATALOG
SILVER_OUTPUT_CATALOG_FUND = f"{CATALOG}.{env}_silver.fundamentals"
SILVER_OUTPUT_CATALOG_PRIC = f"{CATALOG}.{env}_silver.prices"
SILVER_OUTPUT_CATALOG_AGG  = f"{CATALOG}.{env}_silver.daily_price_aggregates"

# OUTPUT - PATH
SILVER_OUTPUT_PATH_FUND = f"{GENERAL_PATH}/silver/fundamentals/"
SILVER_OUTPUT_PATH_PRIC = f"{GENERAL_PATH}/silver/prices/"
SILVER_OUTPUT_PATH_AGG  = f"{GENERAL_PATH}/silver/daily_price_aggregates/"

# ------------------------------
# GOLD
# ------------------------------

GOLD_CHECKPOINT_LOC_IND = f"{GENERAL_PATH}/checkpoints/gold_indicators_stream/"

GOLD_OUTPUT_CATALOG_IND = f"{CATALOG}.{env}_gold.daily_price_indicators"
GOLD_OUTPUT_PATH_IND   = f"{GENERAL_PATH}/gold/daily_price_indicators/"

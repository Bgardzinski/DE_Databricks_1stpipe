# Databricks notebook source
from pyspark.sql.functions import (
    to_date, first, max, min, 
    avg, last, sum, year, 
    month, dayofmonth, window
)
from delta.tables import DeltaTable
from src.config.config_file import (
    SILVER_OUTPUT_CATALOG_PRIC, 
    SILVER_OUTPUT_CATALOG_AGG, 
    SILVER_CHECKPOINT_LOC_AGG
)

# Read streaming data from silver table
def readStream():
    df_stream = spark.readStream.table(SILVER_OUTPUT_CATALOG_PRIC) \
        .withWatermark("extract_time", "5 day")
    return df_stream



# Aggregate daily OHLCV per symbol
def aggregate_daily_prices(df_stream):
    daily_agg = (
        df_stream
        .withColumn("extract_date", to_date("extract_time"))
        .groupBy("symbol", "year", "month", "extract_date" )
        .agg(
            max("open").alias("open"),
            max("day_high").alias("day_high"),
            min("day_low").alias("day_low"),
            avg("current_price").alias("avg_price"),
            max("volume").alias("volume"),
        )
        .withColumnRenamed("extract_date", "date")
    )
    return daily_agg


def write_to_delta(daily_agg):
    def upsert_to_delta(batch_df, batch_id):
        try:
            delta_table = DeltaTable.forName(spark, SILVER_OUTPUT_CATALOG_AGG)
            delta_table.alias("target").merge(
                batch_df.alias("source"),
                "target.symbol = source.symbol AND target.date = source.date"
            ).whenMatchedUpdate(
                set={
                    "open": "source.open",
                    "day_high": "source.day_high",
                    "day_low": "source.day_low",
                    "avg_price": "source.avg_price",
                    "volume": "source.volume"
                }) \
            .whenNotMatchedInsertAll() \
            .execute()
        except Exception as e:
            print(f"Error in batch {batch_id}: {e}")
            batch_df.show()
            raise


    query = (daily_agg.writeStream
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", SILVER_CHECKPOINT_LOC_AGG)
        .outputMode("update")
        .trigger(availableNow=True)
        .start())
    query.awaitTermination()



stream = readStream()
daily_agg = aggregate_daily_prices(stream)
write_to_delta(daily_agg)



# COMMAND ----------

df = spark.sql("SELECT * FROM hive_metastore.dev_silver.daily_price_aggregates")
pandas_df = df.limit(100).toPandas()
display(pandas_df)

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------


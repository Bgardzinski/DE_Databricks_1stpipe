# Databricks notebook source
from src.config.config_file import SILVER_OUTPUT_CATALOG_AGG, GOLD_CHECKPOINT_LOC_IND, GOLD_OUTPUT_CATALOG_IND
from src.utils.technical_indicators import moving_average, exponential_moving_average, calculate_rsi, calculate_bollinger_bands, calculate_macd, calculate_obv
from src.utils.load_history_for_calculations import read_prices, take_latest_row, add_prev_prefix, run_all_indicators
from pyspark.sql.functions import (
    lit, struct, avg, col, row_number, when, stddev, lag, sum as spark_sum, max as spark_max
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from functools import reduce
from pyspark.sql.functions import date_sub, current_date
import time
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


output_columns = ['symbol', 'date', 'open', 'day_high', 'day_low', 'avg_price', 'volume', 'year', 'month', 'ma_12', 'ma_26', 'ma_60', 'ema_12', 'ema_26', 'ema_60', 'rsi_12', 'rsi_26', 'rsi_60', 'bb_upper_12', 'bb_lower_12', 'bb_width_12', 'bb_upper_26', 'bb_lower_26', 'bb_width_26', 'bb_upper_60', 'bb_lower_60', 'bb_width_60', 'macd', 'signal', 'macd_hist', 'obv']
windows = [60,26,12]

def readStream_prices():
    return spark.readStream.table(SILVER_OUTPUT_CATALOG_AGG)


def write_to_delta(input_df, input_table, output_table, granulity):
    def upsert_to_delta(batch_df, batch_id, spark):

        distinct_dates = [row['date'] for row in batch_df.select('date').distinct().collect()]
        distinct_dates = sorted(distinct_dates)
        distinct_dates_count = batch_df.select("date").distinct().count()

        if distinct_dates_count < 2:
            df_filtered = read_prices(input_table,distinct_dates[0], granulity, spark)
            df_filtered.cache()
    
            latest_records = take_latest_row(output_table, distinct_dates[0], granulity, spark)
            latest_records = add_prev_prefix(latest_records)
            result_df = batch_df.join(latest_records, on="symbol", how="left")

            batch_df = run_all_indicators(df_filtered, result_df, windows, output_columns)

        else:
            
            dfs = []
            for single_date in distinct_dates:

                batch_by_date = batch_df.filter(col("date") == single_date)
                df_filtered = read_prices(input_table, single_date, granulity)
                df_filtered.cache()
                if 'latest_records' not in locals():
                    latest_records = take_latest_row(output_table, single_date, granulity)
                latest_records = add_prev_prefix(latest_records)
                result_df = batch_by_date.join(latest_records, on="symbol", how="left")
                df_single = run_all_indicators(df_filtered, result_df, windows, output_columns)
                latest_records = df_single
                dfs.append(df_single)
                
            if dfs:
                batch_df = reduce(lambda a, b: a.unionByName(b), dfs)
            else:
                return


        delta_table = DeltaTable.forName(spark, output_table)
        delta_table.alias("target").merge(
            batch_df.alias("source"),
            "target.symbol = source.symbol AND target.date = source.date"
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


    query = (input_df.writeStream
        .foreachBatch(lambda batch_df, batch_id: upsert_to_delta(batch_df, batch_id, spark))
        .option("checkpointLocation", GOLD_CHECKPOINT_LOC_IND)
        .outputMode("update")
        .trigger(availableNow=True)
        .start())
    query.awaitTermination()





df_streamed = readStream_prices()
write_to_delta(df_streamed, SILVER_OUTPUT_CATALOG_AGG, GOLD_OUTPUT_CATALOG_IND, 'daily')


# COMMAND ----------

df = spark.sql("SELECT * FROM hive_metastore.dev_gold.daily_price_indicators")
pandas_df = df.limit(100).toPandas()
display(pandas_df)

# COMMAND ----------

display(spark.readStream.table(f"{env}_silver.daily_price_aggregates"))

# COMMAND ----------


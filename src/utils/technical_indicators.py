# Databricks notebook source
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    avg,
    stddev,
    lag,
    sum as spark_sum,
    max as spark_max,
)
from pyspark.sql.window import Window


def moving_average(df_input, df_output, window_sizes):
    agg_exprs = []
    for ws in window_sizes:
        agg_exprs.append(avg(when(col("rn") <= ws, col("avg_price"))).alias(f"ma_{ws}"))

    ma_df = df_input.groupBy("symbol").agg(*agg_exprs)
    df_output = df_output.join(ma_df, on="symbol", how="left")
    return df_output


def exponential_moving_average(df_output, window_sizes):

    for ws in window_sizes:
        k = 2 / (ws + 1)
        ema_col = f"ema_{ws}"
        prev_col = f"prev_{ema_col}"
        ma_col = f"ma_{ws}"

        df_output = df_output.withColumn(
            ema_col,
            when(
                col(prev_col).isNull(),  # First day fallback to MA
                col(ma_col)
            ).otherwise(
                col(prev_col) * (1 - k) + col("avg_price") * k
            )
        )

    return df_output


def calculate_rsi(df_input, df_output, window_sizes):
    rsi_dfs = []

    win_spec = Window.partitionBy("symbol").orderBy(col("rn").asc())

    # Calculate price difference (delta)
    df_with_delta = df_input.withColumn("price_diff", col("avg_price") - lag("avg_price", 1).over(win_spec))

    # Calculate gain and loss
    df_with_gains = df_with_delta.withColumn("gain", when(col("price_diff") > 0, col("price_diff")).otherwise(0))
    df_with_gains = df_with_gains.withColumn("loss", when(col("price_diff") < 0, -col("price_diff")).otherwise(0))

    for ws in window_sizes:

        # Only keep rows where we have enough history
        df_valid = df_with_gains.filter(col("rn") <= ws)

        # Aggregate total gains and losses over the window
        rsi_df = df_valid.groupBy("symbol").agg(
            (spark_sum("gain") / ws).alias("avg_gain"),
            (spark_sum("loss") / ws).alias("avg_loss")
        )

        # Calculate RSI
        rsi_df = rsi_df.withColumn(
            f"rsi_{ws}",
            when(
                col("avg_loss") == 0, 100.0
            ).otherwise(
                100 - (100 / (1 + (col("avg_gain") / col("avg_loss"))))
            )
        ).select("symbol", f"rsi_{ws}")

        rsi_dfs.append(rsi_df)

    # Join all RSI outputs on symbol
    if rsi_dfs:
        rsi_all = reduce(lambda left, right: left.join(right, on="symbol", how="outer"), rsi_dfs)
    else:
        rsi_all = df_input.select("symbol").distinct()

    df_output = df_output.join(rsi_all, on="symbol", how="left")

    return df_output
    


def calculate_macd(df_output):

    k_9 = 2 / (9 + 1)

    # Step 5: Calculate MACD Line
    df_joined = df_output.withColumn("macd", col("ema_12") - col("ema_26"))

    # Step 6: Calculate Signal Line (EMA_9 of MACD)
    df_joined = df_joined.withColumn(
        "signal",
        when(col("prev_signal").isNull(), col("macd"))
        .otherwise(col("prev_signal") * (1 - k_9) + col("macd") * k_9)
    )

    # Step 7: Histogram
    df_joined = df_joined.withColumn("macd_hist", col("macd") - col("signal"))

    # Step 8: Select only relevant columns for update
    macd_cols = ["symbol", "macd", "signal", "macd_hist"]
    macd_df = df_joined.select(macd_cols)

    # Step 9: Merge into df_output
    df_output = df_output.join(macd_df, on="symbol", how="left")

    return df_output


def calculate_bollinger_bands(df_input, df_output, window_sizes):
    bb_dfs = []

    for ws in window_sizes:
        # Only calculate BB for symbols with at least `ws` days
        valid_symbols = (
            df_input.groupBy("symbol")
            .agg(spark_max("rn").alias("max_rn"))
            .filter(col("max_rn") >= ws)
            .select("symbol")
        )
        df_filtered = df_input.join(valid_symbols, on="symbol", how="inner")

        # Compute average and stddev of avg_price over last `ws` days
        bb_df = df_filtered.groupBy("symbol").agg(
            avg(when(col("rn") <= ws, col("avg_price"))).alias(f"bb_ma_{ws}"),
            stddev(when(col("rn") <= ws, col("avg_price"))).alias(f"bb_std_{ws}")
        )

        # Calculate upper and lower bands
        bb_df = bb_df.withColumn(f"bb_upper_{ws}", col(f"bb_ma_{ws}") + 2 * col(f"bb_std_{ws}"))
        bb_df = bb_df.withColumn(f"bb_lower_{ws}", col(f"bb_ma_{ws}") - 2 * col(f"bb_std_{ws}"))
        bb_df = bb_df.withColumn(f"bb_width_{ws}", col(f"bb_upper_{ws}") - col(f"bb_lower_{ws}"))

        # Select relevant columns
        bb_df = bb_df.select("symbol", f"bb_upper_{ws}", f"bb_lower_{ws}", f"bb_width_{ws}")
        bb_dfs.append(bb_df)

    # Join all BB results
    if bb_dfs:
        bb_all = reduce(lambda left, right: left.join(right, on="symbol", how="outer"), bb_dfs)
    else:
        bb_all = df_input.select("symbol").distinct()

    df_output = df_output.join(bb_all, on="symbol", how="left")

    return df_output


def calculate_obv(df_input, df_output):

    # Create a window by symbol ordered by date
    win = Window.partitionBy("symbol").orderBy(col("rn").asc())

    # Compute price difference
    df_with_delta = df_input.withColumn("price_diff", col("avg_price") - lag("avg_price", 1).over(win))

    # Determine OBV adjustment: +volume if price up, -volume if price down, 0 otherwise
    df_with_direction = df_with_delta.withColumn(
        "obv_change",
        when(col("price_diff") > 0, col("volume"))
        .when(col("price_diff") < 0, -col("volume"))
        .otherwise(0)
    )

    # OBV is cumulative sum of obv_change per symbol
    df_with_obv = df_with_direction.withColumn("obv", spark_sum("obv_change").over(win.rowsBetween(Window.unboundedPreceding, Window.currentRow)))

    # Keep only the latest OBV (rn == 1)
    latest_obv = df_with_obv.filter(col("rn") == 1).select("symbol", "obv")

    # Join into result
    df_output = df_output.join(latest_obv, on="symbol", how="left")

    return df_output

# COMMAND ----------


from datetime import timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    struct,
    row_number,
    date_sub,
)
from pyspark.sql.window import Window
from src.utils.technical_indicators import (
    moving_average,
    exponential_moving_average,
    calculate_rsi,
    calculate_macd,
    calculate_bollinger_bands,
    calculate_obv
)
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def read_prices(input_table, row_date, granulity):

    if granulity == "daily":
        datecol = "date"
        nb_days = 90
    elif granulity == "hourly":
        datecol = "timestamp"
        nb_days = 5

    start_date = (row_date - timedelta(days=nb_days))
    end_date = row_date

    month_pairs = []
    current = start_date.replace(day=1)
    while current <= end_date:
        month_pairs.append((current.year, current.month))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


    df_recent = spark.table(input_table) \
        .withColumn("year_month", struct(col("year"), col("month"))) \
        .filter(col("year_month").isin([struct(lit(y), lit(m)) for (y, m) in month_pairs])) \
        .filter((col("date") >= lit(start_date)) & (col("date") <= lit(end_date)))

    window_spec = Window.partitionBy("symbol").orderBy(col(datecol).desc())
    df_latest = df_recent.withColumn("rn", row_number().over(window_spec))

    return df_latest


def take_latest_row(table_path, row_date, granulity):

    if granulity == "daily":
        datecol = "date"
    elif granulity == "hourly":
        datecol = "timestamp"

    #five_days_ago = date_sub(row_date, 5)

    five_days_ago = date_sub(lit(row_date), 5)
    df_recent = (
        spark.read.table(table_path)
        .filter((col(datecol) >= five_days_ago) & (col(datecol) < row_date))  # Adjust to your date column
    )

    # Define window: partition by symbol, order by date descending
    window_spec = Window.partitionBy("symbol").orderBy(col(datecol).desc())

    # Assign row number to get the latest record per symbol
    df_latest = (
        df_recent
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    return df_latest


def add_prev_prefix(df):
    for c in df.columns:
        if c != "symbol":
            df = df.withColumnRenamed(c, f"prev_{c}")
    return df


def run_all_indicators(df_filtered, df_result, windows, output_columns):

    df_result = moving_average(df_filtered, df_result, windows)

    df_result = exponential_moving_average(df_result, windows)

    df_result = calculate_rsi(df_filtered, df_result, windows)

    df_result = calculate_macd(df_result)

    df_result = calculate_bollinger_bands(df_filtered, df_result, windows)

    df_result = calculate_obv(df_filtered, df_result)

    df_result = df_result.select(*output_columns)

    return df_result
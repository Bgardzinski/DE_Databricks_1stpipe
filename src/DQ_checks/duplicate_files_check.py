from datetime import datetime
from pyspark.sql.functions import col, year, month, dayofmonth, hour, concat_ws
from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def duplicate_file_check_bronze(df_incoming, 
                                bronze_table: str,
                                layer: str, 
                                logging_table: str, 
                                critical: bool = True):
    """
    Check for duplicate symbol + extract_time (hour granularity) in the incoming DataFrame
    against already loaded Bronze table (load only relevant month partitions),
    log result to DQ log Delta table,
    and optionally raise exception if critical.

    Args:
        df_incoming (DataFrame): Incoming PySpark DataFrame to check
        bronze_table (str): Full table name of Bronze output table
        layer (str): Layer name (e.g., Bronze)
        logging_table (str): Full catalog name of DQ log table
        critical (bool): If True, raises exception on FAIL
    """
    now = datetime.now()

    # Extract unique year/month/day from incoming batch
    incoming_days = df_incoming.withColumn("year", year("extract_time")) \
                               .withColumn("month", month("extract_time")) \
                               .select("year", "month").distinct()

    # Load only relevant day partitions
    df_bronze_filtered = spark.table(bronze_table).filter(f"year={now.year} AND month={now.month}")

    # Prepare incoming batch at hour granularity
    df_incoming_hour = df_incoming.withColumn("extract_hour", concat_ws("-",
                            year("extract_time"),
                            month("extract_time"),
                            dayofmonth("extract_time"),
                            hour("extract_time")))

    # Prepare Bronze filtered table at hour granularity
    df_bronze_hour = df_bronze_filtered.withColumn("extract_hour", concat_ws("-",
                            year("extract_time"),
                            month("extract_time"),
                            dayofmonth("extract_time"),
                            hour("extract_time")))

    # Join incoming batch with Bronze on symbol + extract_hour
    duplicates = df_incoming_hour.join(
        df_bronze_hour,
        on=["symbol", "extract_hour"],
        how="inner"
    )

    duplicate_count = duplicates.count()

    if duplicate_count > 0:
        status = "FAIL"
        error_msg = f"{duplicate_count} duplicate (symbol, extract_time-hour) records found in Bronze table"
        observed_value = f"{duplicate_count} duplicates"
    else:
        status = "PASS"
        error_msg = ""
        observed_value = "0 duplicates"

    # Build DQ log row
    dq_row = Row(
        table_path=bronze_table,
        layer=layer,
        check_name="duplicate_file_check",
        check_type="quality",
        expected_value="No duplicate (symbol, extract_time-hour) in Bronze table",
        observed_value=observed_value,
        status=status,
        error_message=error_msg,
        check_timestamp=now,
        year=int(now.year),
        month=now.month,
        day=now.day
    )

    # Convert to DataFrame and append to DQ log table
    dq_row_df = spark.createDataFrame([dq_row]) \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("month", col("month").cast("int")) \
        .withColumn("day", col("day").cast("int"))

    dq_row_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(logging_table)

    # Raise exception if critical and failed
    if critical and status == "FAIL":
        raise Exception(f"Critical DQ check failed: {error_msg}")

    return dq_row_df
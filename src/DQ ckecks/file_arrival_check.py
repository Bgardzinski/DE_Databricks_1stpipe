# Databricks notebook source
from datetime import datetime
from azure.storage.blob import ContainerClient, BlobServiceClient
from pyspark.sql import Row
from src.config.config_file import CONTAINER_URL#, CONNECTION_STRING
from pyspark.sql.functions import col


def file_arrival_check_log(table_path: str,
                           layer: str,
                           batch_id: str,
                           logging_table: str,
                           critical: bool = True):
    """
    Check if the latest blob in the ADLS folder arrived in the current hour,
    log result to DQ log Delta table, and optionally raise exception if critical.

    Args:
        table_path (str): Path in ADLS to check
        layer (str): Layer name (e.g., Bronze)
        batch_id (str): Batch ID for this run
        logging_table (str): Full catalog name of DQ log table
        critical (bool): If True, raises exception on FAIL
    """
    now = datetime.now()
    year, month, day, hour = now.year, now.month, now.day, now.hour

    container_url = 'stockstoragebg1'
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=stockstoragebg1;AccountKey=q1HDzmQpreCTkJN9r3MMSGNUy9uKPv1PG6vn32FPt/KvoYfYcGH3s1XvQMUwaoT5L+PDmAMMKFG4+AStKjJ7jw==;EndpointSuffix=core.windows.net'
    folder_prefix = f"{table_path}/{year}/{month}"

    try:
        # Create service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        client = blob_service_client.get_container_client(container_url)

        blobs = list(client.list_blobs(name_starts_with=folder_prefix))
        if not blobs:
            status = "FAIL"
            error_msg = f"No files found in folder {folder_prefix}"
            observed_value = "Missing"
        else:
            latest_blob = max(blobs, key=lambda b: b.last_modified)
            blob_time = latest_blob.last_modified

            if (blob_time.year == year and
                blob_time.month == month and
                blob_time.day == day and
                blob_time.hour == hour):
                status = "PASS"
                error_msg = ""
                observed_value = "Found"
            else:
                status = "FAIL"
                error_msg = (f"Latest file {latest_blob.name} arrived at "
                             f"{blob_time}, not in current hour {hour}")
                observed_value = f"{blob_time}"

    except Exception as e:
        status = "FAIL"
        error_msg = str(e)
        observed_value = "Error"

    # Build DQ log row
    dq_row = Row(
        table_path=table_path,
        layer=layer,
        batch_id=batch_id,
        check_name="file_arrival_check",
        check_type="ingestion",
        expected_value=f"File for {year}-{month:02d}-{day:02d} hour {hour:02d}",
        observed_value=observed_value,
        status=status,
        error_message=error_msg,
        check_timestamp=now,
        year=int(year),
        month=month,
        day=day
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


file_arrival_check_log("dev/raw-data/hourly_prices",
                           "Silver",
                           "1",
                           "hive_metastore.dev_logging.dq_log")

# COMMAND ----------


from pyspark.sql import SparkSession

def get_spark(app_name="StockPipeline"):
    """
    Returns a Spark session. Uses getOrCreate() to ensure a single session per application.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()
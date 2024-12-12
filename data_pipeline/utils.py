def get_global_spark_session():
    """
    This function returns a spark session.
    """
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("EHR") \
        .master("local[*]") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

def log(message: str):
    """
    This function logs a message.
    """
    
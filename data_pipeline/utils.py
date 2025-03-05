from pyspark.sql import SparkSession

def get_global_spark_session() -> SparkSession:
    """
    This function returns a spark session.
    """
    return SparkSession.builder \
        .appName("EHR") \
        .master("local[*]") \
        .config("spark.executor.memory", "3g") \
        .config("spark.sql.parquet.columnReaderBatchSize", "1024") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.extraClassPath", "/mnt/e/Thesis/project/.data/postgresql-42.7.4.jar") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def log(message: str):
    """
    This function logs a message.
    """
    pass
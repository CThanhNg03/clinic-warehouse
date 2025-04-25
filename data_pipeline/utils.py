from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, **job_config) -> SparkSession:
    """
    Creates and returns a Spark session with dynamic configurations.

    Args:
        app_name (str): The name of the Spark job.
        master (str, optional): The Spark master URL (default: "local[*]").
        **job_config: Additional Spark configurations as key-value pairs.

    Returns:
        SparkSession: The configured Spark session.
    """
    config = SparkConf().setAppName(app_name)
    # Apply additional configurations if provided
    for key, value in job_config.items():
        config.set(key, value)

    return SparkSession.builder \
        .config(conf=config) \
        .enableHiveSupport() \
        .getOrCreate()

class Debug():
    iteration_num = 1

    @classmethod
    def debug_loop(cls, msg, iterations = None):
        if not iterations:
            iterations = [1]

        if cls.iteration_num in iterations:
            print(f"Debug in {cls.iteration_num}: {msg}")
        cls.iteration_num += 1

    @classmethod
    def reset(cls):
        cls.iteration_num = 1
    
    @classmethod
    def debug(cls, msg):
        print(f"Debug: {msg}")
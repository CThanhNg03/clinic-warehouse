import os
from typing import IO, List
from pyspark.sql import SparkSession

from config.logger import logger
from data_pipeline.utils import get_global_spark_session
from pyspark.sql.functions import explode

BATCH_SIZE = 8

OUTPUT_PARTITION = 4

spark: SparkSession = get_global_spark_session()
bronze_layer = os.environ.get("BRONZE_LAYER")

def ingest_data():
    """
    This function ingests data from Kafka.
    """
    
    pass

def ingest_initial_data():
    """
    This function ingests initial batch data from the local filesystem.
    """
    path = os.environ.get("EHR_PATH")

    if not path:
        raise ValueError("EHR_PATH is not set.")

    if not os.path.exists(path):
        raise FileNotFoundError(f"EHR_PATH '{path}' does not exist.")

    batch, batch_num, batch_id = [], 0, 0
    for dirpath in os.listdir(path):
        outer = os.path.join(path, dirpath)
        if os.path.isdir(outer):
            paths = []
            for file in os.listdir(outer):
                inner = os.path.join(outer, file)
                if os.path.isdir(inner):
                    paths.extend([os.path.join(inner, f) for f in os.listdir(inner)])
            batch.extend(paths)
            batch_num += 1
        if batch_num == BATCH_SIZE:
            batch_name = f"batch_{batch_id}"
            save_batch_data(batch, batch_name)
            batch, batch_num, batch_id = [], 0, batch_id + 1

def save_batch_data(data: List[str] | IO, batch_name: str):
    """
    This function saves a batch of JSON data to storage as Parquet files.
    """
    if not bronze_layer:
        raise ValueError("BRONZE_LAYER is not set.")

    logger.info(f"Processing batch: {batch_name}")
    try:
        df = spark.read.option("multiline", True).json(data)
        df = df.select("entry") \
                .withColumn("entry", explode("entry"))
        df.coalesce(OUTPUT_PARTITION).write.mode("overwrite").parquet(f"{bronze_layer}/{batch_name}.parquet")
        logger.info(f"Saved batch: {batch_name} to {bronze_layer}")
    except Exception as e:
        logger.error(f"Error saving batch {batch_name}: {e}")

if __name__ == "__main__":
    ingest_initial_data()
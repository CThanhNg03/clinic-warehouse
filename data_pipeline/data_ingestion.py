import os
from typing import IO, List, Union
from data_pipeline.utils import Debug
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from config.logger import logger
from config.settings import envi
from pyspark.sql.functions import explode

BATCH_SIZE = os.getenv("ETL_BATCH_SIZE", 8)

OUTPUT_PARTITION = os.getenv("OUTPUT_PARTITION", 4)

DATABASE = envi.DATABASE
INGESTED_BATCH_TABLE = envi.INGESTED_BATCH_TABLE
BRONZE_LAYER = envi.BRONZE_LAYER

def create_bronze_metadata_table(spark: SparkSession):
    """
    This function creates a metadata table for the bronze layer.
    """
    global BRONZE_LAYER
    global DATABASE
    global INGESTED_BATCH_TABLE
    if not BRONZE_LAYER:
        raise ValueError("BRONZE_LAYER is not set.")

    try:
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {DATABASE}
        """)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{INGESTED_BATCH_TABLE} (
                batch_id INT,
                save_ts TIMESTAMP,
                source STRING
            )
            STORED AS ORC
            TBLPROPERTIES (
                'transactional' = 'true'
            )
        """)
    except AnalysisException as e:
        logger.error(f"Error creating bronze metadata table: {e}")


def get_last_batch_id(spark: SparkSession, source: str) -> int:
    """
    This function retrieves the last batch ID from the bronze layer.
    """
    global DATABASE
    global INGESTED_BATCH_TABLE
    try:
        batch_id = spark.sql(f"""
            SELECT MAX(batch_id) as batch_id
            FROM {DATABASE}.{INGESTED_BATCH_TABLE}
            WHERE source = '{source}'
        """).collect()[0][0]

        if batch_id is None:
            logger.warning("Batch metadata not found. Starting from batch ID 0.")
            return None
        return batch_id
    
    except AnalysisException:
        logger.warning("Batch metadata table not found, creating it. Starting from batch ID 0.")
        create_bronze_metadata_table(spark)
        return None

def save_last_batch_id(spark: SparkSession, batch_id: int, source: str = "local"):
    """
    This function saves the last batch ID to the bronze layer.
    """
    global DATABASE
    global INGESTED_BATCH_TABLE
    try:
        spark.sql(f"""
            INSERT INTO {DATABASE}.{INGESTED_BATCH_TABLE} (batch_id, save_ts, source)
            VALUES ({batch_id}, CURRENT_TIMESTAMP, '{source}')
        """)
    except AnalysisException as e:
        logger.error(f"Error saving batch metadata: {e}")

def fetch_data_from_kafka(spark: SparkSession):
    """
    This function ingests data from Kafka.
    """
    
    pass

def fetch_data_from_local(spark: SparkSession, last_batch: int):
    """
    This function ingests initial batch data from the local filesystem.
    It read from $EHR_PATH
    """
    global BRONZE_LAYER
    BRONZE_LAYER = BRONZE_LAYER + "/local" if BRONZE_LAYER else None
    path = envi.EHR_PATH

    if not path:
        raise ValueError("EHR_PATH is not set.")

    if not os.path.exists(path):
        raise FileNotFoundError(f"EHR_PATH '{path}' does not exist.")

    batch, batch_num, batch_id = [], 0, 0
    for dirpath in sorted(os.listdir(path)):
        outer = os.path.join(path, dirpath)

        if not os.path.isdir(outer):
            continue

        paths = []
        for file in os.listdir(outer):
            inner = os.path.join(outer, file)
            if os.path.isdir(inner):
                paths.extend([os.path.join(inner, f) for f in os.listdir(inner)])
        batch.extend(paths)
        batch_num += 1

        if batch_id < last_batch:
            batch.clear()
            batch_num, batch_id = 0, batch_id + 1
            continue

        if batch_num == BATCH_SIZE:
            batch_name = f"batch_{batch_id}"
            save_batch_data(spark, batch, batch_name)
            save_last_batch_id(spark, batch_id)
            batch.clear()
            batch_num, batch_id = 0, batch_id + 1

def save_batch_data(spark: SparkSession, data: Union[List[str], IO], batch_name: str):
    """
    This function saves a batch of JSON data to storage as Parquet files.
    """
    if not BRONZE_LAYER:
        raise ValueError("BRONZE_LAYER is not set.")

    logger.info(f"Processing batch: {batch_name}")
    try:
        df = spark.read.option("multiline", True).json(data)
        df = df.select("entry") \
                .withColumn("entry", explode("entry"))
        df.coalesce(OUTPUT_PARTITION).write.mode("overwrite").parquet(f"{BRONZE_LAYER}/{batch_name}.parquet")
        # Save metadata
        
        logger.info(f"Saved batch: {batch_name} to {BRONZE_LAYER}")
    except Exception as e:
        logger.error(f"Error saving batch {batch_name}: {e}")

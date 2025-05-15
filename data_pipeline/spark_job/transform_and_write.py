import argparse
from datetime import datetime
import os
from typing import Literal, Optional
from venv import logger
from data_pipeline.data_ingestion import INGESTED_BATCH_TABLE
from data_pipeline.data_sinking import write_to_silver
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from data_pipeline.data_cleaning import transform_data
from data_pipeline.utils import get_spark_session
from config.settings import envi

DATABASE = envi.DATABASE
TRANSFORMED_BATCH_TABLE = envi.TRANSFORMED_BATCH_TABLE
INGESTED_BATCH_TABLE = envi.INGESTED_BATCH_TABLE

def create_metadata_table(spark: SparkSession, silver_layer: str):
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {DATABASE}
        """
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{TRANSFORMED_BATCH_TABLE} (
              batch_id INT,
              save_ts TIMESTAMP,
              source STRING
              )
        STORED AS ORC
        TBLPROPERTIES (
            'transactional' = 'true'
        )
    """)

def get_next_unprocessed_batch(spark, source: str) -> Optional[str]:
    try:
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW ingested_view AS
            SELECT DISTINCT batch_id, save_ts
            FROM {DATABASE}.{INGESTED_BATCH_TABLE}
            WHERE source = '{source}'
        """)

        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW transformed_view AS
            SELECT DISTINCT batch_id, save_ts
            FROM {DATABASE}.{TRANSFORMED_BATCH_TABLE}
            WHERE source = '{source}'
        """)

        result = spark.sql("""
            SELECT i.batch_id
            FROM ingested_view i
            LEFT ANTI JOIN transformed_view t
            ON i.batch_id = t.batch_id
            ORDER BY i.save_ts ASC
            LIMIT 1
        """)

        rows = result.collect()
        if not rows:
            return -1
        return rows[0]["batch_id"]
    
    except AnalysisException as e:
        if "not found" in str(e):
            create_metadata_table(spark, envi.STORAGE_LOCATION)
            logger.info("Metadata table created as it was not found.")
            return get_next_unprocessed_batch(spark, source)
        logger.error(f"Error retrieving next unprocessed batch: {e}")
        raise e
    
def save_transformed_batch_id(spark: SparkSession, batch_id: int, silver_layer: str, source: str = "local"):
    """
    This function saves the last transformed batch ID to the silver layer.
    """
    try:
        spark.sql(f"""
            INSERT INTO {DATABASE}.{TRANSFORMED_BATCH_TABLE} (batch_id, save_ts, source)
            VALUES ({batch_id}, CURRENT_TIMESTAMP(), "{source}")
        """)
    except AnalysisException as e:
        logger.error(f"Error saving transformed batch metadata: {e}")
        if "cannot be found" in str(e):
            create_metadata_table(spark, silver_layer)
            save_transformed_batch_id(spark, batch_id, silver_layer, source)
            logger.info(f"Transformed batch ID {batch_id} saved successfully.")
        else:
            logger.error(f"Unexpected error saving transformed batch ID: {e}")
            raise e

def main(source: Literal['local', 'kakfa'] = 'local'):
    STORAGE_LOCATION = envi.STORAGE_LOCATION
    bronze_layer = os.getenv("BRONZE_LAYER")

    if not bronze_layer or not DATABASE:
        raise ValueError("BRONZE_LAYER or SILVER_LAYER is not set.")
    
    bronze_layer = bronze_layer + "/" + source

    job_config = {
        # 'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        # 'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        # 'spark.sql.catalog.spark_catalog.type': 'hadoop',
        # 'spark.sql.catalog.spark_catalog.warehouse': STORAGE_LOCATION
    }
    spark = get_spark_session(app_name="Transform and Write Data", **job_config)

    transform_batch = get_next_unprocessed_batch(spark, source)

    if transform_batch == -1:
        logger.info("No new batches to process.")
        return
    
    if source == 'kakfa':
        transform_batch = datetime.fromtimestamp(int(transform_batch)).strftime("%Y-%m-%d %H:%M:%S")
        
    df = spark.read.parquet(f"{bronze_layer}/batch_{transform_batch}.parquet")

    # this is for local run
    # df.repartition(30)

    # Transform data
    transform_dfs = transform_data(df)

    try:
        # Write transformed data to silver layer
        write_to_silver(transform_dfs, DATABASE, mode="append")

    except AnalysisException as e:
        logger.error(f"Error writing to silver layer: {e}")
        if "cannot be found" in str(e):
            create_metadata_table(spark, STORAGE_LOCATION)
            write_to_silver(transform_dfs, DATABASE, transform_batch, mode="create")
        else:
            raise e
    finally:
        # Save transformed batch ID
        save_transformed_batch_id(spark, transform_batch, STORAGE_LOCATION, source)
        logger.info(f"Transformed batch ID {transform_batch} saved successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform and write data to silver layer")
    parser.add_argument(
        "--source",
        type=str,
        choices=["local", "kafka"],
        default="local",
        help="Source of data: 'local' or 'kafka'",
    )
    args = parser.parse_args()
    main(source=args.source)
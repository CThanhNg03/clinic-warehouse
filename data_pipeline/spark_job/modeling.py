from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from typing import Optional

from venv import logger
from data_pipeline.data_cleaning import get_warehouse_model, model_table
from data_pipeline.data_sinking import MODELED_BATCH_TABLE, write_to_postgres
from data_pipeline.utils import get_spark_session

from config.settings import envi   

DATABASE = envi.DATABASE
TRASNFORMED_BATCH_TABLE = envi.TRANSFORMED_BATCH_TABLE
MODELED_BATCH_TABLE = envi.MODELED_BATCH_TABLE

def create_metadata_table(spark: SparkSession, silver_layer: str):
    """
    This function creates a metadata table for the modeled data.
    """
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {DATABASE}
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{MODELED_BATCH_TABLE} (
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
    """
    This function retrieves the next unprocessed batch from the transformed data.
    """
    try:
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW transformed_view AS
            SELECT DISTINCT batch_id, save_ts
            FROM {DATABASE}.{TRASNFORMED_BATCH_TABLE}
            WHERE source = '{source}'
        """)

        result = spark.sql(f"""
            SELECT t.batch_id
            FROM transformed_view t
            LEFT ANTI JOIN {DATABASE}.{MODELED_BATCH_TABLE} m
            ON t.batch_id = m.batch_id
            ORDER BY t.save_ts ASC
            LIMIT 1
        """)

        rows = result.collect()
        if not rows:
            return None
        return rows[0]["batch_id"]
    
    except AnalysisException:
        logger.warning("Transformed metadata table not found, creating it.")
        create_metadata_table(spark, envi.SILVER_LAYER)
        return None
    
def get_next_unprocessed_batch(spark, source: str) -> Optional[str]:
    """
    This function retrieves the next unprocessed batch from the transformed data.
    """
    try:
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW transformed_view AS
            SELECT DISTINCT batch_id, save_ts
            FROM {DATABASE}.{TRASNFORMED_BATCH_TABLE}
            WHERE source = '{source}'
        """)

        result = spark.sql(f"""
            SELECT t.batch_id
            FROM transformed_view t
            LEFT ANTI JOIN {DATABASE}.{MODELED_BATCH_TABLE} m
            ON t.batch_id = m.batch_id
            ORDER BY t.save_ts ASC
            LIMIT 1
        """)

        rows = result.collect()
        if not rows:
            return None
        return rows[0]["batch_id"]
    
    except AnalysisException:
        logger.warning("Transformed metadata table not found, creating it.")
        create_metadata_table(spark, envi.SILVER_LAYER)
        return None
    
def main():
    """
    Main function to run the data modeling tasks.
    """

    YAML_FILE = envi.WAREHOUSE_MODEL_YAML

    # Initialize Spark session
    spark = get_spark_session(app_name="Modeling Data")

    warehouse_model = get_warehouse_model(YAML_FILE)
    
    # Get the next unprocessed batch
    batch_id = get_next_unprocessed_batch(spark, source="local")
    if batch_id is None:
        logger.info("No unprocessed batches found.")
        return
    logger.info(f"Processing batch ID: {batch_id}")

    for schema in warehouse_model:
        tables = warehouse_model[schema]
        for table in tables:
            meta = tables[table]
            df = spark.sql(f"SELECT * FROM {DATABASE}.{meta.source} WHERE batch_id = {batch_id}")
            if df.count() == 0:
                logger.info(f"No data found for batch ID {batch_id} in table {meta.source}.")
                continue
            model = model_table(df, meta)
            write_to_postgres(model, f"{schema}.{table}")
            logger.info(f"Data written to {schema}.{table} for batch ID {batch_id}.")

if __name__ == "__main__":
    main()


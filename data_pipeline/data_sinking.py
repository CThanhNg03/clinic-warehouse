from typing import Dict
import pyspark.sql as pd
from pyspark.sql.utils import AnalysisException

from config.settings import envi
from config.logger import logger

TRANSFORMED_BATCH_TABLE = envi.TRANSFORMED_BATCH_TABLE
MODELED_BATCH_TABLE = envi.MODELED_BATCH_TABLE
DATABASE = envi.DATABASE


def create_metadata_table(spark: pd.SparkSession, layer: str):
    """
    Create a metadata table for the specified layer.
    
    Args:
        spark: Spark session.
        layer: Layer to create the metadata table for (e.g., 'silver', 'gold').
    """ 
    if layer == 'silver':
        table_name = TRANSFORMED_BATCH_TABLE
    elif layer == 'gold':   
        table_name = MODELED_BATCH_TABLE
    else:
        raise ValueError("Invalid layer specified. Use 'silver' or 'gold'.")

    try:
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {DATABASE}
        """)
        
        spark.sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{table_name} (
                batch_id INT,
                save_ts TIMESTAMP,
                source STRING
            )
            STORED AS PARQUET
            LOCATION '{layer}/metadata'
        """)
    except AnalysisException as e:
        print(f"Error creating metadata table {table_name}: {e}")

def get_unmodeled_batch(spark: pd.SparkSession) -> int:
    """
    Get the next unmodeled batch ID from the metadata table.
    """
    try:
        result = spark.sql(f"""
            SELECT t.batch_id
            FROM {DATABASE}.{TRANSFORMED_BATCH_TABLE} t
            LEFT ANTI JOIN {DATABASE}.{MODELED_BATCH_TABLE} m
            ON t.batch_id = m.batch_id
            ORDER BY t.save_ts ASC
            LIMIT 1
        """)
        
        rows = result.collect()
        if not rows:
            logger.info("No unmodeled batches found.")
            return None
        
        return rows[0]["batch_id"]
    except AnalysisException as e:
        logger.error(f"Error fetching unmodeled batch: {e}")
        raise e


def save_batch_id(spark: pd.SparkSession, batch_id: int, layer: str = 'gold'):
    """
    Save the batch ID to a metadata table in the specified layer.
    
    Args:
        spark: Spark session.
        batch_id: Batch ID to save.
        layer: Layer to save the batch ID (default is 'silver').
    """
    if layer == 'silver':
        table_name = envi.TRANSFORMED_BATCH_TABLE
    elif layer == 'bronze':
        table_name = envi.INGESTED_BATCH_TABLE
    elif layer == 'gold':
        table_name = envi.MODELED_BATCH_TABLE
    else:
        raise ValueError("Invalid layer specified. Use 'silver', 'bronze', or 'gold'.")
    
    try:
        spark.sql(f"""
            INSERT INTO {envi.DATABASE}.{table_name} (batch_id, save_ts, source)
            VALUES ({batch_id}, CURRENT_TIMESTAMP(), '{layer}')
        """)
    except AnalysisException as e:
        if "cannot be found" in str(e):
            create_metadata_table(spark, layer)
            save_batch_id(spark, batch_id, layer)
            logger.info(f"Batch ID {batch_id} saved successfully.")
        else:
            logger.error(f"Error saving batch ID {batch_id}: {e}")
            raise e

def write_to_silver(spark: pd.SparkSession, dfs: Dict[str, pd.DataFrame], dst: str, batch_id: int, *, mode: str = "append", format: str = "parquet", table_type: str = "hive"):
    """
    Write Spark DataFrames to a Silver layer table, adaptable for Hive and Iceberg.

    Args:
        dfs: Dictionary of Spark DataFrames to write.
        dst: Destination path or table name for the Silver layer.
        mode: Write mode (default is "append").
        format: File format to use (default is "parquet").
        table_type: Table type, either "hive" or "iceberg" (default is "hive").
    """
    for table_name, df in dfs.items():
        writer = df.withColumn("batch_id", batch_id) \
            .writeTo(f"{dst}.{table_name}") 
        
        if table_type == "hive":
            writer.tableProperty("format", format)
        
        if table_type != "iceberg":
            raise ValueError("Unsupported table type. Supported types are 'hive' and 'iceberg'.")
        
        if mode == "create":
            writer.create()
        else:
            writer.append()
    
def write_to_postgres(df: pd.DataFrame, table_name: str, *, mode: str = "append"):
    """
    Write a Spark DataFrame to a PostgreSQL table.
    
    Args:
        df: Spark DataFrame to write.
        table_name: Name of the PostgreSQL table.
        mode: Write mode (default is "append").
    """

    jdbc_url = f"{envi.database['uri']}warehouse"
    properties = {
        "user": envi.database["user"],
        "password": envi.database["password"],
        "driver": envi.database["driver"]
    }
    try:
        df.drop_duplicates() \
            .write \
            .mode("append") \
            .option("truncate", "false") \
            .option("isolationLevel", "NONE") \
            .option("sessionInitStatement", "SET session_replication_role = replica;") \
            .jdbc(url=jdbc_url, table=table_name, mode=mode, properties=properties)
            
    except AnalysisException as e:
        print(f"Error writing to table {table_name}: {e}")

def sink_modeled_data(spark: pd.SparkSession, dfs: Dict[str, pd.DataFrame], dst: str, batch_id: int):
    """
    Sink the modeled data to a PostgreSQL database.
    
    Args:
        spark: Spark session.
        dfs: Dictionary of Spark DataFrames to sink.
        dst: Destination path or table name for the PostgreSQL database.
        batch_id: Batch ID to associate with the data.
    """
    for table_name, df in dfs.items():
        write_to_postgres(df, f"{dst}.{table_name}", mode="append")
    
    save_batch_id(spark, batch_id, layer="gold")


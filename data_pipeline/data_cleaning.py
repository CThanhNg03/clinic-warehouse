from dataclasses import dataclass
from typing import Dict, List, Optional
import pyspark.sql as pd    
from pyspark.sql.functions import col, explode, collect_list, udf, lit, when, regexp_extract, concat_ws, isnull, array_size, isnotnull, from_json, to_date, to_timestamp
from pyspark.sql.utils import AnalysisException

from data_pipeline.utils import get_global_spark_session
from config.settings import envi
from data_pipeline import transform

@dataclass
class WarehouseTable:
    source: str
    columns: List[str]
    transform: Optional[Dict[str, List[str]]]
    
def get_warehouse_model() -> Dict[str, Dict[str, WarehouseTable]]:
    import os
    import yaml

    module_path = os.path.abspath(__file__)
    base_dir = os.path.dirname(module_path)
    yaml_path = os.path.join(base_dir, 'transform', 'warehouse_model.yaml')

    with open(yaml_path, 'r') as file:
        warehouse_model = yaml.safe_load(file)

    return warehouse_model

def transform_resource_type(df: pd.DataFrame, resource_type: str) -> Dict[str, pd.DataFrame]:
    """
    Processes the dataframe by a specific resource type and applies the corresponding transformations.

    Parameters:
    df: pd.DataFrame - The dataframe to be processed.
    resource_type (str): The type of resource to be processed (e.g., "Patient", "Observation").

    Returns:
    Dict[str, pd.DataFrame]: A dictionary where the keys are resource types and the values are the transformed Spark DataFrames.
    """
    transform_functions = {
        "Patient": transform.patient.transform_patient,
        "Observation": transform.observation.transform_observation,
        "Condition": transform.condition.transform_condition,
        "MedicationRequest": transform.medication.transform_medication_request,
        "Immunization": transform.immunization.transform_immunization,
        "Procedure": transform.procedure.transform_procedure,
        "CarePlan": transform.careplan.transform_careplan,
        "Encounter": transform.encounter.transform_encounter,
        "DiagnosticReport": transform.diagnostic.transform_diagnostic,
        "AllergyIntolerance": transform.allergy.transform_allergy,
    }

    details_dfs: Dict[str, pd.DataFrame] = {}
    transform_function = transform_functions.get(resource_type)
    if transform_function:
        details_dfs = transform_function(df, details_dfs)
    return details_dfs

def transform_data(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    This function transforms data from file data with json columns to a flat table.
    Input:
    df:
        - path: str - path to the metadata file
        - data: dict - json data
    """
    df = df.withColumn("resourceType", col("entry.resource.resourceType")) \
        .groupBy("resourceType") \
        .agg(collect_list("entry").alias("entries")) \
        .withColumn("data", explode("entries")) \
        .drop("entries")\
        .select("resourceType", col("data.resource").alias("data")).persist()
    
    unique_resourceTypes = df.select("resourceType").distinct().rdd.map(lambda r: r["resourceType"]).collect()

    # Create a dictionary to store the transformed data
    details_dfs: Dict[str, pd.DataFrame] = {}
    for resource_type in unique_resourceTypes:
        resource = df.filter(col("resourceType") == resource_type)
        details_dfs.update(transform_resource_type(resource, resource_type))
    
    return details_dfs

def model_data(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    This function models the data into dimensions and facts for a data warehouse.
    :param dfs: Dictionary of Spark DataFrames keyed by their original name.
    :return: Dictionary of Spark DataFrames modeled into dimensions and facts with schema prefix.
    """
    model_schema = get_warehouse_model()

    models = {}

    for schema in model_schema:
        tables = model_schema[schema]
        for table in tables:
            meta = tables[table]
            source = meta.source
            if source not in dfs:
                print(f"Source {source} not found in dataframes")
                continue
            model = dfs[source].selectExpr(meta.columns)
            if meta.transform:
                if meta.transform["to_date"]:
                    for field in meta.transform["to_date"]:
                        model = model.withColumn(field, to_date(col(field), "yyyy-MM-dd"))
                if meta["to_timestamp"]:
                    for field in meta.transform["to_timestamp"]:
                        model = model.withColumn(field, to_timestamp(col(field), "yyyy-MM-dd'T'HH:mm:ss"))
            models[f"{schema}.{table}"] = model
    
    return models

def write_to_silver(dfs: Dict[str, pd.DataFrame], dst: str, *, mode: str = "append"):
    """
    Write a Spark DataFrame to a Silver layer table.
    :param df: Spark DataFrame to write.
    :param table_name: Name of the Silver layer table.
    :param mode: Write mode (default is "append").
    """
    for df in dfs:
        dfs[df].write.mode("overwrite").parquet(f"{dst}/{df}")

def write_to_postgres(df: pd.DataFrame, table_name: str, *, mode: str = "append"):
    """
    Write a Spark DataFrame to a PostgreSQL table.
    :param df: Spark DataFrame to write.
    :param table_name: Name of the PostgreSQL table.
    :param mode: Write mode (default is "append").
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

if __name__ == "__main__":
    spark = get_global_spark_session()

    import os
    import dotenv

    dotenv.load_dotenv()

    bronze_layer = os.environ.get("BRONZE_LAYER")
    silver_layer = os.environ.get("SILVER_LAYER")
    
    path = os.listdir(bronze_layer)

    detail_dfs = transform_data(spark.read.parquet(f"{bronze_layer}/{path[0]}"))
    # for df in path:
    #     detail_dfs[df] = spark.read.parquet(f"{silver_layer}/{df}")
        
    models = model_data(detail_dfs)
    for df in models:
        if df not in ["care.fact_careplan"]:
            continue
        write_to_postgres(models[df], df)
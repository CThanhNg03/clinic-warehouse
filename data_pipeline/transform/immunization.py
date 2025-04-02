from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode

from .udf import extract_id_udf

def transform_immunization(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    immunization = df.withColumn("coding", explode("data.vaccineCode.coding"))

    details_dfs["vaccine_code"] = immunization.select(
        col("coding.code").alias("code"),
        col("coding.display").alias("display")
    ).dropDuplicates(["code"])

    details_dfs["immunization"] = immunization.select(
        extract_id_udf(col("data.patient.reference")).alias("patient_id"),
        extract_id_udf(col("data.encounter.reference")).alias("encounter_id"),
        col("coding.code").alias("vaccineCode"),
        col("data.date").alias("date"),
        col("data.status").alias("status"),
        col("data.wasNotGiven").alias("wasNotGiven"),
        col("data.primarySource").alias("primarySource")
    )
    return details_dfs

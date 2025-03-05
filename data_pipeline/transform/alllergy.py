from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode

from .udf import extract_id_udf

def transform_allergy(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    allergy = df.withColumn("coding", explode("data.code.coding")) \
            .withColumn("category", explode("data.category"))

    details_dfs["allergy_code"] = allergy.select(
        col("coding.code").alias("code"),
        col("coding.display").alias("display"),
        "data.type",
        "category"
    ).dropDuplicates(["code"])

    details_dfs["allergy"] = allergy.select(
        col("data.criticality").alias("criticality"),
        extract_id_udf(col("data.patient.reference")).alias("patient_id"),
        col("data.clinicalStatus").alias("clinicalStatus"),
        col("data.assertedDate").alias("assertedDate"),
        col("coding.code").alias("allergy_code")
    )
    pass

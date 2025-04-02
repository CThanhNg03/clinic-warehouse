from typing import Dict
import pyspark.sql.dataframe as pd

from pyspark.sql.functions import col, explode

from .udf import extract_id_udf

def transform_condition(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    condition = df.withColumn("coding", explode("data.code.coding"))

    details_dfs["condition_code"] = condition.select(
        col("coding.code").alias("code"),
        col("coding.display").alias("display")
    ).dropDuplicates(["code"])

    details_dfs["condition"] = condition.select(
        col("data.id").alias("condition_id"),
        col("data.onsetDateTime").alias("onsetDateTime"),
        col("data.clinicalStatus").alias("clinicalStatus"),
        col("data.verificationStatus").alias("verificationStatus"),
        col("coding.code").alias("code"),
        extract_id_udf(col("data.subject.reference")).alias("patient_id"),
        extract_id_udf(col("data.context.reference")).alias("encounter_id"),
        col("data.abatementDateTime").alias("abatementDateTime")
    )
    return details_dfs

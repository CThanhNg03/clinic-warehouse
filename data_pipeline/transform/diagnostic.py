from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode

from .udf import extract_id_udf

def transform_diagnostic(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    diagnostic = df.withColumn("coding", explode("data.code.coding")) \
                .withColumn("result", explode("data.result"))

    details_dfs["diagnostic_code"] = diagnostic.select(
        col("coding.code").alias("code"),
        col("coding.display").alias("display")
    ).dropDuplicates(["code"])

    details_dfs["diagnostic_result"] = diagnostic.select(
        col("data.id").alias("diagnostic_id"),
        extract_id_udf(col("result.reference")).alias("result_id"),
        col("result.display").alias("display")
    )

    details_dfs["diagnostic"] = diagnostic.select(
        col("data.id").alias("diagnostic_id"),
        col("data.status").alias("status"),
        col("data.issued").alias("issued"),
        col("data.effectiveDateTime").alias("effectiveDateTime"),
        extract_id_udf(col("data.subject.reference")).alias("patient_id"),
        extract_id_udf(col("data.encounter.reference")).alias("encounter_id"),
        col("coding.code").alias("report_code")
    )
    pass

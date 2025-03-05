from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode, from_json

from .udf import extract_id_udf
from .schema import encounter_type_schema

def transform_encounter(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    encounter = df.withColumn("type", explode(from_json("data.type", encounter_type_schema))) 

    details_dfs["encounter_type"] = encounter.selectExpr(
        "type.coding[0].code as code",
        "type.text as display"
    ).dropDuplicates(["code"])

    details_dfs["encounter_reason"] = encounter.selectExpr(
        "explode(data.reason.coding) as coding"
    ).select(
        col("coding.code").alias("code"),
        col("coding.display").alias("display")
    )

    details_dfs["encounter"] = encounter.select(
        col("data.id").alias("encounter_id"),
        extract_id_udf(col("data.patient.reference")).alias("patient_id"),
        col("data.status").alias("status"),
        col("data.class.code").alias("class"),
        col("type.coding").getItem(0).getItem("code").alias("type_code"),
        col("data.reason.coding").getItem(0).getItem("code").alias("reason_code"),
        col("data.period.start").alias("periodStart"),
        col("data.period.end").alias("periodEnd")
    )
    pass

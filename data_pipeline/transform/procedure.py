from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode, from_json, lit, regexp_extract, when

from .udf import extract_id_udf
from .schema import procedure_reason_schema

def transform_procedure(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    procedure = df.withColumn(
        "reasonReference",
        from_json(col("data.reasonReference"), procedure_reason_schema).getItem("reference")
    )

    details_dfs["procedure_type"] = procedure.selectExpr(
        "explode(data.code.coding) as coding"
    ).select(
        col("coding.code").alias("code"),
        col("coding.display").alias("display")
    )

    details_dfs["procedure"] = procedure.select(
        extract_id_udf(col("data.subject.reference")).alias("patient_id"),
        extract_id_udf(col("data.encounter.reference")).alias("encounter_id"),
        extract_id_udf(col("reasonReference")).alias("reasonReference"),
        col("data.code.coding").getItem(0).getItem("code").alias("procedureType"),
        col("data.status").alias("status"),
        when(
        col("data.performedPeriod.start").isNotNull(),
        col("data.performedPeriod.start")
        ).otherwise(col("data.performedDateTime")).alias("performedPeriodStart"),
        when(
        col("data.performedPeriod.end").isNotNull(),
        col("data.performedPeriod.end")
        ).otherwise(col("data.performedDateTime")).alias("performedPeriodEnd")
    )
    pass
    return details_dfs

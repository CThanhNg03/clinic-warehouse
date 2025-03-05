from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode, from_json

from .udf import extract_id_udf
from .schema import careplan_category_schema

def transform_careplan(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    careplan = df.withColumn("activity", explode("data.activity")) \
                .withColumn("addresses", explode("data.addresses"))

    details_dfs["careplan_category"] = careplan.selectExpr("explode(data.category) as category") \
                .select(from_json("category", careplan_category_schema).alias("category")) \
                .selectExpr(
                    "category.coding[0].code as code",
                    "category.coding[0].display as display"
                )

    details_dfs["careplan_activity"] = careplan.selectExpr(
        "activity.detail.code.coding[0].code as code", 
        "activity.detail.code.coding[0].display as display")

    details_dfs["careplan"] = careplan.select(
        extract_id_udf(col("data.subject.reference")).alias("patient_id"),
        extract_id_udf(col("data.context.reference")).alias("encounter_id"),
        extract_id_udf(col("addresses.reference")).alias("address_id"),
        col("data.status").alias("status"),
        col("activity.detail.status").alias("activityStatus"),
        col("data.period.start").alias("periodStart"),
        col("data.period.end").alias("periodEnd"),
        col("activity.detail.code.coding").getItem(0).getItem("code").alias("activityCode"),
        col("data.category").alias("categoryCode") 
    )\
        .withColumn("categoryCode", from_json(col("categoryCode").getItem(0), careplan_category_schema).getItem("coding").getItem(0).getItem("code"))
    pass

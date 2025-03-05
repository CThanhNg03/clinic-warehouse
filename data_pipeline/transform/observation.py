from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode, lit

from .udf import extract_id_udf

def transform_observation(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    observation_code = df.select("data.code.coding") \
                            .withColumn("code", explode("coding")) \
                            .select(
                                col("code.code").alias("code"),
                                col("code.display").alias("display")) \
                            .distinct()
    
    observation_valueCodeableConcept = df.select("data.valueCodeableConcept", col("data.id").alias("observation_id")) \
                                            .withColumn("value", explode("valueCodeableConcept.coding")) \
                                            .select(
                                                "observation_id",
                                                col("value.code").alias("code"),
                                                col("value.display").alias("value")) \
                                            .withColumn("unit", lit("concept"))
    
    observation_component = df.select("data.component", col("data.id").alias("observation_id")) \
                                .withColumn("component", explode("component")) \
                                .withColumn("code", explode("component.code.coding")) \
                                .select(
                                    "observation_id",
                                    col("code.code").alias("code"),
                                    col("code.display").alias("display"),
                                    col("component.valueQuantity.value").alias("value"),
                                    col("component.valueQuantity.unit").alias("unit")
                                )
    
    observation_result = df.select(
        col("data.id").alias("observation_id"),
        col("data.valueQuantity.value").alias("value"),
        col("data.valueQuantity.unit").alias("unit"),
        col("data.code.coding").getItem(0).getItem("code").alias("code")
    )

    component_result = observation_component.select(
        "observation_id",
        "code",
        "value",
        "unit"
    )

    details_dfs["observation_result"] = observation_result.unionByName(component_result).unionByName(observation_valueCodeableConcept)

    details_dfs["observation_code"] = observation_code.unionByName(observation_component.select("code", "display")).distinct()

    details_dfs["observation"] = df.select(
        col("data.id").alias("observation_id"),
        col("data.status").alias("status"),
        col("data.issued").alias("issued"),
        col("data.effectiveDateTime").alias("effectiveDateTime"),
        extract_id_udf(col("data.subject.reference")).alias("patient_id"),
        extract_id_udf(col("data.encounter.reference")).alias("encounter_id"),
        col("data.code.coding").getItem(0).getItem("code").alias("code")
    )
    pass

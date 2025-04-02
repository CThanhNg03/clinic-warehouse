from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode, lit, regexp_extract, when, concat_ws, isnull
from pyspark.sql.types import IntegerType

from .udf import extract_display_udf

def transform_patient(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    details_dfs["patient_extension"] = df.select("data.extension", col("data.id").alias("patient_id"))\
        .withColumn("extension", explode("extension"))\
        .withColumn("type", regexp_extract(col("extension.url"), r"([^/]+)$", 1))\
        .withColumn(
            "value",
            when(
                col("extension").isNotNull() & col("extension").getItem("valueCodeableConcept").isNotNull(),
                extract_display_udf(col("extension").getItem("valueCodeableConcept"))
            )
            .when(
                col("extension").isNotNull() & col("extension").getItem("valueAddress").isNotNull(),
                concat_ws(", ", col("extension").getItem("valueAddress").getItem("city"), col("extension").getItem("valueAddress").getItem("state"))
            )
            .when(
                col("extension").isNotNull() & col("extension").getItem("valueCode").isNotNull(),
                col("extension").getItem("valueCode")
            )
            .when(
                col("extension").isNotNull() & col("extension").getItem("valueString").isNotNull(),
                col("extension").getItem("valueString")
            )
            .otherwise(lit(None))
        )\
        .drop("extension")

    patient_address = df.select("data.address", col("data.id").alias("patient_id"))\
        .withColumn("address", explode("address"))

    # Explode the nested structure
    exploded_df = patient_address.withColumn("address_exploded", col("address")) \
        .withColumn("ext_exploded", explode(col("address_exploded.extension"))) \
        .withColumn("inner_ext_exploded", explode(col("ext_exploded.extension")))

    # Extract latitude and longitude
    latitude_df = exploded_df.filter(col("inner_ext_exploded.url") == lit("latitude")).select(
        col("inner_ext_exploded.valueDecimal").alias("latitude"),
        exploded_df["address_exploded"].alias("address"),
        exploded_df["patient_id"].alias("patient_id")
    )

    longitude_df = exploded_df.filter(col("inner_ext_exploded.url") == lit("longitude")).select(
        col("inner_ext_exploded.valueDecimal").alias("longitude"),
        exploded_df["address_exploded"].alias("address"),
        exploded_df["patient_id"].alias("patient_id")
    )

    details_dfs["patient_address"] = latitude_df.join(longitude_df, ["address", "patient_id"])\
        .select(
            "patient_id",
            "latitude",
            "longitude",
            col("address.city").alias("city"),
            col("address.state").alias("state"),
            col("address.postalCode").alias("postalCode"),
            col("address.line").getItem(0).alias("line")
        )

    details_dfs["patient_telecom"] = df.select("data.telecom", col("data.id").alias("patient_id"))\
        .withColumn("telecom", explode("telecom"))\
        .select(
            "patient_id",
            col("telecom.system").alias("system"),
            col("telecom.value").alias("value"),
            col("telecom.use").alias("use"),
            col("telecom.extension").getItem(0).getItem("valueBoolean").alias("okay_to_leave_message")
        )

    details_dfs["patient_name"] = df.select("data.name", col("data.id").alias("patient_id"))\
        .withColumn("name", explode("name"))\
        .select(
            "patient_id",
            col("name.use").alias("use"),
            col("name.family").alias("family"),
            col("name.given").alias("given"),
            col("name.prefix").alias("prefix"),
            col("name.suffix").alias("suffix")
        )\
        .withColumn("given", explode("given"))\
        .withColumn("prefix", explode("prefix"))\
        .withColumn("suffix", explode("suffix"))

    details_dfs["patient"] = df.select(
        col("data.id").alias("patient_id"),
        col("data.gender").alias("gender"),
        col("data.birthDate").alias("birthDate"),
        col("data.deceasedDateTime").alias("deceasedDateTime"),
        col("data.multipleBirthBoolean").alias("multipleBirth"),
        col("data.multipleBirthInteger").alias("multipleBirthInteger"),
        col("data.maritalStatus.coding").getItem(0).getItem("code").alias("maritalStatus")
    )\
    .withColumn("multipleBirth", when(isnull(col("multipleBirth")), col("multipleBirthInteger")).otherwise(col("multipleBirth").cast(IntegerType())))\
    .drop("multipleBirthInteger")
    pass
    return details_dfs

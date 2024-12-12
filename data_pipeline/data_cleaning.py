from typing import Dict
import pandas as pd
import pyspark.sql.dataframe as ps    
from pyspark.sql.functions import col, explode, collect_list, udf, lit, when, regexp_extract, concat_ws, isnull, array_size, isnotnull, from_json
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType

extract_display_udf = udf(lambda x: ",".join([v['display'] for v in x['coding']]) if x else None, StringType())

extract_type_udf = udf(lambda x: x.split("/")[-1] if x else None, StringType())

extract_id_udf = udf(lambda x: x.split(":")[-1] if x is not None else None, StringType())

reason_reference_schema = ArrayType(
    StructType([
        StructField("reference", StringType(), True)
    ])
)

procedure_reason_schema = StructType([
    StructField("reference", StringType(), True)
])

careplan_category_schema = StructType([
    StructField("coding", ArrayType(
        StructType([
            StructField("code", StringType(), True),
            StructField("display", StringType(), True),
            StructField("system", StringType(), True)
        ])
    ), True)
])

encounter_type_schema = ArrayType(StructType([
            StructField("coding", ArrayType(
                StructType([
                    StructField("code", StringType(), True),
                    StructField("display", StringType(), True),
                    StructField("system", StringType(), True)
                ])
            ), True),
            StructField("text", StringType(), True)
        ]))

def transform_resource_type(df: ps.DataFrame, resource_type: str) -> Dict[str, pd.DataFrame]:
    """
    This function process the dataframe by a specific resource type.
    """
    details_dfs: Dict[str, ps.DataFrame] = {}
    if resource_type == "Patient":
        # Extract patient extension data
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
            exploded_df["address_exploded"].alias("address_exploded"),
            exploded_df["patient_id"].alias("patient_id")
        )

        longitude_df = exploded_df.filter(col("inner_ext_exploded.url") == lit("longitude")).select(
            col("inner_ext_exploded.valueDecimal").alias("longitude"),
            exploded_df["address_exploded"].alias("address_exploded"),
            exploded_df["patient_id"].alias("patient_id")
        )

        details_dfs["patient_address"] = (
            latitude_df.join(longitude_df, latitude_df["address_exploded"] == longitude_df["address_exploded"], "inner")
        ).select(
            latitude_df["patient_id"],
            latitude_df["latitude"],
            longitude_df["longitude"],
            latitude_df["address_exploded.city"].alias("city"),
            latitude_df["address_exploded.state"].alias("state"),
            latitude_df["address_exploded.postalCode"].alias("postalCode"),
            latitude_df["address_exploded.line"].getItem(0).alias("line")
        )

        details_dfs["patient_telecom"] = df.select("data.telecom", col("data.id").alias("patient_id"))\
                          .withColumn("telecom", explode("telecom")) \
                          .select(
                              "patient_id",
                              col("telecom.system").alias("system"),
                              col("telecom.value").alias("value"),
                              col("telecom.use").alias("use"),
                              col("telecom.extension").getItem(0).getItem("valueBoolean").alias("okay_to_leave_message")
                          )
        
        details_dfs["patient_name"] = df.select("data.name", col("data.id").alias("patient_id")) \
                      .withColumn("name", explode("name")) \
                      .select(
                          "patient_id",
                          col("name.use").alias("use"),
                          col("name.family").alias("family"),
                          col("name.given").alias("given"),
                          col("name.prefix").alias("prefix"),
                          col("name.suffix").alias("suffix")
                      ) \
                      .withColumn("given", explode("given")) \
                      .withColumn("prefix", explode("prefix")) \
                      .withColumn("suffix", explode("suffix"))
        
        details_dfs["patient"] = df.select(
                        col("data.id").alias("patient_id"),
                        col("data.gender").alias("gender"),
                        col("data.birthDate").alias("birthDate"),
                        col("data.deceasedDateTime").alias("deceasedDateTime"),
                        col("data.multipleBirthBoolean").alias("multipleBirth"),
                        lit(None).alias("multipleBirthInteger"),
                        # col("data.multipleBirthInteger").alias("multipleBirthInteger"),
                        col("data.maritalStatus.coding").getItem(0).getItem("code").alias("maritalStatus")) \
                    .withColumn("multipleBirth", when(isnull(col("multipleBirth")), col("multipleBirthInteger")).otherwise(col("multipleBirth").cast(IntegerType()))) \
                    .drop("multipleBirthInteger")
        pass

    elif resource_type == "Observation":
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

        details_dfs["observation_result"] = observation_result.union(component_result).union(observation_valueCodeableConcept)

        details_dfs["observation_code"] = observation_code.union(observation_component.select("code", "display")).distinct()

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

    elif resource_type == "Condition":
        details_dfs["condition_code"] = df.select("data.code.coding") \
                          .withColumn("coding", explode("coding")) \
                          .select(
                              col("coding.code").alias("code"),
                              col("coding.display").alias("display")
                          ) \
                          .dropDuplicates(["code"])
        details_dfs["condition"] = df.select(
            col("data.id").alias("condition_id"),
            col("data.onsetDateTime").alias("onsetDateTime"),
            col("data.clinicalStatus").alias("clinicalStatus"),
            col("data.verificationStatus").alias("verificationStatus"),
            col("data.code.coding").getItem(0).getItem("code").alias("code"),
            extract_id_udf(col("data.subject.reference")).alias("patient_id"),
            extract_id_udf(col("data.context.reference")).alias("encounter_id"),
            col("data.abatementDateTime").alias("abatementDateTime")
        )
        pass

    elif resource_type == "MedicationRequest":
        medication_request = df.withColumn("context_id", extract_id_udf(col("data.context.reference"))) \
                        .withColumn("medication_code", 
                                           col("data.medicationCodeableConcept.coding").getItem(0).getItem("code") ) \
                        .withColumn("medication_request_id", concat_ws("_", "context_id", "medication_code")) \
                        .withColumn("medication_class",
                                  when(array_size(col("data.dosageInstruction")) == 0, lit("no instructions")) \
                                  .when(col("data.dosageInstruction").getItem(0).getItem("asNeededBoolean"), lit("as needed")) \
                                  .otherwise(lit("scheduled")))
        
        details_dfs["medication"] = medication_request.select("data.medicationCodeableConcept.coding") \
                              .withColumn("coding", explode("coding")) \
                              .select(
                                  col("coding.code").alias("code"),
                                  col("coding.display").alias("display")
                              ).dropDuplicates(["code"])
        details_dfs["dosage_instruction"] = medication_request.select("medication_request_id", "data.dosageInstruction") \
                                            .withColumn("dosage_instruction", explode("dosageInstruction")) \
                                            .select(
                                                "medication_request_id",
                                                col("dosage_instruction.asNeededBoolean").alias("asNeededBoolean"),
                                                col("dosage_instruction.sequence").alias("sequence"),
                                                col("dosage_instruction.timing.repeat.frequency").alias("frequency"),
                                                col("dosage_instruction.timing.repeat.period").alias("period"),
                                                col("dosage_instruction.timing.repeat.periodUnit").alias("periodUnit"),
                                                col("dosage_instruction.doseQuantity.value").alias("doseQuantity"),
                                                col("dosage_instruction.additionalInstructions").getItem(0).getItem("coding").getItem(0).getItem("code").alias("additionalInstructionsCode")
                                            )

        details_dfs["additional_instruction"] = medication_request.select("medication_request_id", "data.dosageInstruction") \
                                                .withColumn("dosage_instruction", explode("dosageInstruction")) \
                                                .withColumn("additional_instruction", explode("dosage_instruction.additionalInstructions.coding")) \
                                                .select(
                                                    "medication_request_id",
                                                    col("additional_instruction.code").alias("code"),
                                                    col("additional_instruction.display").alias("display")
                                                )

        details_dfs["dispense_request"] = medication_request.select(
            "medication_request_id",
            col("data.dispenseRequest.numberOfRepeatsAllowed").alias("numberOfRepeatsAllowed"),
            col("data.dispenseRequest.quantity.value").alias("quantiyValue"),
            col("data.dispenseRequest.quantity.unit").alias("quantityUnit"),
            col("data.dispenseRequest.expectedSupplyDuration.value").alias("expectedSupplyDurationValue"),
            col("data.dispenseRequest.expectedSupplyDuration.unit").alias("expectedSupplyDurationUnit")
        )


        medication_request = medication_request.select(
            "medication_request_id",
            extract_id_udf(col("data.patient.reference")).alias("patient_id"),
            col("data.dateWritten").alias("dateWritten"),
            extract_id_udf(col("data.context.reference")).alias("encounter_id"),
            col("data.medicationCodeableConcept.coding").getItem(0).getItem("code").alias("medicationCode"),
            col("data.status").alias("status"),
            col("data.reasonReference").alias("reasonReference"),
            "medication_class"
        )

        details_dfs["medication_request"] = medication_request.withColumn(
            "reasonReference", from_json(col("reasonReference"), reason_reference_schema).getItem(0).getItem("reference")) \
            .withColumn("reasonReference", when(isnotnull(col("reasonReference")), extract_id_udf(col("reasonReference"))))

        pass

    elif resource_type == "Immunization":
        details_dfs["vaccine_code"] = df.select("data.vaccineCode.coding") \
                                    .withColumn("coding", explode("coding")) \
                                    .select(
                                        col("coding.code").alias("code"),
                                        col("coding.display").alias("display")
                                    )

        details_dfs["immunization"] = df.select(
            extract_id_udf(col("data.patient.reference")).alias("patient_id"),
            extract_id_udf(col("data.encounter.reference")).alias("encounter_id"),
            col("data.vaccineCode.coding").getItem(0).getItem("code").alias("vaccineCode"),
            col("data.date").alias("date"),
            col("data.status").alias("status"),
            col("data.wasNotGiven").alias("wasNotGiven"),
            col("data.primarySource").alias("primarySource")
        )
        pass

    elif resource_type == "Procedure":
        procedure = df.withColumn(
            "reasonReference",
            from_json(col("data.reasonReference"), procedure_reason_schema).getItem("reference")
        )

        details_dfs["procedure_type"] = procedure.select("data.code.coding") \
                                .withColumn("coding", explode("coding")) \
                                .select(
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
            isnotnull(col("data.performedPeriod.start")),
            col("data.performedPeriod.start")
            ).otherwise(col("data.performedDateTime")).alias("performedPeriodStart"),
            when(
            isnotnull(col("data.performedPeriod.end")),
            col("data.performedPeriod.end")
            ).otherwise(col("data.performedDateTime")).alias("performedPeriodEnd")
        )
        pass

    elif resource_type == "CarePlan":
        careplan = df.withColumn("activity", explode("data.activity")) \
                      .withColumn("addresses", explode("data.addresses"))
        
        details_dfs["careplan_category"] = careplan.select("data.category") \
                                .withColumn("category", explode("category")) \
                                .withColumn("category", from_json("category", careplan_category_schema)) \
                                .select(
                                    col("category.coding").getItem(0).getItem("code").alias("code"),
                                    col("category.coding").getItem(0).getItem("display").alias("display")
                                )

        details_dfs["careplan_activity"] = careplan.select("activity") \
                                .select(
                                    col("activity.detail.code.coding").getItem(0).getItem("code").alias("code"),
                                    col("activity.detail.code.coding").getItem(0).getItem("display").alias("display"),
                                )

        careplan = careplan.select(
            extract_id_udf(col("data.subject.reference")).alias("patient_id"),
            extract_id_udf(col("data.context.reference")).alias("encounter_id"),
            extract_id_udf(col("addresses.reference")).alias("address_id"),
            col("data.status").alias("status"),
            col("activity.detail.status").alias("activityStatus"),
            col("data.period.start").alias("periodStart"),
            col("data.period.end").alias("periodEnd"),
            col("activity.detail.code.coding").getItem(0).getItem("code").alias("activityCode")
        )
        pass

    elif resource_type == "Encounter":
        encounter = df.withColumn("type", from_json("data.type", encounter_type_schema)) \
                            .withColumn("type", explode("type")) 

        details_dfs["encounter_type"] = encounter.select(
            col("type.coding").getItem(0).getItem("code").alias("code"),
            col("type.text").alias("display")
        ).dropDuplicates(["code"])

        details_dfs["encounter_reason"] = encounter.select("data.reason.coding")\
                                .withColumn("coding", explode("coding")) \
                                .select(
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
            col("data.period.end").alias("periodEnd"))
        pass

    elif resource_type == "DiagnosticReport":
        details_dfs["diagnostic_code"] = df.select("data.code.coding") \
                                .withColumn("coding", explode("coding")) \
                                .select(
                                    col("coding.code").alias("code"),
                                    col("coding.display").alias("display")
                                ).dropDuplicates(["code"])

        details_dfs["diagnostic_result"] = df.select("data.result", col("data.id").alias("diagnostic_id")) \
                                    .withColumn("result", explode("result")) \
                                    .select(
                                        "diagnostic_id",
                                        extract_id_udf(col("result.reference")).alias("result_id"),
                                        col("result.display").alias("display"),
                                    )

        details_dfs["diagnostic"] = df.select(
            col("data.id").alias("diagnostic_id"),
            col("data.status").alias("status"),
            col("data.issued").alias("issued"),
            col("data.effectiveDateTime").alias("effectiveDateTime"),
            extract_id_udf(col("data.subject.reference")).alias("patient_id"),
            extract_id_udf(col("data.encounter.reference")).alias("encounter_id"),
            col("data.code.coding").getItem(0).getItem("code").alias("report_code")
        )
        pass

    elif resource_type == "AllergyIntolerance":
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

    return details_dfs

def transform_data(df: ps.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    This function transforms data from file data with json columns to a flat table.
    Input:
    df:
        - path: str - path to the metadata file
        - data: dict - json data
    """
    df = df.withColumn("entry", explode("entry")) \
        .withColumn("resourceType", col("entry.resource.resourceType")) \
        .groupBy("resourceType") \
        .agg(collect_list("entry").alias("entries")) \
        .withColumn("data", explode("entries")) \
        .drop("entries")\
        .select("resourceType", col("data.resource").alias("data"))
    
    unique_resourceTypes = df.select("resourceType").distinct().collect()

    # Create a dictionary to store the transformed data
    details_dfs: Dict[str, ps.DataFrame] = {}
    for resource_type in unique_resourceTypes():
        resource = df.filter(col("resourceType") == resource_type)
        details_dfs.update(transform_resource_type(resource, resource_type))



from typing import Dict
import pyspark.sql.dataframe as pd
from pyspark.sql.functions import col, explode, lit, when, concat_ws, size, from_json

from .udf import extract_id_udf
from .schema import reason_reference_schema


def transform_medication_request(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    medication_request = df.withColumn("context_id", extract_id_udf(col("data.context.reference"))) \
            .withColumn("medication_code", 
                        col("data.medicationCodeableConcept.coding").getItem(0).getItem("code") ) \
            .withColumn("medication_request_id", concat_ws("_", "context_id", "medication_code")) \
            .withColumn("medication_class",
                    when(size(col("data.dosageInstruction")) == 0, lit("no instructions")) \
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
        col("data.dispenseRequest.quantity.value").alias("quantityValue"),
        col("data.dispenseRequest.quantity.unit").alias("quantityUnit"),
        col("data.dispenseRequest.expectedSupplyDuration.value").alias("expectedSupplyDurationValue"),
        col("data.dispenseRequest.expectedSupplyDuration.unit").alias("expectedSupplyDurationUnit")
    )

    details_dfs["medication_request"] = medication_request.select(
        "medication_request_id",
        extract_id_udf(col("data.patient.reference")).alias("patient_id"),
        col("data.dateWritten").alias("dateWritten"),
        extract_id_udf(col("data.context.reference")).alias("encounter_id"),
        col("data.medicationCodeableConcept.coding").getItem(0).getItem("code").alias("medicationCode"),
        col("data.status").alias("status"),
        col("data.reasonReference").alias("reasonReference"),
        "medication_class"
    ).withColumn(
        "reasonReference", from_json(col("reasonReference"), reason_reference_schema).getItem(0).getItem("reference")
    ).withColumn(
        "reasonReference", when(col("reasonReference").isNotNull(), extract_id_udf(col("reasonReference")))
    )
    return details_dfs

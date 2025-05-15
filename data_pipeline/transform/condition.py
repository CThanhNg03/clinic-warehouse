from typing import Dict
import pyspark.sql.dataframe as pd

from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StringType

from .udf import extract_id_udf

def categorize_condition(name: str) -> str:
    name = name.lower()
    if any(keyword in name for keyword in [
        'diabetes', 'insulin', 'retinopathy', 'microalbuminuria'
    ]):
        return 'Endocrine / Metabolic'
    elif any(keyword in name for keyword in [
        'cardiac arrest', 'hypertension'
    ]):
        return 'Cardiology'
    elif any(keyword in name for keyword in [
        'fracture', 'injury', 'sprain', 'concussion', 'osteoarthritis', 'meniscus',
        'laceration', 'rupture of patellar', 'thigh', 'hand', 'foot', 'burn', 'wound',
        'rupture', 'bullet'
    ]):
        return 'Trauma / Musculoskeletal'
    elif any(keyword in name for keyword in [
        'cancer', 'neoplasm', 'tumor', 'carcinoma'
    ]):
        return 'Oncology'
    elif any(keyword in name for keyword in [
        'asthma', 'bronchitis', 'sinusitis', 'copd', 'pneumonia', 'emphysema',
        'pharyngitis', 'sore throat', 'bronch'
    ]):
        return 'Respiratory'
    elif any(keyword in name for keyword in [
        'rheumatoid', 'gout', 'fibromyalgia'
    ]):
        return 'Rheumatology'
    elif any(keyword in name for keyword in [
        'rhinitis', 'otitis', 'allergic reaction'
    ]):
        return 'ENT / Allergy'
    elif any(keyword in name for keyword in [
        'migraine', 'stroke', 'brain', 'paralysis', 'alzheimer', 'dementia'
    ]):
        return 'Neurological'
    elif any(keyword in name for keyword in [
        'attention deficit', 'adhd', 'autism'
    ]):
        return 'Mental / Behavioral'
    elif any(keyword in name for keyword in [
        'pregnancy', 'miscarriage', 'eclampsia', 'infertility', 'uterine', 'ovum'
    ]):
        return 'Reproductive / Obstetrics'
    elif any(keyword in name for keyword in [
        'urinary', 'kidney', 'renal', 'cystitis', 'pyelonephritis'
    ]):
        return 'Urology / Nephrology'
    elif any(keyword in name for keyword in [
        'appendicitis', 'bleeding from anus', 'colon', 'diarrhea', 'rectal', 'polyp'
    ]):
        return 'Gastroenterology'
    elif any(keyword in name for keyword in [
        'history of appendectomy', 'history of amputation', 'history of cardiac arrest'
    ]):
        return 'Post-surgical History'
    elif 'overdose' in name:
        return 'Toxicology'
    else:
        return 'Other'
    
categorize_condition_udf = udf(categorize_condition, StringType())

def transform_condition(df: pd.DataFrame, details_dfs: Dict[str, pd.DataFrame]):
    condition = df.withColumn("coding", explode("data.code.coding"))

    details_dfs["condition_code"] = condition.select(
        col("coding.code").alias("code"),
        col("coding.display").alias("display"),
        categorize_condition_udf(col("coding.display")).alias("category")
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

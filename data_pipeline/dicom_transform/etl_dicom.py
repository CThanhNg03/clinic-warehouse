from hdfs import InsecureClient

from .udf import extract_dicom_metadata_udf, extract_image_data_udf

def ingest_dicom(file, target_path, dfs_config):
    """
        Ingest DICOM files from the local filesystem into system distributed storage.
    """
    client =  InsecureClient(dfs_config['url'], user=dfs_config['user'])
    client.makedirs(target_path, overwrite=True)
    client.upload(target_path, file, overwrite=True)

def extract_dicom(input_dir, spark):
    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.dcm").load(input_dir)

    df = df.withColumnRenamed("path", "file_path") \
            .withColumn("metadata", extract_dicom_metadata_udf(df['content'])) \
            .withColumn("image_data", extract_image_data_udf(df['content']))
    
    return df

def save_dicom(df, spark):
    """
        Save the DICOM DataFrame to HIVE table.
    """
    df.saveAsTable("dicom_data", format="parquet", mode="overwrite")

    return 1
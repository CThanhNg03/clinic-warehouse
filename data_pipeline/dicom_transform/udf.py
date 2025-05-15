from pyspark.sql.functions import udf
import pydicom

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import io

dicom_metadata_schema = StructType([
    StructField("PatientID", StringType(), True),
    StructField("StudyDate", StringType(), True),
    StructField("Modality", StringType(), True),
    StructField("StudyDescription", StringType(), True),
    StructField("SeriesDescription", StringType(), True)
])

def extract_dicom_metadata(file_content):
    """
    Extract metadata from a DICOM file.
    
    Args:
        file_content (str): file_content in bytes
        
    Returns:
        str: Metadata extracted from the DICOM file.
    """
    try:
        if isinstance(file_content, (bytes, bytearray)):
            buffer = io.BytesIO(file_content)
        ds = pydicom.dcmread(buffer)
        metadata = {
            "PatientID": ds.PatientID if hasattr(ds, 'PatientID') else None,
            "StudyDate": ds.StudyDate if hasattr(ds, 'StudyDate') else None,
            "Modality": ds.Modality if hasattr(ds, 'Modality') else None,
            "StudyDescription": ds.StudyDescription if hasattr(ds, 'StudyDescription') else None,
            "SeriesDescription": ds.SeriesDescription if hasattr(ds, 'SeriesDescription') else None
        }
        return metadata

    except Exception as e:
        return str(e)
    
def extract_image_data(file_content):
    """
    Extract pixel data from a DICOM file.
    
    Args:
        file_path (str): Path to the DICOM file.
        
    Returns:
        list: Pixel data extracted from the DICOM file.
    """
    try:
        if isinstance(file_content, (bytes, bytearray)):
            buffer = io.BytesIO(file_content)
        ds = pydicom.dcmread(buffer)
        if hasattr(ds, 'pixel_array'):
            return ds.pixel_array.flatten().tolist()
        else:
            return None
    except Exception as e:
        return str(e)
    
extract_dicom_metadata_udf = udf(lambda file_path: extract_dicom_metadata(file_path), dicom_metadata_schema)

extract_image_data_udf = udf(lambda file_path: extract_image_data(file_path), ArrayType(StringType()))


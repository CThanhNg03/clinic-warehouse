import dotenv
import os

dotenv.load_dotenv(override=True)

class SettingEnvironment:
    def __init__(self):
        self.env = os.getenv('ENV')
        self.ehr_source = os.getenv('EHR_PATH')
        self.dicom_source = os.getenv('DICOM_PATH')
        self.storage = {
            'type': os.getenv('STORAGE_TYPE'),
            'path': os.getenv('STORAGE_PATH'),
            'host': os.getenv('STORAGE_HOST'),
            'port': os.getenv('STORAGE_PORT'),
            'user': os.getenv('STORAGE_USER'),
        }
        
    def is_production(self):
        return self.env == 'production'
    
    def is_development(self):
        return self.env == 'development'
    
envi = SettingEnvironment()
import dotenv
import os

dotenv.load_dotenv(override=True)

class SettingEnvironment:
    DATABASE = os.getenv('STORAGE_NAME')
    STORAGE_LOCATION = os.getenv('STORAGE_PATH')

    BRONZE_LAYER = os.getenv('BRONZE_LAYER')
    SILVER_LAYER = os.getenv('SILVER_LAYER')

    INGESTED_BATCH_TABLE = os.getenv('INGESTED_BATCH_TABLE', 'ingested_batch')
    TRANSFORMED_BATCH_TABLE = os.getenv('TRANSFORM_BATCH_TABLE', 'transformed_batch')
    MODELED_BATCH_TABLE = os.getenv('MODELED_BATCH_TABLE', 'modeled_batch')

    EHR_PATH = os.getenv('EHR_PATH')

    WAREHOUSE_MODEL_YAML = os.getenv('WAREHOUSE_MODEL_YAML')

    def __init__(self):
        self.env = os.getenv('ENV')
        
    def is_production(self):
        return self.env == 'production'
    
    def is_development(self):
        return self.env == 'development'
    
envi = SettingEnvironment()
import dotenv
import os

dotenv.load_dotenv()

SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY')

SQLALCHEMY_DATABASE_URI = os.getenv('SUPERSET_DB_URI')
print(SQLALCHEMY_DATABASE_URI)
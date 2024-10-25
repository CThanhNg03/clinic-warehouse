from config.logger import logger
from data_source.ehr import EhrSource
from injection import data_injector

def main():
    ehr_source = data_injector.get(EhrSource)
    ehr_source.upload_data()

if __name__ == '__main__':
    main()
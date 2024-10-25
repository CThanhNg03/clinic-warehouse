import os
import json
import zipfile

from data_storage.abstract import AbstractStorage
from config.env import envi
from config.logger import logger

class EhrSource:
    """
    EHR data source class
    Data source for EHR data in JSON format, stored in a archived file (.zip).
    """
    def __init__(self, *, data_storage: AbstractStorage):
        self.data_path = envi.ehr_source
        self.data_storage = data_storage

    def stream_zip_data(self):
        # Open the archive file and stream the data
        with zipfile.ZipFile(self.data_path, 'r') as archive:
            for member in archive.namelist():
                with archive.open(member) as file:
                    yield file

    def upload_data(self):
        batch_size = 100
        for data in self.stream_zip_data():
            batch_size -= 1
            if batch_size == 0:
                logger.info('Uploading data...')
            self.data_storage.save(data=data, destination='ehr')


if __name__ == '__main__':
    ehr_source = EhrSource(data_path='E:/Thesis/archive.zip')
    gen = ehr_source.stream_zip_data()
    first = next(gen).get('entry')
    types = set()
    print(len(first))
    for res in first:
        resource_type = res.get('resource').get('resourceType')
        if resource_type:
            types.add(resource_type)
        else:
            print(res)
        types.add(res.get('resource').get('resourceType'))

    print(types)
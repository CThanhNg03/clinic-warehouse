from typing import IO
import os

from data_storage.abstract import AbstractStorage
from config.env import envi


class LocalStorage(AbstractStorage):
    def __init__(self):
        self.path = envi.storage.get('path')
        os.makedirs(self.path, exist_ok=True)

    def save(self, *, data: IO, destination: str):
        destination_path = os.path.join(self.path, destination)
        os.makedirs(destination_path, exist_ok=True)
        print(destination_path)
        file_name = '_'.join(data.name.split('/'))
        with open(os.path.join(destination_path, file_name), 'w') as f:
            content = data.read().decode('utf-8')
            f.write(content) 

    def load(self, *, source: str):
        try:
            with open(os.path.join(self.path, source), 'rb') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"The file {source} does not exist.")
    
    def delete(self, *, source):
        os.remove(os.path.join(self.path, source))

if __name__ == '__main__':
    storage = LocalStorage()
    with open('.test/test.json', 'rb') as f:
        storage.save(data=f, destination='ehr')
    print(storage.load(source='ehr/test.json'))
    storage.delete(source='ehr/test.json')
    print(storage.load(source='ehr/test.json'))
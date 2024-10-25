from data_storage.abstract import AbstractStorage
from config.env import envi

class HdfsStorage(AbstractStorage):
    def __init__(self):
        self.path = envi.storage.get('path')
        self.host = envi.storage.get('host')
        self.port = envi.storage.get('port')
        self.user = envi.storage.get('user')
        self.client = None

    def save(self, *, data, destination):
        return super().save(data=data, destination=destination)
    
    def load(self, *, source):
        return super().load(source=source)
    
    def delete(self, *, source):
        return super().delete(source=source)
from config.env import envi

from .local import LocalStorage
from .hdfs import HdfsStorage
from .abstract import AbstractStorage

def get_storage() -> AbstractStorage:
    """
    Get the storage object based on the configuration
    """
    storage_type = envi.storage.get('type')
    if storage_type == 'local':
        return LocalStorage()
    elif storage_type == 'hdfs':
        return HdfsStorage()
    else:
        raise ValueError(f'Unknown storage type: {storage_type}')
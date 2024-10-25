from injector import Injector, Module, Binder, singleton

from data_source.ehr import EhrSource
from data_storage import get_storage, AbstractStorage

class DIModule(Module):
    def __init__(self):
        self.storage = get_storage()

    def configure(self, binder: Binder):
        binder.bind(AbstractStorage, self.storage, scope=singleton)
        binder.bind(EhrSource, to=EhrSource(data_storage=self.storage), scope=singleton)

data_injector = Injector([DIModule()])

from abc import ABC, abstractmethod

class AbstractStorage(ABC):
    @abstractmethod
    def save(self, *, data, destination):
        pass

    @abstractmethod
    def load(self, *, source):
        pass

    @abstractmethod
    def delete(self, *, source):
        pass

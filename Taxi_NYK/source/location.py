from abc import ABC, abstractmethod
 
class Location(ABC):
 
    @abstractmethod
    def readObjectFile(self):
        pass


    @abstractmethod
    def readConfigFile(self):
        pass   
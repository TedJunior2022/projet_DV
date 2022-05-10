from abc import ABC, abstractmethod

class DbServer(ABC):

    @abstractmethod
    def connectTOdb(user, pwd):
        pass
    @abstractmethod
    def connectTOdbComplete(db_name, user, pwd):
        pass

    @abstractmethod
    def executeRequest(database, request):
        pass
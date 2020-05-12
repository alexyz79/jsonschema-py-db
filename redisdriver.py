from database import DatabaseDriver
from schema import JSONSchemaObject
from rejson import Client, Path
from jsonpath import parse

class RedisDriver(DatabaseDriver):

    _host = "localhost"
    _port = 6379
    _client = None
    
    def __init__(self, host:str="localhost", port:int=6379):
        self._host = host
        self._port = port
        self._client = Client(
            host=host, port=port, decode_responses=True,
            encoder=JSONSchemaObject.JSONSchemaEncoder() )

    def find_one(self,schema:str, query:str):
        pass

    def find_all(self,schema:str, query:str):
        pass

    def delete_one(self,schema:str, query:str):
        pass

    def delete_all(self,schema:str, query:str):
        pass

    def save_one(self,schema:str, query:str):
        pass

    def save_all(self,schema:str, query:str):
        pass



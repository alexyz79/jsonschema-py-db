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

    def find_by_ref(self, ref:str):

        return self._client.jsonget(ref)
        

    def find_id_by(self, idx:str, value:str, version:str):

        result = []
        for member in self._client.smembers("{}:{}".format(idx,value)):
            
            if version == "all":
                result.append(member)
                continue

            # we split the index to check against the version
            idxs = str(member).split(":")

            # the _version is the second token of idxs
            if idxs[1] == version:
                result.append(member)
            
        return result

    def save(self, obj_list: list, indexed_attrs: list):

        # First cycle is just to verify if we do not have any
        # index integrity violation        
        for obj in indexed_attrs:

            # We do not store neither _id or _version
            if  obj[1] == "_id" or obj[1] == "_version":
                continue

            if obj[2] is None or obj[2] == "":
                raise ValueError("Indexed value {} must not be empty".format(obj[1]))            

            # the indexed is composed by schema path:indexes:attr_name 
            indexed_key = store_name = "{}:indexes:{}:{}".format(obj[0], obj[1], obj[2])
            
            # we already have this key let's get any value and make 
            # sure we this belongs to the same id            
            for member in self._client.smembers(indexed_key):
                # we only need to use one element since the _id MUST be equal
                idxs = str(member).split(":")

                # the _id is the first token of idxs, check if we recieved the same
                # id, if not this is a index violation
                if not str(obj[3]).startswith(idxs[0]):
                    raise ValueError("{}:{} not unique, another object already have that value".format(obj[1], obj[2]))
                
                # we just need one iteration
                break

        # this cycle we just store the indexes
        for obj in indexed_attrs:

            if obj[2] is None or obj[2] == "" or obj[1] == "_id" or obj[1] == "_version":
                continue
                  
            # Set the store name and store data
            store_name = "{}:indexes:{}:{}".format(obj[0], obj[1], obj[2])
            store_data = obj[3]
            self._client.sadd(store_name,store_data)

        # We now store the actual objects, and return the added ids
        ids = []
        for obj in obj_list:

            # Set the store name and store data
            store_name = "{}:{}".format(obj[0], obj[1])
            store_data = obj[2]
            self._client.jsonset(store_name, Path.rootPath(), store_data)
            ids.append(obj[1])

        return ids

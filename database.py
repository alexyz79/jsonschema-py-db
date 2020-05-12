from schema import JSONSchemaObject, JSONSchemaArray
import uuid

class DatabaseDriver(object):
    pass

class DatabaseLayer(object): 

    @staticmethod
    def _extract_relations(obj:JSONSchemaObject):

        # result is a list (last schema path, last _id, list of objects to save )
        schema_path = None
        last_id = None
        d = {}
        obj_list = []

        # Check if this object has an _id field, 
        # if it have then means it is probably related to
        # something
        if "_id" in obj.__dict__["__attrs__"]:
            
            # If _id is empty means we must add an ID, we are most probably 
            # saving the object for the first time
            if obj.__dict__["__attrs__"]["_id"] == "" or obj.__dict__["__attrs__"]["_id"] is None:
                obj.__dict__["__attrs__"]["_id"] =  str(uuid.uuid4())
            
            # we set the second element of our tuple as our id
            schema_path = obj._schema_path
            last_id = obj.__dict__["__attrs__"]["_id"]

        # now we copy all attrs to our new dict
        for key, value in obj.__dict__["__attrs__"].items():
            
            # If this object we need to check for a one to one relation
            if isinstance(value,JSONSchemaObject):
                
                # Try to extract the relation of the object
                relation = DatabaseLayer._extract_relations(value)

                # check if we have a relation
                if relation[0] is not None: 
                    d[key] = "{}:{}".format(relation[0],relation[1])
                else:
                    d[key] = relation[2]

                obj_list.extend(relation[3])

            # If this is an array we must check for a one to many relation
            elif isinstance(value,JSONSchemaArray):

                d[key] = []
                for svalue in value:

                    # If this object we need to check for a one to one relation
                    if isinstance(svalue,JSONSchemaObject):
                        
                        # Try to extract the relation of the object
                        relation = DatabaseLayer._extract_relations(svalue)

                        # check if we have a relation
                        if relation[0] is not None: 
                            d[key].append("{}:{}".format(relation[0],relation[1]))
                        else:
                            d[key].append(relation[2])

                        obj_list.extend(relation[3])

                    else:
                        d[key].append(svalue)
            else:
                d[key] = value
            
        if "_id" in obj.__dict__["__attrs__"]:
            obj_list.append([obj._schema_path, obj.__dict__["__attrs__"]["_id"], d])

        return (schema_path,last_id,d,obj_list)

    @staticmethod
    def store(obj:JSONSchemaObject,ref:str=None):

        relations = DatabaseLayer._extract_relations(obj)

        if ref == "" and relations[1] is None:
            raise AttributeError("Schema must have an unique key field or must pass ref name")

        for r in relations[3]:

            # Set the store name and store data
            store_name = "{}.{}".format(r[0],r[1])
            store_data = r[2]

            print(store_name)
            print(store_data)
            # # # Connect if needed and store the object in redis
            # RedisObject._connect()
            # RedisObject._client.jsonset(store_name, Path.rootPath(), store_data)

        # we return the reference just because it might
        # be needed
        return True

    def delete(self):
        pass


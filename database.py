from schema import JSONSchemaObject, JSONSchemaArray
import uuid


class DatabaseDriver(object):
    """
    Interface for the database driver
    """

    def find_one(self, str, query: str):
        raise NotImplementedError()

    def find_all(self, query: str):
        raise NotImplementedError()

    def delete(self, obj_list: list):
        raise NotImplementedError()

    def find_by_ref(self, ref:str):
        raise NotImplementedError()

    def find_id_by(self, idx:str, value:str, version:str):
        raise NotImplementedError()

    def save(self, obj_list: list, indexed_attrs: list):
        raise NotImplementedError()


class NullDriver(DatabaseDriver):
    """
    A Null driver for debuggin
    """

    def find_by_ref(self, ref:str):
        pass

    def find_id_by(self, idx:str, value:str, version:str):
        pass

    def save(self, obj_list: list, indexed_attrs: list):

        ids = []
        for obj in obj_list:

            # Set the store name and store data
            store_name = "{}:{}".format(obj[0], obj[1])
            store_data = obj[2]
            # print(store_name)
            # print(store_data)
            ids.append(obj[1])

        for obj in indexed_attrs:

            # We do not store empty index values neither _id or _version
            if obj[2] is None or obj[2] == "":
                continue

            # Set the store name and store data
            store_name = "{}:indexes:{}:{}".format(obj[0], obj[1], obj[2])
            store_data = obj[3]
            print(store_name)
            print(store_data)
            ids.append(obj[1])

        return ids


class DatabaseLayer(object):

    @staticmethod
    def _extract_relations(obj: JSONSchemaObject):

        # result is always a tuple (last schema path, last _id, list of objects to save, indexed_attrs )
        schema_path = None
        last_id = None
        d = {}
        obj_list = []
        indexed_attrs = []

        # Check if this object has an _id field,
        # if it have then means it is probably related to
        # something
        if "_id" in obj.__dict__["__attrs__"]:

            # If _id is empty means we must add an ID, we are most probably
            # saving the object for the first time
            if obj.__dict__["__attrs__"]["_id"] == "" or obj.__dict__["__attrs__"]["_id"] is None:
                obj.__dict__["__attrs__"]["_id"] = str(uuid.uuid4())

            # we set the second element of our tuple as our id
            schema_path = obj._schema_path
            last_id = obj.__dict__["__attrs__"]["_id"]

            # We can only have version of objects with id's
            if "_version" in obj.__dict__["__attrs__"]:

                # If _id is empty means we must add an ID, we are most probably
                # saving the object for the first time
                if obj.__dict__["__attrs__"]["_version"] == "" or obj.__dict__["__attrs__"]["_version"] is None:
                    obj.__dict__["__attrs__"]["_version"] = "latest"

                # we set the second element of our tuple as our id
                schema_path = obj._schema_path
                last_id = "{}:{}".format(last_id, obj.__dict__[
                                         "__attrs__"]["_version"])

        # now we copy all attrs to our new dict
        for attr_name, value in obj.__dict__["__attrs__"].items():

            # To have indexed names we must have an _id on the schema
            if str(attr_name).startswith("_") and last_id is not None:
                indexed_attrs.append(
                    (obj._schema_path, attr_name, value, last_id))

            # If this object we need to check for a one to one relation
            if isinstance(value, JSONSchemaObject):

                # Try to extract the relation of the object
                relation = DatabaseLayer._extract_relations(value)

                # check if we have a relation
                if relation[0] is not None:
                    # set the value of this attr to the ref
                    d[attr_name] = "ref:{}:{}".format(relation[0], relation[1])
                else:
                    # otherwisw just add the returned json
                    d[attr_name] = relation[2]

                # append to our object list the returned objects if any
                obj_list.extend(relation[3])

                # Append our indexed attrs if any
                indexed_attrs.extend(relation[4])

            # If this is an array we must check for a one to many relation
            elif isinstance(value, JSONSchemaArray):

                d[attr_name] = []
                for svalue in value:

                    # If this object we need to check for a one to one relation
                    if isinstance(svalue, JSONSchemaObject):

                        # Try to extract the relation of the object
                        relation = DatabaseLayer._extract_relations(svalue)

                        # check if we have a relation
                        if relation[0] is not None:
                            # set the value of this attr to the ref
                            d[attr_name].append("ref:{}:{}".format(
                                relation[0], relation[1]))
                        else:
                            # otherwisw just add the returned json
                            d[attr_name].append(relation[2])

                        # append to our object list the returned objects if any
                        obj_list.extend(relation[3])

                        # Append our indexed attrs if any
                        indexed_attrs.extend(relation[4])

                    else:
                        # append to our json this value
                        d[attr_name].append(svalue)
            else:
                d[attr_name] = value

        # if this object have an _id we must append this json to the object list
        if last_id is not None:
            obj_list.append(
                [obj._schema_path, last_id, d])

        # result is always a tuple ( last schema path, last _id, list of objects to save )
        return (schema_path, last_id, d, obj_list, indexed_attrs)

    def __init__(self, drv: DatabaseDriver = NullDriver()):
        self._driver = drv

    def store(self, obj: JSONSchemaObject, ref: str = ""):

        relations = DatabaseLayer._extract_relations(obj)

        # we get the last inserted id
        last_id = relations[1]
        obj_list = relations[3]
        indexed_attrs = relations[4]

        if last_id is None:

            # with last id or without ref name we can't store the object
            if ref == "":
                raise AttributeError(
                    "Schema must have an unique key field or must pass ref name")

            # when the schema does not provide an _id field, the object will
            # not be returned in the obj_list, so we must append it in the
            # right format [ schema path, id, json]
            json = relations[2]
            obj_list.append([obj._schema_path, ref, json])

        return self._driver.save(obj_list, indexed_attrs)

    def find_by_ref(self, schema_name:str, ref:str):

        # fetch the schema first
        schema = JSONSchemaObject.get_schema(schema_name)
    
        if "_id" not in schema["properties"]:
            return None

        ref = "{}:{}".format(schema_name,ref)
        
        return self._driver.find_by_ref(ref)


    def find_all_by(self, schema_name:str, idx:str, value:str, version:str="all"):

        def _search_references(json:dict):

            for attr,value in json.items():

                if isinstance(value,list):
                    
                    result = []
                    for e in value:
                        if not isinstance(e,dict):
                            result.append(e)
                            continue
                        result.append(_search_references(e))

                    json[attr] = result

                elif isinstance(value,dict):
                    json[attr] = _search_references(value)
                elif str(value).startswith("ref:"):
                    ref = str(value)[4:] # we remove the ref: part
                    json[attr] = self._driver.find_by_ref(ref)

            return json

        # fetch the schema first
        schema = JSONSchemaObject.get_schema(schema_name)
        u_idx = "_{}".format(idx)
    
        if u_idx in schema["properties"]:
            idx = u_idx
        elif idx not in schema["properties"]:
            return None

        idx = "{}:indexes:{}".format(schema_name,idx)
        refs = self._driver.find_id_by(idx,value,version)
        obj_list = []

        for ref in refs:
        
            json = _search_references(self.find_by_ref(schema_name,ref))
            json_object = JSONSchemaObject.from_json(schema_name,json)
        
            if json_object is None:
                continue
            obj_list.append(json_object)
        
        return obj_list


    def find_one_by(self, schema_name:str, idx:str, value:str, version:str="all"):

        obj_list = self.find_all_by(schema_name,idx,value,version)

        if len(obj_list) > 0:
            return obj_list[0]

        return None

    def find_all(self, query: str):
        pass

    def delete(self, ref: list):
        pass

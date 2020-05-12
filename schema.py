'''
PyLib - Datalayer Python API
'''
from datetime import datetime
from json import JSONDecodeError, JSONDecoder, JSONEncoder, load, loads
from sys import modules
from typing import NewType
from jsonpath import parse
import uuid

"""
Defaults types interfaces used by the API
"""


class JSONSchemaException(BaseException):
    '''
    A simple schema exception
    '''

class JSONSchemaArray(object):
    """ 
    This class represents a JSON array
    For more information check:
    https://json-schema.org/understanding-json-schema/reference/array.html  
    """

    @staticmethod
    def _validate_array_value(property_info: dict, schema_name: str, value: object):
        
        if "$ref" in property_info:

            # a reference to another object, normalize the reference if
            # it is an anchor in our schema
            ref = property_info["$ref"]
            if str(ref).startswith("#"):
                ref = str(ref).replace("#", schema_name)

            # We recieved a None just create an empty object
            if value is None:
                obj = JSONSchemaObject(
                    schema_name=schema_name, schema_path=ref)

            # we recieved JSONSchemaObject, check if it's of the same type
            elif isinstance(value, JSONSchemaObject):
                if value.schema_path != ref:
                    raise ValueError(
                        "value not an JSONSchemaObject of type {}".format(ref))

                obj = value

            # dict can also be converted into a json object
            elif isinstance(value, dict):
                obj = JSONSchemaObject(
                    schema_name=schema_name, schema_path=ref, **value)

            else:
                # we recieved a invalid type for a ref
                raise ValueError("{} is not supported".format(type(value)))

            return obj

        else:

            # Nested arrays are not supported
            if property_info["type"] == "array":
                raise NotImplementedError("Nested arrays are not supported")

            # We must assure that we are sending a value of the
            # right type
            py_type = JSONSchemaObject._get_python_type(property_info)
                
            # If we just recieved a None just returns the default value
            if value is None:
                return JSONSchemaObject._get_default_value(py_type)

            # type of value must be the same type of the schema
            if type(value) is not py_type:
                raise ValueError("value is not of type {}".format(py_type))

            return value


    def __init__(self, attribute_name:str, schema_name:str, schema_path):

        # Initialize our attributes
        self._attribute_name = attribute_name
        self._schema_path = schema_path
        self._schema_name = schema_name
        self.__dict__['__array__'] = []


    def __len__(self):
        """
        Return the length of the array
        """
        # Get attribute schema
        attribute_name = self._attribute_name
        schema = JSONSchemaObject.get_schema(self._schema_path)[
            'properties'][attribute_name]

        # Check if we are a reference to other object
        if "$ref" in schema["items"]:
            # a reference to another object, just try to return the len of the array
            return len(self.__dict__["__array__"])
        else:
            # depending on the items definition we might have more then one
            # per value so return the array size / num items
            return int(len(self.__dict__["__array__"])/len(schema["items"]))

    def __delitem__(self, key):
        """
        Removes a item from the array
        """
        # Key must be an integer
        if not isinstance(key, int):
            raise ValueError("Can only delete by index")

        # Get attribute schema
        attribute_name = self._attribute_name
        schema = JSONSchemaObject.get_schema(self._schema_path)[
            'properties'][attribute_name]

        # Check if we are a reference to a schema
        if "$ref" in schema["items"]:

            # a reference to another object, check bounds and delete object
            if key >= len(self.__dict__["__array__"]):
                raise IndexError("Index out of bounds")

            del self.__dict__["__array__"][key]

        else:

            # depending on the schema definition we might have a tuple
            # per value so delete the N consecutive items
            # for more information check:
            # https://json-schema.org/understanding-json-schema/reference/array.html#tuple-validation
            if key >= len(self.__dict__["__array__"])/len(schema["items"]):
                raise IndexError("Index out of bounds")

            # compose our slice limits and delete the tuple/element
            start = key*len(schema["items"])
            stop = start + len(schema["items"])
            del self.__dict__["__array__"][start:stop]

    def __getitem__(self, key):
        """
        Returns a item from the array 
        """
        # Key can only be integers or slices
        if not isinstance(key, int) and not isinstance(key, slice):
            raise ValueError("Key must be an integer or a slice")

        # Slices aren't supported yet, altought it might be very straight forward
        # to implement it
        if isinstance(key, slice):
            raise NotImplementedError

        # Get attribute schema
        attribute_name = self._attribute_name
        schema = JSONSchemaObject.get_schema(self._schema_path)[
            'properties'][attribute_name]

        # Check if we are a reference to other schema
        if "$ref" in schema["items"]:

            # a reference to another object, just check bounds and
            # return the object at the specified index
            if key >= len(self.__dict__["__array__"]):
                raise IndexError("Index out of bounds")

            return self.__dict__["__array__"][key]

        else:
            # depending on the schema of the attribute we might have a tuple
            # first we check the bounds ( 0 <= key < array length / schema items length)
            # for more information check:
            # https://json-schema.org/understanding-json-schema/reference/array.html#tuple-validation
            if key >= len(self.__dict__["__array__"])/len(schema["items"]):
                raise IndexError("Index out of bounds")

            # compose the slice limits and retur the tuple
            start = key*len(schema["items"])
            stop = start + len(schema["items"])
            return self.__dict__["__array__"][start:stop]

    def __setitem__(self, key, value):
        """
        Set a element of the array with value at the specified the index
        """

        # Key can only be integer
        if not isinstance(key, int):
            raise ValueError("Key must be an integer")

        # Get attribute schema
        attribute_name = self._attribute_name
        schema = JSONSchemaObject.get_schema(self._schema_path)[
            'properties'][attribute_name]

        # Are we a array of tuples?
        if type(schema["items"]) is list:
            
            # our array is a tuple
            if not isinstance(value, tuple) and not isinstance(value, list):
                raise ValueError("Value must be of type tuple or list")

            # Check bounds, we just need to check the end element, meaning
            # key + len of items in schema >= length of our array
            if key+len(schema["items"]) >= len(self.__dict__["__array__"]):
                raise IndexError("Index out of bounds")

            # run throught the sent list/tuple verify type and add it
            # to our array
            i = 0
            for item in schema["items"]:

                if i < len(value):
                    element = value[i]
                else:
                    element = None

                obj = JSONSchemaArray._validate_array_value(
                    item, self._schema_name, element)

                self.__dict__["__array__"][key+i] = obj
                i += 1

        else:

            # is not a tuple, validate value and set it on our array
            self.__dict__["__array__"][key] = JSONSchemaArray._validate_array_value(
                schema["items"],
                self._schema_name, value)

    def __iter__(self):
        self.__iter_index__ = 0
        return self

    def __next__(self):
        # Get attribute schema
        attribute_name = self._attribute_name
        schema = JSONSchemaObject.get_schema(self._schema_path)[
            'properties'][attribute_name]

        # Check if we are a reference to other object
        if "$ref" in schema["items"]:
            # a reference to another object, just try to return the key
            if self.__iter_index__ >= len(self.__dict__["__array__"]):
                raise StopIteration

            obj = list(self.__dict__["__array__"])[self.__iter_index__]
        else:
            # depending on the items definition we might need to send more
            # then one, so send a slice of our array
            if self.__iter_index__ >= len(self.__dict__["__array__"])/len(schema["items"]):
                raise StopIteration

            start = self.__iter_index__*len(schema["items"])
            stop = start + len(schema["items"])
            obj = self.__dict__["__array__"][start:stop]

        self.__iter_index__ += 1
        return obj

    def __str__(self):
        return str(self.__dict__["__array__"])

    def __contains__(self, *args, **kwargs):
        raise NotImplementedError

    def append(self, *args, **kwargs):

        # Get attribute schema
        attribute_name = self._attribute_name
        schema = JSONSchemaObject.get_schema(self._schema_path)[
            'properties'][attribute_name]

        # Are we a array of tuples?
        if type(schema["items"]) is list:
            
            # run throught the sent list/tuple verify type and add it
            # to our array
            i = 0
            for item in schema["items"]:

                if i < len(args):
                    element = args[i]
                else:
                    element = None

                obj = JSONSchemaArray._validate_array_value(
                    item, self._schema_name, element)

                list.append(self.__dict__["__array__"], obj)
                i += 1

        else:

            # is not a tuple, validate value append it to our array

            # depending on the schema we might need to validate
            # the kwargs or args, by default we use kwargs
            element = kwargs
            if "$ref" in schema["items"]:
                # a ref to a schema is a object, pass the kwargs
                if self._attribute_name in kwargs:
                     element = kwargs[self._attribute_name]

            elif schema["items"]["type"] == "object":
                # a object type pass the kwargs
                if self._attribute_name in kwargs:
                     element = kwargs[self._attribute_name]
                
            elif schema["items"]["type"] == "array":
                # Nested arrays are not supported
                raise NotImplementedError("Nested arrays are not supported")
            else:
                # other types we can pass the first element
                if len(args) != 1:
                    raise ValueError("Value can't be of type list/typle")
                element = args[0]

            list.append(self.__dict__["__array__"], JSONSchemaArray._validate_array_value(
                schema["items"],
                self._schema_name, element))

    def count(self):
        return self.__len__()

    def index(self, item):
        raise NotImplementedError

    def insert(self, item):
        raise NotImplementedError

    def pop(self):
        raise NotImplementedError

    def reverse(self):
        raise NotImplementedError

    def sort(self):
        raise NotImplementedError


class JSONSchemaObject(object):
    '''
    A general schema, the definition is stored in a json file
    '''
    _schemas_url = "file://schema"
    _schemas_version = "latest"
    _schemas_cache = {}
    _models_cache = {}
    __internals__ = [
        '__dict__',
        '__class__',
        '_generate_from_schema',
        '_generate_attribute',
        '_schema_name',
        '_schema_path',
        'to_json',
        'from_json'
    ]


    class JSONSchemaEncoder(JSONEncoder):
        '''
        SchemaModel to JSON encoder
        '''

        def default(self, o):   # pylint: disable=method-hidden
            if isinstance(o, JSONSchemaObject):
                return o.__dict__["__attrs__"]
            elif isinstance(o, JSONSchemaArray):
                return o.__dict__["__array__"]
            elif isinstance(o,uuid.UUID):
                return str(o)
            else:
                # call base class implementation which takes care of
                # raising exceptions for unsupported types
                return JSONEncoder.default(self, object)

    def to_json(self):
        '''
        Serialize class to json
        '''
        return JSONSchemaObject.JSONSchemaEncoder().encode(self)

    @staticmethod
    def from_json(schema_name:str,json: object):
        '''
        Deserialize class from json
        '''
        if isinstance(json, str):
            json = JSONDecoder().decode(json)

        if isinstance(json, dict):

            if schema_name in JSONSchemaObject._models_cache:
                T = JSONSchemaObject._models_cache[schema_name]
                return T(**json)

            return JSONSchemaObject(schema_name=schema_name,**json)
        
        raise NotImplementedError

    @staticmethod
    def set_schemas_location(uri: str, version: str = "latest"):
        """
        Set the schemas location and version
        """
        JSONSchemaObject._schemas_url = uri
        JSONSchemaObject._schemas_version = version

    @staticmethod
    def set_schema(name: str, schema: object):
        """
        Store a JSON schema in the internal cache
        """
        if isinstance(schema, str):
            try:
                schema = loads(schema)
            except JSONDecodeError:
                raise JSONSchemaException("Invalid schema")

        JSONSchemaObject._schemas_cache[name] = schema

    @staticmethod
    def get_schema(name: str):
        """
        Get a JSON schema from the internal cache or try to download it 
        if not available
        """
        # we always user lowercase
        name = name.lower()
        definition = None

        # we are trying to get a definition
        if name.find("definitions") > 0:
            parts = name.split('/')

            if len(parts) < 3:
                raise JSONSchemaException("Invalid definition name")

            name = parts[0]
            definition = parts[2]

        # Check if schema is already available
        if name not in JSONSchemaObject._schemas_cache:
            JSONSchemaObject._retrieve_schema(name)

        # Get the schema from our cache
        schema = JSONSchemaObject._schemas_cache[name]

        if definition is None:
            # We want the all schema
            return schema

        # Check if schema have the definitions
        if "definitions" not in schema:
            raise JSONSchemaException(
                "Schema {} does not provide definitions".format(name))

        # Check if schema have the definitions
        if definition not in schema["definitions"]:
            raise JSONSchemaException(
                "Definition {} not in schema {}".format(definition, name))

        # we want just a definition
        return schema["definitions"][definition]

    @staticmethod
    def new_model(name: str):
        def new_schema_model():
            return JSONSchemaObject(schema_name=name)
        new_schema_model.__name__ = name
        return new_schema_model

    @staticmethod
    def get_attr_schema(schema_name:str,jpath:str):

        schema = JSONSchemaObject.get_schema(schema_name)
        return parse(jpath).find(schema)        


    @staticmethod
    def _retrieve_schema(name: str):
        """
        Download a schema from the defined URI
        """

        if str(JSONSchemaObject._schemas_url).startswith("file://"):

            # Try to open the schema from the defined location
            try:
                # Handle file:// -> filesystem access

                # Clean the file:// from the url
                folder = str(JSONSchemaObject._schemas_url).replace(
                    "file://", "")

                # Load json schema from the file
                with open(
                        "{}/{}/{}.json".format(folder, JSONSchemaObject._schemas_version, name)) as def_file:
                    JSONSchemaObject.set_schema(name, load(def_file))

                return JSONSchemaObject._schemas_cache[name]

            except JSONDecodeError:
                raise JSONSchemaException("Invalid schema")
            except FileNotFoundError:
                raise JSONSchemaException("Schema not found")
        elif str(JSONSchemaObject._schemas_url).startswith("http://") or \
                str(JSONSchemaObject._schemas_url).startswith("https://"):
            # Handle http(s):// -> http download
            raise NotImplementedError(
                "HTTP/HTTPS protocol not implemented yet")
        else:
            raise NotImplementedError("URI {} not supported".format(
                JSONSchemaObject._schemas_url))

    @staticmethod
    def _get_python_type(property_info):
        """
        Returns the the equivalent JSON type in Python
        """
        if property_info["type"] == "string":
            # A string
            return str
        elif property_info["type"] == "integer":
            # A Number
            return int
        elif property_info["type"] == "number":
            # A Decimal
            return float
        elif property_info["type"] == "array":
            # A List
            return JSONSchemaArray
        elif property_info["type"] == "object":
            if "properties" in property_info:
                # if we have a properties, it is a JSONSchemaObject
                return JSONSchemaObject
            # otherwise is just a dictionary
            return dict
        elif property_info["type"] == "boolean":
            # A boolean
            return bool
        elif property_info["type"] == "null":
            # A null
            return None
        else:
            # Unknown type
            raise JSONSchemaException(
                "Unknown data type {}".format(property_info["type"]))

    @staticmethod
    def _validate_value(name:str,schema_path:str,value:object):
        """
        Validates the value against the schema,
        """
        # For now we only do a basic validation, the goal is not to 
        # make the attribute assignment too complicated, a proper validation 
        # should be done before storing the structure 
        
        # Get the current schema so we can validate the data
        attr_schema = JSONSchemaObject.get_schema(schema_path)["properties"][name]

        # the null type is special kind of type
        if attr_schema["type"] == "null":
            # A null is a None
            return value == None

        # Get the current python type to compare
        py_type = JSONSchemaObject._get_python_type(attr_schema)

        if attr_schema["type"] == "array":
            # A List
            return type(value) is JSONSchemaArray or type(value) is list
        
        if attr_schema["type"] == "object":
            
            if "properties" in attr_schema:
                # if we have a properties, it is a JSONSchemaObject
                return type(value) is JSONSchemaObject or type(value) is dict

            # otherwise is just a dictionary
            return dict

        return type(value) is py_type

    @staticmethod
    def _get_json_type(T: type):
        """
        Returns the the equivalent JSON type in Python
        """
        if T is str:
            # A string
            return "string"
        elif T is int:
            # A Number
            return "integer"
        elif T is float:
            # A Decimal
            return "number"
        elif T is list:
            # A List
            return "array"
        elif T is dict:
            # A dict
            return "object"
        elif T is JSONSchemaArray:
            # A JSON Schema Object
            return "array"
        elif T is JSONSchemaObject:
            # A JSON Schema Object
            return "object"
        elif T is bool:
            # A null
            return "boolean"
        else:
            # Unknown type
            raise JSONSchemaException(
                "Unknown data type {}".format(T))

    @staticmethod
    def _get_default_value(T: type, schema_name:str=None,schema_path:str=None,attribute_name:str=None,**kwargs):
        """
        Get the default value for the T type, from the schema, or 
        return the default value for the T type in Python 
        """

        # If we path schema_path and attribute name we must check if a
        # default value as been declared in our schema
        if schema_path is not None and attribute_name is not None:
            schema = JSONSchemaObject.get_schema(schema_path)
            attr_schema = schema["properties"][attribute_name]
            if "default" in attr_schema:
                return T(attr_schema["default"])

        # Otherwise we return the Python default values
        if T is str:
            # A string
            return ""
        elif T is int:
            # A Number
            return 0
        elif T is float:
            # A Decimal
            return 0.0
        elif T is list:
            # A JSON array
            return JSONSchemaArray(
                attribute_name=attribute_name,
                schema_name=schema_name,
                schema_path=schema_path)
        elif T is dict:
            # A JSON Schema Object
            return JSONSchemaObject(schema_name=schema_name,**kwargs)
        elif T is JSONSchemaArray:
            # A JSON array
            # We must recieve this parameters to be able to work properly
            return JSONSchemaArray(
                attribute_name=attribute_name,
                schema_name=schema_name,
                schema_path=schema_path)

        elif T is JSONSchemaObject:
            # A JSON Schema Object
            return JSONSchemaObject(schema_name=schema_name,**kwargs)
        elif T is bool:
            # A boolean
            return False
        elif T is None:
            # A boolean
            return None
        else:
            # Unknown type
            raise JSONSchemaException(
                "Unknown data type {}".format(T))

    def __init__(self, **kwargs):
        """
        The constructor of the class
        """
        # By default we use the name of the classname as a reference
        if self.__class__.__name__ != JSONSchemaObject.__name__:
           
            # Get the schema from the class name
            self._schema_name = self.__class__.__name__.lower()
            
            # Store the class on our classes cache for reflection
            if self._schema_name not in JSONSchemaObject._models_cache:
                JSONSchemaObject._models_cache[self._schema_name] = self.__class__

        elif "schema_name" in kwargs:

            # otherwise check if we are sending the schema name on
            # kwargs
            self._schema_name = kwargs["schema_name"]
            del kwargs["schema_name"]
        
        else:
            # This cannot happen
            raise NotImplementedError("Invalid classname")

        schema_path = kwargs.get("schema_path")

        # get the schema from our store
        if schema_path is None:
            self._schema_path = self._schema_name
            schema = JSONSchemaObject.get_schema(
                str(self._schema_name).lower())
        else:
            self._schema_path = schema_path.lower()
            schema = JSONSchemaObject.get_schema(
                str(self._schema_path).lower())
            del kwargs["schema_path"]

        # Generate the class attributes
        self.__dict__["__attrs__"] = self._generate_from_schema(
            schema, **kwargs)

    def _generate_from_schema(self, schema: dict, **kwargs):
        '''
        Dynamically generate the class from schema
        '''
        obj = {}

        for attribute_name, property_info in dict(schema["properties"]).items():

            if "$ref" in property_info:
                
                # a reference to another object
                ref = property_info["$ref"]
                
                # check if it's an anchor reference
                if str(ref).startswith("#"):
                    ref = str(ref).replace(
                        "#", self._schema_name)

                # Check if attribute as been passed in kwargs
                if attribute_name in kwargs:
                    # passed, we send them instead of the all kwargs
                    obj[attribute_name] = JSONSchemaObject(
                        schema_name=ref, schema_path=ref, **(kwargs[attribute_name]))
                else:
                    # not passed, we do not send anything
                    obj[attribute_name] = JSONSchemaObject(
                        schema_name=ref, schema_path=ref)

            elif "type" in property_info:
                # it's not a reference, it is a defined type
                obj[attribute_name] = self._generate_attribute(
                    attribute_name, property_info, **kwargs)

            else:
                raise JSONSchemaException(
                    "Unhandled property {}".format(attribute_name))

        return obj

    def _generate_attribute(self, attribute_name: str, property_info: dict, **kwargs):
        """ 
        Generate the object attribute, based on the schema 
        """
        # If we do not have a key "type" this is not a valid property definition
        if "type" not in property_info:
            raise AttributeError("Not a valid property definition")

        # Get the Python type from the property definition
        py_type = JSONSchemaObject._get_python_type(
            property_info)

        # Get the default value for this property, we always send the schema definition
        py_default = JSONSchemaObject._get_default_value(
            py_type,
            schema_name=self._schema_name,
            schema_path=self._schema_path,
            attribute_name=attribute_name,
            **kwargs
        )

        # No initial value, return default value
        if not kwargs:
            return py_default

        # Try to get it from the initial_values dict
        if attribute_name not in kwargs:
            return py_default

        # Get the value from passed kwargs
        value = kwargs.get(attribute_name)

        # if we have a array of objects we must convert it to a JSON
        # array
        if py_type is JSONSchemaArray and isinstance(value, list):

            # compose the objects with the dict data
            array = JSONSchemaArray(
                schema_name=self._schema_name,
                schema_path=self._schema_path,
                attribute_name=attribute_name,
            )

            for v in value:
                array.append(**v)                                

            return array

        # check if the value is a valid type
        if isinstance(value, py_type):
            return value

        # oops someting went wrong
        raise AttributeError("{} attribute {} is not type {}".format(
            self._schema_path, attribute_name, py_type))

    def __setattr__(self, name: str, value: object):
        '''
        Set an object attribute 
        '''
        # We might have _attribs in our schema (ie. _id, _version ...)
        # this allows to access them without the _
        u_name = "_{}".format(name)

        if name in JSONSchemaObject.__internals__:
            # is it an internal attribute? we must use super class
            return super().__setattr__(name,value)
        elif name in self.__dict__["__attrs__"]:
            
            # Before setting the value do a basic validation
            if not JSONSchemaObject._validate_value(name,self._schema_path,value):
                raise ValueError("value is not valid")

            self.__dict__["__attrs__"][name] = value
        
        elif u_name in self.__dict__["__attrs__"]:

            
            # Before setting the value do a basic validation
            if not JSONSchemaObject._validate_value(u_name,self._schema_path,value):
                raise ValueError("value is not valid")

            self.__dict__["__attrs__"][u_name] = value

        elif hasattr(self, name):
            super().__setattr__(name, value)
            
        else:
            raise AttributeError("{} instance has no attribute {}\n".format(
                self.__class__.__name__, name))

    def __getattribute__(self, name):
        '''
        Get the object value
        '''
        # We might have _attribs in our schema (ie. _id, _version ...)
        # this allows to access them without the _
        u_name = "_{}".format(name)

        if name in JSONSchemaObject.__internals__:
            # is it an internal attribute? we must use super class
            # to return it
            return super().__getattribute__(name)

        elif str(name).startswith("remove_"):
            # this helper allows to use attributes such as remove_port which is
            # translated to the attr ports.remove
            attrib = "{}s".format(str(name).replace("remove_", ""))

            if attrib not in self.__dict__["__attrs__"]:
                return super().__getattribute__(name)

            if not isinstance(self.__dict__['__attrs__'][attrib], JSONSchemaArray):
                raise AttributeError("{} not an attribute".format(attrib))

            return self.__dict__['__attrs__'][attrib].__delitem__

        elif str(name).startswith("get_"):
            # this helper allows to use attributes such as get_port which is
            # translated to the attr ports.get
            attrib = "{}s".format(str(name).replace("get_", ""))

            if attrib not in self.__dict__["__attrs__"]:
                return super().__getattribute__(name)

            if not isinstance(self.__dict__['__attrs__'][attrib], JSONSchemaArray):
                raise AttributeError("{} not an attribute".format(attrib))
            return self.__dict__['__attrs__'][attrib].__getitem__

        elif str(name).startswith("append_"):
            # this helper allows to use attributes such as append_port which is
            # translated to the attr ports.append
            attrib = "{}s".format(str(name).replace("append_", ""))

            if attrib not in self.__dict__["__attrs__"]:
                return super().__getattribute__(name)

            if not isinstance(self.__dict__['__attrs__'][attrib], JSONSchemaArray):
                raise AttributeError("{} not an attribute".format(attrib))
            return self.__dict__['__attrs__'][attrib].append

        elif name in self.__dict__['__attrs__']:
            # are we asking for an existing attribute?
            return self.__dict__['__attrs__'][name]

        elif u_name in self.__dict__['__attrs__']:
            # are we asking for an existing attribute?
            return self.__dict__['__attrs__'][u_name]

        # let the supper handle with the rest of it
        return super().__getattribute__(name)

    def __str__(self):
        return self.to_json()

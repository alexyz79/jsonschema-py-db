import unittest
from schema import JSONSchemaObject, JSONSchemaArray
from database import DatabaseLayer
from redisdriver import RedisDriver

schema_user = """
{
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "user",
    "description" : "Application user",
    "required": [ "username", "password" ],
    "type": "object",
    "properties": {
        "login": {
            "type": "string",
            "description": "The users's login name."
        },
        "password": {
            "type": "string",
            "description": "The users's password."
        },
        "first": {
            "type": "string",
            "description": "The users's first name."
        },
        "last": {
            "type": "string",
            "description": "The users's last name."
        },
        "super_user": {
            "type": "boolean",
            "description": "If users is a super user."
        },
        "roles": {
            "type": "array",
            "description": "The user's roles.",
            "items" : {
                "$ref" : "role.json"
            }
        }
    }
}
"""

schema_role = """
{
    "$id": "https://example.com/geographical-location.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "role",
    "description": "Application role",
    "required": [ "name" ],
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "descrition": "The role name"
        },
        "permissions": {
            "type": "array",
            "items" : [
                {
                    "type": "string"
                },
                {
                    "type": "string"
                }
            ]
        }
    }
}
"""

schema_addresses = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",

    "definitions": {
        "address": {
            "type": "object",
            "properties": {
                "street_address": { "type": "string" },
                "city":           { "type": "string" },
                "state":          { "type": "string" }
            },
            "required": ["street_address", "city", "state"]
        }
    },

    "type": "object",

    "properties": {
        "billing_address": { "$ref": "#/definitions/address" },
        "shipping_address": { "$ref": "#/definitions/address" }
    }
}
"""

schema_person = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://test.local/person.json",
    "title" : "person"
    "description" : "just a person"
    "definitions": {
        "person": {
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "children": {
                    "type": "array",
                    "items": { "$ref": "#/definitions/person" },
                    "default": []
                }
            }
        }
    },

    "type": "object",

    "properties": {
        "person": { "$ref": "#/definitions/person" }
    }
}
"""

schema_callback = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://test.local/callback.json",
    "title": "Callback",
    "description": "",
    "type": "object",
    "properties": {
        "_id": {
            "description" : "Callback ID",
            "type": "string"
        },
        "_version": {
            "description" : "Callback version",
            "type": "string"
        },
        "_name": {
            "description" : "Callback name",
            "type": "string"
        },
        "tags": {
            "description" : "Callback tags",
            "type": "array",
            "items": { "$ref" : "#/definitions/tag" }
        },
        "parameters": {
            "description" : "Callback parameters",
            "type": "array",
            "items": { "$ref" : "#/definitions/parameter" }
        },
        "code": {
            "description": "Callback python code",
            "type": "string"
        },
        "libraries": {
            "description": "Callback python libraries",
            "type": "array",
            "items": {
                "type": "string"
            }
        }
    },
    "definitions" : {
        "parameter" : {
            "type": "object",
            "properties": {
                "name": { "type":"string" },
                "data": { "type":"object" }
            }
        },
        "tag" : {
            "type": "object",
            "properties": {
                "_id": {"type":"string"},
                "name": { "type":"string" },
                "value": { "type":"string" }
            }
        }
    }
}
"""

schema_node = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://test.local/node.json",
    "title": "Node",
    "description": "A node",
    "type": "object",
    "properties": {
        "_id": {
            "description" : "Node ID",
            "type": "string"
        },
        "_version": {
            "description" : "Node Version",
            "type": "string"
        },
        "name": {
            "description" : "Callback name",
            "type": "string",
            "unique" : "true"
        },        
        "tags": {
            "description" : "Node tags",
            "type": "array",
            "items": { "$ref" : "#/definitions/tag" }
        },
        "parameters": {
            "description" : "Node parameters",
            "type": "array",
            "items": { "$ref" : "#/definitions/parameter" }
        },
        "ports": {
            "description" : "Node ports",
            "type": "array",
            "items": {
                "$ref": "#/definitions/port"
            }
        }
    },
    "definitions": {
        "port": {
            "type": "object",
            "properties": {
                "name": {
                    "description": "Port name",
                    "type": "string"
                },
                "direction": {
                    "description": "Port direction",
                    "type": "string",
                    "enum": [
                        "in",
                        "out"
                    ]
                },
                "protocol": {
                    "description": "Port protocol",
                    "type": "string"
                },
                "tags": {
                    "description" : "Port tags",
                    "type": "array",
                    "items": { "$ref" : "#/definitions/tag" }
                },
                "parameters": {
                    "description" : "Port parameters",
                    "type": "array",
                    "items": { "$ref" : "#/definitions/parameter" }
                },
                "callback": {
                    "$ref": "callback"
                }
            }
        },
        "parameter" : {
            "type": "object",
            "properties": {
                "name": { "type":"string" },
                "data": { "type":"object" }
            }
        },
        "tag" : {
            "type": "object",
            "properties": {
                "name": { "type":"string" },
                "value": { "type":"string" }
            }
        }
    }
}
"""

schema_flow = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://test.local/flow.json",
    "title": "Flow",
    "description": "",
    "type": "object",
    "properties": {
        "_id": {
            "description" : "Flow ID",
            "type": "string"
        },
        "tags": {
            "description" : "Flow tags",
            "type": "array",
            "items": { "$ref" : "#/definitions/tag" }
        },
        "parameters": {
            "description" : "Flow parameters",
            "type": "array",
            "items": { "$ref" : "#/definitions/parameter" }
        },
        "nodes": {
            "description" : "Flow nodes",
            "type": "array",
            "items": {
                "$ref": "#/definitions/node"
            }
        },
        "layers": {
            "description": "Flow layers",
            "type": "array",
            "items": {
                "type": "string"
            }
        }
    },
    "definitions": {
        "node": {
            "type": "object",
            "properties": {
                "class": {
                    "description": "Template of the flow node",
                    "type": "string"
                },
                "name": {
                    "description": "Flow node instance name",
                    "type": "string"
                },
                "tags": {
                    "description": "Flow node instance tags",
                    "type": "array",
                    "items": { "$ref" : "#/definitions/tag" }
                },
                "parameters": {
                    "description" : "Flow node instance parameters",
                    "type": "array",
                    "items": { "$ref" : "#/definitions/parameter" }
                },
                "links": {
                    "description": "",
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/link"
                    }
                }
            }
        },
        "parameter" : {
            "type": "object",
            "properties": {
                "name": { "type":"string" },
                "data": { "type":"object" }
            }
        },
        "tag" : {
            "type": "object",
            "properties": {
                "name": { "type":"string" },
                "value": { "type":"string" }
            }
        },
        "link": {
            "type": "object",
            "properties": {
                "from": { 
                    "type":"string"
                },
                "to": { 
                    "type":"object",
                    "properties" : {
                        "node" : { "type" : "string" },
                        "port" : { "type" : "string" }
                    } 
                },
                "linktype" : { "type" : "string" }
            }
        }
    }
}
"""

# Define models as class extension

class Node(JSONSchemaObject):
    """
    A Node class
    """
    def __init__(self,**kwargs):
        # this must exists otherwise decoding will
        # fail
        super().__init__(**kwargs)

    def delete_parameter(self,name):
        i = 0
        for param in self.parameters:
            if param.name == name:
                self.remove_parameter(i)
                return
            i+=1                    


    def get_parameters_by_name(self, name):
        for param in self.parameters:
            if param.name != name:
                continue
            return param
        return None

    def move(self, x, y):
        visual_params = self.get_parameters_by_name("visual")
        
        if visual_params is None:
            self.append_parameter(
                name="visual",
                data={
                    "position": {
                        "x": x,
                        "y": y
                    }
                }
            )
            return

        if "position" not in visual_params.data:
            visual_params.data["position"] = {}

        visual_params.data["position"]["x"] = x
        visual_params.data["position"]["y"] = y

    def set_color(self, color):
        
        visual_params = self.get_parameters_by_name("visual")
        
        if visual_params is None:
            self.append_parameter(
                name="visual",
                data={
                    "color": color
                }
            )
            return

        visual_params.data["color"] = color

    def get_position(self):
        visual_params = self.get_parameters_by_name("visual")
        
        if visual_params is None:
            return (0.0,0.0)

        return (visual_params.data["position"]["x"], visual_params.data["position"]["y"])

    def get_color(self):
        visual_params = self.get_parameters_by_name("visual")
        
        if visual_params is None:
            return "#000000"
        
        if "color" not in visual_params.data:
            return "#000000"

        return visual_params.data["color"]


class JsonSchemaObjectTests(unittest.TestCase):

    def test_node_no_class(self):
        # Define models without class body
        NodeModel = JSONSchemaObject.new_model("Node")
        node = NodeModel()

        # Add 1 port
        node.append_port(
            name="port1", 
            direction="in",
            protocol="ros1", 
            parameters=[], 
            callback= {
                "_id" :"0000-00-0000000-0000000000",
                "tags" : [
                    { "name" : "label", "value" : "callback_1" }
                ],
                "parameters" : [
                    { 
                        "name" : "ros_parameters", 
                        "data" : {
                            "msgtype" : "movaimsg",
                            "scene" : "scene_a"
                        }
                    }
                ],
                "code" : "print(globals())",
                "libraries" : []
            }
        )

        # Checking num of ports
        self.assertEqual(len(node.ports), 1)

        # Checking port content
        self.assertEqual(node.ports[0].name, "port1")
        self.assertEqual(node.ports[0].direction, "in")
        self.assertEqual(node.ports[0].protocol, "ros1")
        self.assertEqual(len(node.ports[0].parameters), 0)
        self.assertEqual(node.ports[0].callback._id, "0000-00-0000000-0000000000")
        self.assertEqual(node.ports[0].callback.tags[0].name, "label")
        self.assertEqual(node.ports[0].callback.tags[0].value, "callback_1")
        self.assertEqual(node.ports[0].callback.parameters[0].name, "ros_parameters")
        self.assertEqual(node.ports[0].callback.parameters[0].data["msgtype"], "movaimsg")
        self.assertEqual(node.ports[0].callback.parameters[0].data["scene"], "scene_a")
        self.assertEqual(node.ports[0].callback.code, "print(globals())")
        self.assertEqual(len(node.ports[0].callback.libraries), 0)

        # change port name
        node.ports[0].name = "port3"
        self.assertEqual(node.ports[0].name, "port3")

        # Append a new node
        node.append_port(
            name="port2", direction="out",
            protocol="ros1", parameters=[
                {
                    "name": "rosdata",
                    "data": {
                        "message": "movaimsg"
                    }
                }
            ])

        # Checking num of ports
        self.assertEqual(len(node.ports), 2)

        # Check port data
        self.assertEqual(node.ports[1].name, "port2")
        self.assertEqual(node.ports[1].direction, "out")
        self.assertEqual(node.ports[1].protocol, "ros1")

        # Check port parameters
        self.assertEqual(node.ports[1].parameters[0].name, "rosdata")
        self.assertEqual(
            node.ports[1].parameters[0].data["message"], "movaimsg")

        # Check node parameters
        node.append_parameter(
            name="visual",
            data={
                "color": "red",
                "position": {
                    "x": 10.0,
                    "y": 5.0
                }
            }
        )

        # Check port parameters
        self.assertEqual(node.parameters[0].name, "visual")
        self.assertEqual(node.parameters[0].data["color"], "red")
        self.assertEqual(
            node.parameters[0].data["position"]["x"], 10.0)
        self.assertEqual(
            node.parameters[0].data["position"]["y"], 5.0)

    def test_node_class(self):

        # Define models by calling the class
        node = Node()

        # Add 1 port
        node.append_port(
            name="port1", 
            direction="in",
            protocol="ros1", 
            parameters=[], 
            callback= {
                "_id" :"0000-00-0000000-0000000000",
                "tags" : [
                    { "name" : "label", "value" : "my awsome callback" }
                ],
                "parameters" : [
                    { 
                        "name" : "ros_parameters", 
                        "data" : {
                            "msgtype" : "movaimsg",
                            "scene" : "scene_a"
                        }
                    }
                ],
                "code" : "print(globals())",
                "libraries" : []
            }
        )

        # Checking num of ports
        self.assertEqual(len(node.ports), 1)

        # Checking port content
        self.assertEqual(node.ports[0].name, "port1")
        self.assertEqual(node.ports[0].direction, "in")
        self.assertEqual(node.ports[0].protocol, "ros1")
        self.assertEqual(len(node.ports[0].parameters), 0)

        # Append a new node
        node.append_port(
            name="port2", direction="out",
            protocol="ros1", parameters=[
                {
                    "name": "rosdata",
                    "data": {
                        "message": "movaimsg"
                    }
                }
            ])

        # Checking num of ports
        self.assertEqual(len(node.ports), 2)

        # Check port data
        self.assertEqual(node.ports[1].name, "port2")
        self.assertEqual(node.ports[1].direction, "out")
        self.assertEqual(node.ports[1].protocol, "ros1")
        # self.assertEqual(node.ports[1].callback, "")

        # Check port parameters
        self.assertEqual(node.ports[1].parameters[0].name, "rosdata")
        self.assertEqual(
            node.ports[1].parameters[0].data["message"], "movaimsg")

        # Check node parameters
        node.append_parameter(
            name="visual",
            data={
                "color": "#FF00FF",
                "position": {
                    "x": 10.0,
                    "y": 5.0
                }
            }
        )

        # Check port parameters
        self.assertEqual(node.parameters[0].name, "visual")
        self.assertEqual(node.parameters[0].data["color"], "#FF00FF")
        self.assertEqual(
            node.parameters[0].data["position"]["x"], 10.0)
        self.assertEqual(
            node.parameters[0].data["position"]["y"], 5.0)

        # Check class methods
        node.move(20.0, 30.0)
        self.assertEqual(node.get_position(), (20.0, 30.0))
        self.assertEqual(node.get_color(), "#FF00FF")

        # Remove the parameter
        node.remove_parameter(0)
        self.assertEqual(node.get_position(), (0.0, 0.0))
        self.assertEqual(node.get_color(), "#000000")

        # Move again
        node.move(20.0, 30.0)
        self.assertEqual(node.get_position(), (20.0, 30.0))
        self.assertEqual(node.get_color(), "#000000")

        # Remove visual parameters again
        node.delete_parameter("visual")
        self.assertEqual(node.get_position(), (0.0, 0.0))
        self.assertEqual(node.get_color(), "#000000")

        # Another attempt
        node.move(120.0, 130.0)
        node.set_color("#F0F0F0")
        self.assertEqual(node.get_position(), (120.0, 130.0))
        self.assertEqual(node.get_color(), "#F0F0F0")

        # Serialize and desserialize
        node_str = str(node)
        node_1 = JSONSchemaObject.from_json("node",node_str)
        self.assertEqual(type(node), type(node_1))
        self.assertEqual(node.to_json(), node_1.to_json())

        # Execute same operations on the deserialized object

        # Checking num of ports
        self.assertEqual(len(node_1.ports), 2)

        # Check port data
        self.assertEqual(node_1.ports[1].name, "port2")
        self.assertEqual(node_1.ports[1].direction, "out")
        self.assertEqual(node_1.ports[1].protocol, "ros1")
        # self.assertEqual(node_1.ports[1].callback, "")

        # Check port parameters
        self.assertEqual(node_1.ports[1].parameters[0].name, "rosdata")
        self.assertEqual(
            node_1.ports[1].parameters[0].data["message"], "movaimsg")

        # Remove the parameter
        node_1.remove_parameter(0)
        self.assertEqual(node_1.get_position(), (0.0, 0.0))
        self.assertEqual(node_1.get_color(), "#000000")

        # Move again
        node_1.move(20.0, 30.0)
        self.assertEqual(node_1.get_position(), (20.0, 30.0))
        self.assertEqual(node_1.get_color(), "#000000")

        # Remove visual parameters again
        node_1.delete_parameter("visual")
        self.assertEqual(node_1.get_position(), (0.0, 0.0))
        self.assertEqual(node_1.get_color(), "#000000")

        # Another attempt
        node_1.move(120.0, 130.0)
        node_1.set_color("#F0F0F0")
        self.assertEqual(node_1.get_position(), (120.0, 130.0))
        self.assertEqual(node_1.get_color(), "#F0F0F0")
        self.assertEqual(node.to_json(), node_1.to_json())

    def test_database_layer_nulldriver(self):

        # Define models by calling the class
        node = Node()

        node.name = "My awsome node"

        # Add 1 port
        node.append_port(
            name="port1", 
            direction="in",
            protocol="ros1", 
            parameters=[], 
            callback= {
                "_id" :"",
                "_name": "my awsome callback",
                "tags" : [
                    { "name" : "label", "value" : "my awsome callback" }
                ],
                "parameters" : [
                    { 
                        "name" : "ros_parameters", 
                        "data" : {
                            "msgtype" : "movaimsg",
                            "scene" : "scene_a"
                        }
                    }
                ],
                "code" : "print(globals())",
                "libraries" : []
            }
        )

        # Checking num of ports
        self.assertEqual(len(node.ports), 1)

        # Checking port content
        self.assertEqual(node.ports[0].name, "port1")
        self.assertEqual(node.ports[0].direction, "in")
        self.assertEqual(node.ports[0].protocol, "ros1")
        self.assertEqual(len(node.ports[0].parameters), 0)

        db = DatabaseLayer(drv=RedisDriver())
        ids = db.store(node)

        node.version = "1.1"
        ids = db.store(node)

        # node_1 = db.find_all_by("node","name","My awsome node","1.1")

        # self.assertEqual(node.to_json(), node_1[0].to_json())



if __name__ == '__main__':
    JSONSchemaObject.set_schema("user", schema_user)
    JSONSchemaObject.set_schema("role", schema_role)
    JSONSchemaObject.set_schema("callback", schema_callback)
    JSONSchemaObject.set_schema("node", schema_node)
    JSONSchemaObject.set_schema("flow", schema_flow)
    unittest.main()

import yaml

from tilekiln.definition import Definition


class Config:
    def __init__(self, yaml_string, filesystem):
        '''Create a config from a yaml string
           Creates a config from the yaml string. Any SQL files referenced
           must be in the filesystem.
        '''
        config = yaml.safe_load(yaml_string)
        self.name = config.get("metadata").get("name")
        self.description = config.get("metadata").get("description")
        self.attribution = config.get("metadata").get("attribution")
        self.version = config.get("metadata").get("version")
        self.bounds = config.get("metadata").get("bounds")
        self.center = config.get("metadata").get("center")

        self.layers = []
        for id, l in config.get("vector_layers", {}).items():
            self.layers.append(LayerConfig(id, l, filesystem))


class LayerConfig:
    def __init__(self, id, layer_yaml, filesystem):
        '''Create a layer config
           Creates a layer config from the config yaml for a layer. Any SQL
           files referenced must be in the filesystem.
        '''
        self.id = id
        self.description = layer_yaml.get("description")
        self.fields = layer_yaml.get("fields")
        self.definitions = []
        self.geometry_type = set(layer_yaml.get("geometry_type", []))

        definitions = set()
        for definition in layer_yaml.get("sql", []):
            definitions.add(Definition(definition, filesystem))

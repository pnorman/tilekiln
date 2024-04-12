import json
import yaml

from tilekiln.definition import Definition
from tilekiln.tile import Tile


class Config:
    def __init__(self, yaml_string, filesystem):
        '''Create a config from a yaml string
           Creates a config from the yaml string. Any SQL files referenced must be in the
           filesystem.
        '''
        config = yaml.safe_load(yaml_string)
        self.id = config["metadata"]["id"]
        self.name = config["metadata"].get("name")
        self.description = config["metadata"].get("description")
        self.attribution = config["metadata"].get("attribution")
        self.version = config["metadata"].get("version")
        self.bounds = config["metadata"].get("bounds")
        self.center = config["metadata"].get("center")

        # TODO: Make private and expose needed operations through proper functions
        self.layers = []
        for id, l in config.get("vector_layers", {}).items():
            self.layers.append(LayerConfig(id, l, filesystem))

        self.minzoom = None
        self.maxzoom = None
        if self.layers:
            self.minzoom = min([layer.minzoom for layer in self.layers])
            self.maxzoom = max([layer.maxzoom for layer in self.layers])

    def tilejson(self, url) -> str:
        '''Returns a TileJSON'''

        result = {"tilejson": "3.0.0",
                  "tiles": [f"{url}/{self.id}" + "/{z}/{x}/{y}.mvt"],
                  "attribution": self.attribution,
                  "bounds": self.bounds,
                  "center": self.center,
                  "description": self.description,
                  "maxzoom": self.maxzoom,
                  "minzoom": self.minzoom,
                  "name": self.name,
                  "scheme": "xyz"}

        vector_layers = [{"id": layer.id,
                          "fields": layer.fields,
                          "description": layer.description,
                          "minzoom": layer.minzoom,
                          "maxzoom": layer.maxzoom} for layer in self.layers]
        result["vector_layers"] = [{k: v for k, v in layer.items() if v is not None}
                                   for layer in vector_layers]

        return json.dumps({k: v for k, v in result.items() if v is not None},
                          sort_keys=True, indent=4)

    def layer_queries(self, tile: Tile):
        return {layer.render_sql(tile) for layer in self.layers
                if layer.render_sql(tile) is not None}


class LayerConfig:
    def __init__(self, id, layer_yaml, filesystem):
        '''Create a layer config
           Creates a layer config from the config yaml for a layer. Any SQL files referenced must
           be in the filesystem.
        '''
        self.id = id
        self.description = layer_yaml.get("description")
        self.fields = layer_yaml.get("fields", {})
        self.definitions = []
        self.geometry_type = set(layer_yaml.get("geometry_type", []))

        self.__definitions = set()
        for definition in layer_yaml.get("sql", []):
            self.__definitions.add(Definition(id, definition, filesystem))

        self.minzoom = min({d.minzoom for d in self.__definitions})
        self.maxzoom = max({d.maxzoom for d in self.__definitions})

    def render_sql(self, tile) -> str | None:
        '''Returns the SQL for a layer, given a tile, or None if it is outside the zoom range
           of the definitions
        '''
        if tile.zoom > self.maxzoom or tile.zoom < self.minzoom:
            return None

        for d in self.__definitions:
            if tile.zoom <= d.maxzoom and tile.zoom >= d.minzoom:
                return d.render_sql(tile)

        return None

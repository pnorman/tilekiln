import json
import yaml

import fs

from tilekiln.definition import Definition
from tilekiln.errors import ConfigYAMLError, ConfigError
from tilekiln.tile import Tile


class Config:
    def __init__(self, yaml_string: str, filesystem: fs.base.FS):
        '''Create a config from a yaml string
           Creates a config from the yaml string. Any SQL files referenced must be in the
           filesystem.
        '''

        try:
            config = yaml.safe_load(yaml_string)
        except yaml.parser.ParserError:
            raise ConfigYAMLError("Unable to parse config YAML")

        try:
            metadata = config["metadata"]
        except Exception:
            raise ConfigYAMLError("No metadata found in config") from None

        try:
            self.id = metadata["id"]
        except Exception:
            raise ConfigYAMLError("id not found in config metadata") from None
        if not isinstance(self.id, str) or self.id is None:
            raise ConfigYAMLError("metadata.id is not a string") from None

        self.name = metadata.get("name")
        self.description = metadata.get("description")
        self.attribution = metadata.get("attribution")
        self.version = metadata.get("version")
        self.bounds = metadata.get("bounds")
        self.center = metadata.get("center")
        # TODO: Make private and expose needed operations through proper functions
        self.layers = []
        try:
            for id, l in config.get("vector_layers", {}).items():
                self.layers.append(LayerConfig(id, l, filesystem))
        except Exception:
            raise ConfigError("Unable to process vector_layers")

        if self.layers:
            self.minzoom = min([layer.minzoom for layer in self.layers])
            self.maxzoom = max([layer.maxzoom for layer in self.layers])
        else:
            self.minzoom = None
            self.maxzoom = None

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
    def __init__(self, id: str, layer_yaml: dict, filesystem: fs.base.FS):
        '''Create a layer config
           Creates a layer config from the config yaml for a layer. Any SQL files referenced must
           be in the filesystem.
        '''
        self.id = id
        self.description = layer_yaml.get("description")
        self.fields = layer_yaml.get("fields", {})
        self.definitions: list[Definition] = []
        self.geometry_type = set(layer_yaml.get("geometry_type", []))

        self.__definitions = set()
        for definition in layer_yaml.get("sql", []):
            self.__definitions.add(Definition(id, definition, filesystem))

        self.minzoom = min({d.minzoom for d in self.__definitions})
        self.maxzoom = max({d.maxzoom for d in self.__definitions})

    def render_sql(self, tile: Tile) -> str | None:
        '''Returns the SQL for a layer, given a tile, or None if it is outside the zoom range
           of the definitions
        '''
        if tile.zoom > self.maxzoom or tile.zoom < self.minzoom:
            return None

        for d in self.__definitions:
            if tile.zoom <= d.maxzoom and tile.zoom >= d.minzoom:
                return d.render_sql(tile)

        return None

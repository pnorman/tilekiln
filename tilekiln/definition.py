import jinja2 as j2

import fs

from tilekiln.tile import Tile
from tilekiln.errors import DefinitionError

DEFAULT_EXTENT = 4096
DEFAULT_BUFFER = 0

# Invariants of web mercator
HALF_WORLD = 20037508.34

j2Environment = j2.Environment(loader=j2.BaseLoader(), lstrip_blocks=True, trim_blocks=True)


class Definition:
    def __init__(self, id: str, definition_yaml, filesystem: fs.base.FS):
        self.id = id

        try:
            self.minzoom = definition_yaml["minzoom"]
        except KeyError:
            raise DefinitionError(f"Layer {id} is missing minzoom on a definition") from None
        try:
            self.maxzoom = definition_yaml["maxzoom"]
        except KeyError:
            raise DefinitionError(f"Layer {id} is missing maxzoom on a definition") from None

        self.extent = definition_yaml.get("extent", DEFAULT_EXTENT)
        self.buffer = definition_yaml.get("buffer", DEFAULT_BUFFER)

        # TODO: Let is use directories so one file can include others.
        filename = definition_yaml["file"]
        try:
            self.__template = j2Environment.from_string(filesystem.readtext(filename))
        except fs.errors.ResourceNotFound:
            raise DefinitionError(f"Layer {id} is missing is missing file {filename}") from None

    def render_sql(self, tile: Tile) -> str:
        '''Generate the SQL for a layer
        '''

        # Tile validity constraints. x/y are checked by Tile class
        assert tile.zoom >= self.minzoom
        assert tile.zoom <= self.maxzoom

        # See https://postgis.net/docs/ST_AsMVT.html for SQL source

        inner = self.__template.render(zoom=tile.zoom, x=tile.x, y=tile.y,
                                       bbox=tile.bbox(self.buffer/self.extent),
                                       unbuffered_bbox=tile.bbox(0),
                                       extent=self.extent,
                                       buffer=self.buffer,
                                       tile_length=tile_length(tile),
                                       tile_area=tile_length(tile)**2,
                                       coordinate_length=tile_length(tile)/self.extent,
                                       coordinate_area=(tile_length(tile)/self.extent)**2)

        # TODO: Use proper escaping for self.id in SQL
        return ('''WITH mvtgeom AS\n(\n''' + inner + '''\n)\n''' +
                f'''SELECT ST_AsMVT(mvtgeom.*, '{self.id}', {self.extent})\n''' +
                '''FROM mvtgeom;''')


def tile_length(tile) -> float:
    '''Returns the length of a tile, in projected units
    '''
    # -1 for half vs full world
    return HALF_WORLD/(2**(tile.zoom-1))

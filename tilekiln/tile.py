import pmtiles.tile  # type: ignore


class Tile:
    __slots__ = ("tileid")

    def __init__(self, zoom: int, x: int, y: int):
        '''Creates a tile object, with x, y, and zoom
        '''
        self.tileid = pmtiles.tile.zxy_to_tileid(zoom, x, y)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.tileid == other.tileid

    def __hash__(self):
        return self.tileid

    @property
    def zxy(self):
        return pmtiles.tile.tileid_to_zxy(self.tileid)

    @property
    def zoom(self):
        return self.zxy[0]

    @property
    def x(self):
        return self.zxy[1]

    @property
    def y(self):
        return self.zxy[2]

    def __repr__(self) -> str:
        return f"Tile({self.zoom},{self.x},{self.y})"

    @classmethod
    def from_string(cls, tile: str):
        try:
            fragments = tile.split("/")
            if len(fragments) != 3:
                raise ValueError(f"Unable to parse tile from: {tile}")
            return cls(int(fragments[0]), int(fragments[1]), int(fragments[2]))
        except (ValueError, IndexError):
            raise ValueError(f"Unable to parse tile from: {tile}")

    @classmethod
    def from_tileid(cls, tileid: int):
        # TODO: This converts id to xyz to id, there should be a way with less calls
        (zoom, x, y) = pmtiles.tile.tileid_to_zxy(tileid)
        return cls(zoom, x, y)

    def bbox(self, buffer) -> str:
        '''Returns the bounding box for a tile
        '''
        return f'''ST_TileEnvelope({self.zoom}, {self.x}, {self.y}, margin=>{buffer})'''


def layer_frominput(input: str) -> dict[Tile, set[str]]:
    '''Generates a list of tile layers from string
    '''

    layers: dict[Tile, set[str]] = {}
    for line in input.split("\n"):
        if line.strip() == "":
            continue
        try:
            tiletext, layer = line.split(",")
        except ValueError:
            raise ValueError(f"Unable to parse layer from: {line}")
        tile = Tile.from_string(tiletext)
        if tile in layers:
            layers[tile].add(layer)
        else:
            layers[tile] = {layer}

    return layers

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
        fragments = tile.split("/")
        return cls(int(fragments[0]), int(fragments[1]), int(fragments[2]))

    @classmethod
    def from_tileid(cls, tileid: int):
        # TODO: This converts id to xyz to id, there should be a way with less calls
        (zoom, x, y) = pmtiles.tile.tileid_to_zxy(tileid)
        return cls(zoom, x, y)

    def bbox(self, buffer) -> str:
        '''Returns the bounding box for a tile
        '''
        return f'''ST_TileEnvelope({self.zoom}, {self.x}, {self.y}, margin=>{buffer})'''

class Tile:
    def __init__(self, zoom: int, x: int, y: int):
        '''Creates a tile object, with x, y, and zoom
        '''
        assert zoom >= 0
        assert x >= 0
        assert x < 2**zoom
        assert y >= 0
        assert y < 2**zoom

        self.zoom = zoom
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Tile({self.zoom},{self.x},{self.y})"

    @classmethod
    def from_string(cls, tile):
        fragments = tile.split("/")
        return cls(int(fragments[0]), int(fragments[1]), int(fragments[2]))

    def bbox(self, buffer):
        '''Returns the bounding box for a tile
        '''
        return f'''ST_TileEnvelope({self.zoom}, {self.x}, {self.y}, margin=>{buffer})'''

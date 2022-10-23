class Tile:
    def __init__(self, zoom, x, y):
        '''Creates a tile object, with x, y, and zoom
        '''
        # TODO: Validate the tile?
        self.zoom = zoom
        self.x = x
        self.y = y

    def bbox(self, buffer):
        '''Returns the bounding box for a tile
        '''
        return f'''ST_TileEnvelope({self.zoom}, {self.x}, {self.y}, margin=>{buffer})'''

from tilekiln.tile import Tile


class Tilerange():
    def __init__(self, minz, maxz):
        self.minid = Tile(minz, 0, 0).tileid
        self.maxid = Tile(maxz + 1, 0, 0).tileid

    def __iter__(self):
        for id in range(self.minid, self.maxid):
            yield Tile.from_tileid(id)

    def __len__(self):
        return self.maxid - self.minid

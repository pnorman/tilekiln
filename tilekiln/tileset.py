from __future__ import annotations
from dataclasses import dataclass
import datetime

from tilekiln.config import Config
from tilekiln.tile import Tile

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from tilekiln.storage import Storage


@dataclass
class Tileset:
    '''A set of tiles in storage

    A tileset must always have the associated DB entries with tilejson/etc
    TODO: How to handle populating the DB
    '''

    storage: Storage
    id: str
    minzoom: int
    maxzoom: int
    tilejson: str

    @classmethod
    def from_config(cls, storage: Storage, config: Config):
        '''Create a tileset from a Storage and Config'''
        return cls(storage, config.id, config.minzoom, config.maxzoom,
                   config.tilejson('REPLACED_BY_SERVER'))

    @classmethod
    def from_id(cls, storage: Storage, id: str) -> Tileset:
        '''
        Create a tileset from a Storage and id

        This pulls the metadata from the storage
        '''
        minzoom = storage.get_minzoom(id)
        maxzoom = storage.get_minzoom(id)
        tilejson = storage.get_tilejson(id, 'REPLACED_BY_SERVER')
        return cls(storage, id, minzoom, maxzoom, tilejson)

    def prepare_storage(self) -> None:
        self.storage.create_tileset(self.id, self.minzoom, self.maxzoom,
                                    self.tilejson)

    def update_storage_metadata(self) -> None:
        '''Sets the metadata in storage'''
        self.storage.set_metadata(self.id, self.minzoom, self.maxzoom,
                                  self.tilejson)

    def get_tile(self, tile: Tile) -> tuple[bytes | None, datetime.datetime | None]:
        return self.storage.get_tile(self.id, tile)

    def save_tile(self, tile: Tile, data: bytes) -> datetime.datetime | None:
        return self.storage.save_tile(self.id, tile, data)

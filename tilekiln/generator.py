'''
The code here pulls creates multiple kilns to generate the tiles in parallel
'''
import multiprocessing as mp
from typing import Iterable

import psycopg

from tilekiln.config import Config
from tilekiln.kiln import Kiln
from tilekiln.storage import Storage
from tilekiln.tile import Tile
from tilekiln.tileset import Tileset


kiln: Kiln
tileset: Tileset


def setup(config: Config, source_kwargs, storage_kwargs) -> None:  # type: ignore[no-untyped-def]
    '''
    Sets up the kiln and tileset for the worker function.
    '''
    global kiln, tileset
    source_conn = psycopg.connect(**source_kwargs)
    kiln = Kiln(config, source_conn)

    storage_conn = psycopg.connect(**storage_kwargs)
    storage = Storage(storage_conn)
    tileset = Tileset.from_config(storage, config)


def worker(tile: Tile) -> None:
    global kiln, tileset
    mvt = kiln.render(tile)
    tileset.save_tile(tile, mvt)


def generate(config: Config, source_kwargs, storage_kwargs,  # type: ignore[no-untyped-def]
             tiles: Iterable[Tile], num_processes: int) -> None:
    with mp.Pool(num_processes, setup, (config, source_kwargs, storage_kwargs)) as pool:
        pool.imap_unordered(worker, tiles)
        pool.close()
        pool.join()

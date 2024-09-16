'''
The code here pulls creates multiple kilns to generate the tiles in parallel
'''
import multiprocessing as mp
from collections.abc import Collection

import psycopg_pool

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
    source_pool = psycopg_pool.ConnectionPool(kwargs=source_kwargs)
    kiln = Kiln(config, source_pool)

    storage_pool = psycopg_pool.ConnectionPool(kwargs=storage_kwargs)
    storage = Storage(storage_pool)
    tileset = Tileset.from_config(storage, config)


def worker(tile: Tile) -> None:
    global kiln, tileset
    mvt = kiln.render(tile)
    tileset.save_tile(tile, mvt)


def generate(config: Config, source_kwargs, storage_kwargs,  # type: ignore[no-untyped-def]
             tiles: Collection[Tile], num_processes: int) -> None:

    # If there are no processes and no tiles then there's nothing to do.
    if num_processes == 0 and len(tiles) == 0:
        return

    with mp.Pool(num_processes, setup, (config, source_kwargs, storage_kwargs)) as pool:
        pool.imap_unordered(worker, tiles)
        pool.close()
        pool.join()

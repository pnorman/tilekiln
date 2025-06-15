'''
The code here pulls creates multiple kilns to generate the tiles in parallel
'''
import multiprocessing as mp
from collections.abc import Collection
from typing import Optional

import psycopg_pool

from tilekiln.config import Config
from tilekiln.kiln import Kiln
from tilekiln.storage import Storage
from tilekiln.tile import Tile
from tilekiln.tileset import Tileset


kiln: Kiln
tileset: Tileset
tile_layers: Optional[set[str]]


def setup(config: Config, layers: Optional[set[str]],
          source_kwargs, storage_kwargs) -> None:  # type: ignore[no-untyped-def]
    '''
    Sets up the kiln and tileset for the worker function.
    '''
    global kiln, tileset, tile_layers
    source_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                              check=psycopg_pool.ConnectionPool.check_connection,
                                              kwargs=source_kwargs)
    kiln = Kiln(config, source_pool)

    storage_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                               check=psycopg_pool.ConnectionPool.check_connection,
                                               kwargs=storage_kwargs)
    storage = Storage(storage_pool)
    tileset = Tileset.from_config(storage, config)
    tile_layers = layers


def worker(tile: Tile) -> None:
    global kiln, tileset, tile_layers
    # Reuse the tilelayer worker if we're only rendering specific
    if tile_layers is not None:
        tilelayer_worker((tile, tile_layers))
    else:
        try:
            mvt = kiln.render_all(tile)
            tileset.save_tile(tile, mvt)
        except Exception as e:
            print(f"Error generating {tile}")
            raise RuntimeError(f"Error generating {tile}") from e


def tilelayer_worker(work: tuple[Tile, set[str]]) -> None:
    global kiln, tileset
    tile, layers = work
    try:
        new_mvts = {layer: kiln.render_layer(layer, tile) for layer in layers}
        tileset.save_tile(tile, new_mvts)
    except Exception as e:
        print(f"Error generating {tile}")
        raise RuntimeError(f"Error generating {tile}") from e


def generate(config: Config, source_kwargs, storage_kwargs,  # type: ignore[no-untyped-def]
             tiles: Collection[Tile], num_processes: int,
             layers: Optional[set[str]] = None) -> None:

    # If there are no processes and no tiles then there's nothing to do.
    if num_processes == 0 and len(tiles) == 0:
        return

    with mp.Pool(num_processes, setup, (config, layers, source_kwargs, storage_kwargs)) as pool:
        imap_it = pool.imap_unordered(worker, tiles, 100)
        pool.close()
        pool.join()

        # Check for exceptions
        for x in imap_it:
            pass


def generate_tilelayers(config: Config, source_kwargs,
                        storage_kwargs,  # type: ignore[no-untyped-def]
                        tilelayers: Collection[tuple[Tile, set[str]]], num_processes: int) -> None:

    # If there are no processes and no tiles then there's nothing to do.
    if num_processes == 0 and len(tilelayers) == 0:
        return

    with mp.Pool(num_processes, setup, (config, None, source_kwargs, storage_kwargs)) as pool:
        imap_it = pool.imap_unordered(tilelayer_worker, tilelayers, 100)
        pool.close()
        pool.join()

        # Check for exceptions
        for x in imap_it:
            pass

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
    source_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                              check=psycopg_pool.ConnectionPool.check_connection,
                                              kwargs=source_kwargs)
    kiln = Kiln(config, source_pool)

    storage_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                               check=psycopg_pool.ConnectionPool.check_connection,
                                               kwargs=storage_kwargs)
    storage = Storage(storage_pool)
    tileset = Tileset.from_config(storage, config)


def worker(tile: Tile, layer_filters: set[str] | None = None) -> None:
    global kiln, tileset
    try:
        mvt = kiln.render_all(tile, layer_filters)
        # Because everything was rendered we don't need to check for missing layers
        tileset.save_tile(tile, mvt)
    except Exception as e:
        print(f"Error generating {tile}")
        raise RuntimeError(f"Error generating {tile}") from e


def layer_worker(work: tuple[Tile, set[str]]) -> None:
    global kiln, tileset
    tile, layers = work
    try:
        new_mvts = {layer: kiln.render_layer(layer, tile) for layer in layers}
        # Because everything was rendered we don't need to check for missing layers
        tileset.save_tile(tile, new_mvts)
    except Exception as e:
        print(f"Error generating {tile}")
        raise RuntimeError(f"Error generating {tile}") from e


def generate(  # type: ignore[no-untyped-def]
        config: Config, source_kwargs, storage_kwargs,
        tiles: Collection[Tile], num_processes: int,
        layer_filters: set[str] | None = None) -> None:

    # If there are no processes and no tiles then there's nothing to do.
    if num_processes == 0 and len(tiles) == 0:
        return

    with mp.Pool(num_processes, setup, (config, source_kwargs, storage_kwargs)) as pool:
        # Use starmap to handle multiple arguments
        work_items = [(tile, layer_filters) for tile in tiles]
        imap_it = pool.starmap(worker, work_items, 100)
        pool.close()
        pool.join()

        # Check for exceptions
        for x in imap_it:
            pass


def generate_layers(config: Config, source_kwargs, storage_kwargs,  # type: ignore[no-untyped-def]
                    layers: Collection[tuple[Tile, set[str]]], num_processes: int) -> None:

    # If there are no processes and no tiles then there's nothing to do.
    if num_processes == 0 and len(layers) == 0:
        return

    with mp.Pool(num_processes, setup, (config, source_kwargs, storage_kwargs)) as pool:
        imap_it = pool.imap_unordered(layer_worker, layers, 100)
        pool.close()
        pool.join()

        # Check for exceptions
        for x in imap_it:
            pass

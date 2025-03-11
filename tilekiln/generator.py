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


def setup(config: Config, source_kwargs, storage_kwargs,
          layers: list[str] | None = None) -> None:  # type: ignore[no-untyped-def]
    '''
    Sets up the kiln and tileset for the worker function.

    Args:
        config: The configuration to use
        source_kwargs: Connection parameters for the source database
        storage_kwargs: Connection parameters for the storage database
        layers: Optional list of layer names to render. If None, all layers are rendered.
    '''
    global kiln, tileset
    source_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                              check=psycopg_pool.ConnectionPool.check_connection,
                                              kwargs=source_kwargs)
    kiln = Kiln(config, source_pool, layers)

    storage_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                               check=psycopg_pool.ConnectionPool.check_connection,
                                               kwargs=storage_kwargs)
    storage = Storage(storage_pool)
    tileset = Tileset.from_config(storage, config)


def worker(tile: Tile) -> None:
    global kiln, tileset
    try:
        mvt = kiln.render_all(tile)
        tileset.save_tile(tile, mvt)
    except Exception as e:
        print(f"Error generating {tile}")
        raise RuntimeError(f"Error generating {tile}") from e


def generate(config: Config, source_kwargs, storage_kwargs,  # type: ignore[no-untyped-def]
             tiles: Collection[Tile], num_processes: int,
             layers: list[str] | None = None) -> None:
    """Generate tiles with optional layer filtering.

    Args:
        config: The configuration to use
        source_kwargs: Connection parameters for the source database
        storage_kwargs: Connection parameters for the storage database
        tiles: Collection of tiles to generate
        num_processes: Number of worker processes to use
        layers: Optional list of layer names to render. If None, all layers are rendered.
    """
    # If there are no processes and no tiles then there's nothing to do.
    if num_processes == 0 and len(tiles) == 0:
        return

    # If layers specified, validate they exist
    if layers:
        all_layers = set(config.layer_names())
        unknown_layers = set(layers) - all_layers
        if unknown_layers:
            raise ValueError(f"Unknown layers: {', '.join(unknown_layers)}")

        # Print info about which layers will be rendered
        layer_str = ", ".join(layers)
        print(f"Rendering layers: {layer_str}")

    # Use a multiprocessing setup with additional layer parameter
    with mp.Pool(num_processes, setup, (config, source_kwargs, storage_kwargs, layers)) as pool:
        imap_it = pool.imap_unordered(worker, tiles, 100)
        pool.close()
        pool.join()

        # Check for exceptions
        for x in imap_it:
            pass

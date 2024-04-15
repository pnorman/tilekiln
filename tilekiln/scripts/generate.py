import os
import sys

import click
import psycopg
import psycopg_pool

import tilekiln

from tilekiln.tile import Tile
from tilekiln.tileset import Tileset
from tilekiln.storage import Storage
from tilekiln.kiln import Kiln


@click.group()
def generate() -> None:
    '''Commands for tile generation.

    All tile generation commands run queries against the source database which has the
    geospatial data
    '''
    pass


@generate.command()
@click.option('--config', required=True, type=click.Path(exists=True, dir_okay=False))
@click.option('-n', '--num-threads', default=len(os.sched_getaffinity(0)),
              show_default=True, help='Number of worker processes.')
@click.option('--source-dbname')
@click.option('--source-host')
@click.option('--source-port')
@click.option('--source-username')
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
def tiles(config: int, num_threads: int,
          source_dbname: str, source_host: str, source_port: int, source_username: str,
          storage_dbname: str, storage_host: str, storage_port: int, storage_username: str) -> None:
    '''Generate specific tiles.

    A list of z/x/y tiles is read from stdin and those tiles are generated and saved
    to storage.
    '''

    c = tilekiln.load_config(config)

    tiles = {Tile.from_string(t) for t in sys.stdin}
    threads = min(num_threads, len(tiles))  # No point in more threads than tiles

    click.echo(f"Rendering {len(tiles)} tiles over {threads} threads")

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(pool)

    tileset = Tileset.from_config(storage, c)
    gen_conn = psycopg.connect(dbname=source_dbname, host=source_host,
                               port=source_port, username=source_username)
    kiln = Kiln(c, gen_conn)
    for tile in tiles:
        mvt = kiln.render(tile)
        tileset.save_tile(tile, mvt)

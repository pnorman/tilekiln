import os
import sys

import click
from tqdm import tqdm

import tilekiln

from tilekiln.tile import Tile
import tilekiln.generator


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

    source_kwargs = {"dbname": storage_dbname,
                     "host": storage_host,
                     "port": storage_port,
                     "user": storage_username}
    storage_kwargs = {"dbname": source_dbname,
                      "host": source_host,
                      "port": source_port,
                      "user": source_username}
    tilekiln.generator.generate(c, source_kwargs, storage_kwargs, tqdm(tiles), threads)

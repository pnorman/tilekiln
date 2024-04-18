import sys

import click
import psycopg

import tilekiln

from tilekiln.tile import Tile
from tilekiln.tileset import Tileset
from tilekiln.storage import Storage


@click.group()
def storage() -> None:
    '''Commands working with tile storage.

    These commands allow creation and manipulation of the tile storage database.
    '''
    pass


@storage.command()
@click.option('--config', required=True, type=click.Path(exists=True, dir_okay=False))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port', type=click.INT)
@click.option('--storage-username')
@click.option('--id', help='Override YAML config ID')
def init(config: str,
         storage_dbname: str, storage_host: str, storage_port: int, storage_username: str,
         id: str) -> None:
    '''Initialize storage for tiles.

    Creates the storage for a tile layer and stores its metadata in the database.
    If the metadata tables have not yet been created they will also be setup.
    '''

    c = tilekiln.load_config(config)

    with psycopg.connect(dbname=storage_dbname, host=storage_host,
                         port=storage_port, user=storage_username) as conn:
        storage = Storage(conn)
        storage.create_schema()
        tileset = Tileset.from_config(storage, c)
        tileset.prepare_storage()


@storage.command()
@click.option('--config', type=click.Path(exists=True, dir_okay=False))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port', type=click.INT)
@click.option('--storage-username')
@click.option('--id', help='Override YAML config ID')
def destroy(config: str,
            storage_dbname: str, storage_host: str, storage_port: int, storage_username: str,
            id: str) -> None:
    ''' Destroy storage for tiles'''
    if config is None and id is None:
        raise click.UsageError('''Missing one of '--id' or '--config' options''')

    # No id specified, so load the config for one. We know from above config is not none.
    c = None
    if id is None:
        c = tilekiln.load_config(config)
        id = c.id

    with psycopg.connect(dbname=storage_dbname, host=storage_host,
                         port=storage_port, user=storage_username) as conn:
        storage = Storage(conn)
        storage.remove_tileset(id)


@storage.command()
@click.option('--config', type=click.Path(exists=True, dir_okay=False))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('-z', '--zoom', type=click.INT, multiple=True)
@click.option('--id', help='Override YAML config ID')
def delete(config: str,
           storage_dbname: str, storage_host: str, storage_port: int, storage_username: str,
           zoom: tuple[int], id: str) -> None:
    '''Mass-delete tiles from a tileset

    Deletes tiles from a tileset, by zoom, or delete all zooms.
    '''
    if config is None and id is None:
        raise click.UsageError('''Missing one of '--id' or '--config' options''')

    # No id specified, so load the config for one. We know from above config is not none.
    c = None
    if id is None:
        c = tilekiln.load_config(config)
        id = c.id

    with psycopg.connect(dbname=storage_dbname, host=storage_host,
                         port=storage_port, user=storage_username) as conn:
        storage = Storage(conn)

        if (len(zoom) == 0):
            storage.truncate_tables(id)
        else:
            storage.truncate_tables(id, zoom)


@storage.command()
@click.option('--config', type=click.Path(exists=True, dir_okay=False))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port', type=click.INT)
@click.option('--storage-username')
@click.option('--id', help='Override YAML config ID')
def tiledelete(config: str,
               storage_dbname: str, storage_host: str, storage_port: int, storage_username: str,
               id: str) -> None:
    '''Delete specific tiles.

    A list of z/x/y tiles is read from stdin and those tiles are deleted from
    storage. The entire list is read before deletion starts.
    '''
    if config is None and id is None:
        raise click.UsageError('''Missing one of '--id' or '--config' options''')

    # No id specified, so load the config for one. We know from above config is not none.
    c = None
    if id is None:
        c = tilekiln.load_config(config)
        id = c.id

    with psycopg.connect(dbname=storage_dbname, host=storage_host,
                         port=storage_port, user=storage_username) as conn:
        storage = Storage(conn)

        # TODO: This requires reading all of stdin before starting. This lets it display
        # how many tiles to delete but also means it has to read them all in before starting
        tiles = {Tile.from_string(t) for t in sys.stdin}
        click.echo(f"Deleting {len(tiles)} tiles")
        storage.delete_tiles(id, tiles)

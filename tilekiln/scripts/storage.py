import datetime
import sys

import click
import psycopg_pool

import tilekiln

from tilekiln.tile import Tile, layer_frominput
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

    with psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                     check=psycopg_pool.ConnectionPool.check_connection,
                                     kwargs={"dbname": storage_dbname, "host": storage_host,
                                             "port": storage_port, "user": storage_username
                                             }) as pool:
        storage = Storage(pool)
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

    with psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                     check=psycopg_pool.ConnectionPool.check_connection,
                                     kwargs={"dbname": storage_dbname, "host": storage_host,
                                             "port": storage_port, "user": storage_username
                                             }) as pool:
        storage = Storage(pool)
        storage.remove_tileset(id)


@storage.command()
@click.option('--config', type=click.Path(exists=True, dir_okay=False))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port', type=click.INT)
@click.option('--storage-username')
@click.option('--id', help='Override YAML config ID')
@click.option('-z', '--zoom', required=True, type=click.INT)
@click.option('-x', required=True, type=click.INT)
@click.option('-y', required=True, type=click.INT)
def inspect(config: str,
            storage_dbname: str, storage_host: str, storage_port: int, storage_username: str,
            id: str, zoom: int, x: int, y: int) -> None:
    ''' Inspect a tile in storage'''
    if config is None and id is None:
        raise click.UsageError('''Missing one of '--id' or '--config' options''')

    # No id specified, so load the config for one. We know from above config is not none.
    c = None
    if id is None:
        c = tilekiln.load_config(config)
        id = c.id

    with psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                     check=psycopg_pool.ConnectionPool.check_connection,
                                     kwargs={"dbname": storage_dbname, "host": storage_host,
                                             "port": storage_port, "user": storage_username
                                             }) as conn:
        storage = Storage(conn)
        tile = Tile(zoom, x, y)
        mvt = storage.get_tile_details(id, tile)
        size = 0
        click.echo(f"Tile {zoom}/{x}/{y} in {id}")
        for layer, data in mvt.items():
            click.echo(f"   {layer}: {data_info(data)}")
            if data is not None:
                size += len(data[0])
        click.echo(f"Total stored: {size}b")


def data_info(data: tuple[bytes, datetime.datetime] | None):
    if data is None:
        return "undefined"
    if data[1] is None:
        return f"{len(data[0])}b timestamp invalidly null"
    return f"{len(data[0])}b {data[1].isoformat()}"


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

    with psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                     check=psycopg_pool.ConnectionPool.check_connection,
                                     kwargs={"dbname": storage_dbname, "host": storage_host,
                                             "port": storage_port, "user": storage_username
                                             }) as conn:
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

    with psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                     check=psycopg_pool.ConnectionPool.check_connection,
                                     kwargs={"dbname": storage_dbname, "host": storage_host,
                                             "port": storage_port, "user": storage_username
                                             }) as pool:
        storage = Storage(pool)

        # TODO: This requires reading all of stdin before starting. This lets it display
        # how many tiles to delete but also means it has to read them all in before starting
        tiles = {Tile.from_string(t) for t in sys.stdin}
        click.echo(f"Deleting {len(tiles)} tiles")
        storage.delete_tiles(id, tiles)


@storage.command()
@click.option('--config', type=click.Path(exists=True, dir_okay=False))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port', type=click.INT)
@click.option('--storage-username')
@click.option('--id', help='Override YAML config ID')
def layerdelete(config: str,
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

    with psycopg_pool.ConnectionPool(min_size=1, max_size=1, num_workers=1,
                                     check=psycopg_pool.ConnectionPool.check_connection,
                                     kwargs={"dbname": storage_dbname, "host": storage_host,
                                             "port": storage_port, "user": storage_username
                                             }) as pool:
        storage = Storage(pool)

        tilelayers = layer_frominput(sys.stdin.read())
        click.echo(f"Deleting {len(tilelayers)} tiles")
        storage.delete_tilelayers(id, tilelayers)

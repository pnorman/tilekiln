import click
import tilekiln
from tilekiln.tile import Tile
import sys
import tilekiln.server
from tilekiln.storage import Storage
import uvicorn
import psycopg_pool
import os


@click.group()
def cli():
    pass


@cli.group()
def config():
    '''Commands to work with and check config files'''
    pass


@config.command()
@click.argument('config', type=click.Path(exists=True))
def test(config):
    '''Tests a tilekiln config for validity'''
    tilekiln.load_config(config)


@cli.command()
@click.argument('config', type=click.Path(exists=True))
@click.option('--layer', type=click.STRING)
@click.option('--zoom', '-z', type=click.INT, required=True)
@click.option('-x', type=click.INT, required=True)
@click.option('-y', type=click.INT, required=True)
def sql(config, layer, zoom, x, y):
    '''Prints the SQL for a tile
    '''

    c = tilekiln.load_config(config)

    if layer is None:
        for sql in c.layer_queries(Tile(zoom, x, y)):
            click.echo(sql)
        sys.exit(0)
    else:
        # Iterate through the layers to find the right one
        for lc in c.layers:
            if lc.id == layer:
                sql = lc.render_sql(Tile(zoom, x, y))
                if sql is None:
                    click.echo((f"Zoom {zoom} not between min zoom {lc.minzoom} "
                                f"and max zoom {lc.maxzoom} for layer {layer}."), err=True)
                    sys.exit(1)
                click.echo(sql)
                sys.exit(0)
        click.echo(f"Layer '{layer}' not found in configuration", err=True)
        sys.exit(1)


@cli.command()
@click.argument('config', type=click.Path(exists=True))
@click.option('--bind-host', default='127.0.0.1', show_default=True,
              help='Bind socket to this host. ')
@click.option('--bind-port', default=8000, show_default=True,
              type=click.INT, help='Bind socket to this port.')
@click.option('-n', '--num-threads', default=len(os.sched_getaffinity(0)),
              show_default=True, help='Number of worker processes.')
@click.option('-d', '--dbname')
@click.option('-h', '--host')
@click.option('-p', '--port')
@click.option('-U', '--username')
def dev(config, bind_host, bind_port, num_threads, dbname, host, port, username):
    '''Starts a server for development
    '''
    os.environ[tilekiln.server.TILEKILN_CONFIG] = config
    os.environ[tilekiln.server.TILEKILN_URL] = (f"http://{bind_host}:{bind_port}" +
                                                tilekiln.server.TILE_PREFIX)
    if dbname is not None:
        os.environ["PGDATABASE"] = dbname
    if host is not None:
        os.environ["PGHOST"] = host
    if port is not None:
        os.environ["PGPORT"] = port
    if username is not None:
        os.environ["PGUSER"] = username

    uvicorn.run("tilekiln.server:dev", host=bind_host, port=bind_port, workers=num_threads)


@cli.group()
def storage():
    '''Commands working with tile storage'''
    pass


@storage.command()
@click.argument('config', type=click.Path(exists=True))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
def init(config, storage_dbname, storage_host, storage_port, storage_username):
    ''' Initialize storage for tiles'''
    c = tilekiln.load_config(config)

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(c, pool)
    storage.create_tables()
    pool.close()


@storage.command()
@click.argument('config', type=click.Path(exists=True))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
def destroy(config, storage_dbname, storage_host, storage_port, storage_username):
    ''' Destroy storage for tiles'''
    c = tilekiln.load_config(config)

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(c, pool)
    storage.remove_tables()
    pool.close()


@storage.command()
@click.argument('config', type=click.Path(exists=True))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('-z', '--zoom', type=click.INT, multiple=True)
def delete(config, storage_dbname, storage_host, storage_port, storage_username, zoom):
    ''' Delete tiles from storage, optionally by-zoom'''
    c = tilekiln.load_config(config)

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(c, pool)

    if (zoom == ()):
        storage.truncate_tables()
    else:
        storage.truncate_tables(zoom)

    pool.close()


@storage.command()
@click.argument('config', type=click.Path(exists=True))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
def tiledelete(config, storage_dbname, storage_host, storage_port, storage_username):
    c = tilekiln.load_config(config)

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(c, pool)

    tiles = {Tile.from_string(t) for t in sys.stdin}
    click.echo(f"Deleting {len(tiles)} tiles")
    storage.delete_tiles(tiles)

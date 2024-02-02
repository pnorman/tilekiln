import os
import sys

import click
import psycopg
import psycopg_pool
import uvicorn

import tilekiln
import tilekiln.dev
import tilekiln.server
from tilekiln.tile import Tile
from tilekiln.tileset import Tileset
from tilekiln.storage import Storage
from tilekiln.kiln import Kiln


# Allocated as per https://github.com/prometheus/prometheus/wiki/Default-port-allocations
PROMETHEUS_PORT = 10013


# TODO: Refactor this into one file per group

@click.group()
def cli():
    pass


@cli.group()
def config():
    '''Commands to work with and check config files'''
    pass


@config.command()
@click.option('--config', required=True, type=click.Path(exists=True))
def test(config):
    '''Tests a tilekiln config for validity and exits with exit code 0 if it is valid'''
    tilekiln.load_config(config)


@cli.command()
@click.option('--config', required=True, type=click.Path(exists=True))
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
@click.option('--config', required=True, type=click.Path(exists=True))
@click.option('--bind-host', default='127.0.0.1', show_default=True,
              help='Bind socket to this host.')
@click.option('--bind-port', default=8000, show_default=True,
              type=click.INT, help='Bind socket to this port.')
@click.option('-n', '--num-threads', default=len(os.sched_getaffinity(0)),
              show_default=True, help='Number of worker processes.')
@click.option('-d', '--dbname')
@click.option('-h', '--host')
@click.option('-p', '--port')
@click.option('-U', '--username')
@click.option('--base-url', help='Defaults to http://127.0.0.1:8000' +
              ' or the bind host and port')
@click.option('--id', help='Override YAML config ID')
def dev(config, bind_host, bind_port, num_threads, dbname, host, port, username, base_url, id):
    '''Starts a server for development
    '''
    os.environ[tilekiln.dev.TILEKILN_CONFIG] = config
    os.environ[tilekiln.dev.TILEKILN_ID] = id or tilekiln.load_config(config).id

    if base_url is not None:
        os.environ[tilekiln.dev.TILEKILN_URL] = base_url
    else:
        os.environ[tilekiln.dev.TILEKILN_URL] = (f"http://{bind_host}:{bind_port}")
    if dbname is not None:
        os.environ["PGDATABASE"] = dbname
    if host is not None:
        os.environ["PGHOST"] = host
    if port is not None:
        os.environ["PGPORT"] = port
    if username is not None:
        os.environ["PGUSER"] = username

    uvicorn.run("tilekiln.dev:dev", host=bind_host, port=bind_port, workers=num_threads)


@cli.command()
@click.option('--config', required=True, type=click.Path(exists=True))
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
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('--base-url', help='Defaults to http://127.0.0.1:8000' +
              ' or the bind host and port')
def live(config, bind_host, bind_port, num_threads,
         dbname, host, port, username,
         storage_dbname, storage_host, storage_port, storage_username, base_url):
    '''Starts a server for pre-generated tiles from DB'''
    os.environ[tilekiln.server.TILEKILN_CONFIG] = config
    os.environ[tilekiln.server.TILEKILN_THREADS] = str(num_threads)

    if base_url is not None:
        os.environ[tilekiln.dev.TILEKILN_URL] = base_url
    else:
        os.environ[tilekiln.dev.TILEKILN_URL] = (f"http://{bind_host}:{bind_port}")
    if dbname is not None:
        os.environ["GENERATE_PGDATABASE"] = dbname
    if host is not None:
        os.environ["GENERATE_PGHOST"] = host
    if port is not None:
        os.environ["GENERATE_PGPORT"] = port
    if username is not None:
        os.environ["GENERATE_PGUSER"] = username

    if storage_dbname is not None:
        os.environ["STORAGE_PGDATABASE"] = storage_dbname
    if storage_host is not None:
        os.environ["STORAGE_PGHOST"] = storage_host
    if storage_port is not None:
        os.environ["STORAGE_PGPORT"] = storage_port
    if storage_username is not None:
        os.environ["STORAGE_PGUSER"] = storage_username

    uvicorn.run("tilekiln.server:live", host=bind_host, port=bind_port, workers=num_threads)


@cli.command()
@click.option('--bind-host', default='127.0.0.1', show_default=True,
              help='Bind socket to this host. ')
@click.option('--bind-port', default=8000, show_default=True,
              type=click.INT, help='Bind socket to this port.')
@click.option('-n', '--num-threads', default=len(os.sched_getaffinity(0)),
              show_default=True, help='Number of worker processes.')
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('--base-url', help='Defaults to http://127.0.0.1:8000' +
              ' or the bind host and port')
def serve(bind_host, bind_port, num_threads,
          storage_dbname, storage_host, storage_port, storage_username, base_url):
    '''Starts a server for pre-generated tiles from DB'''

    os.environ[tilekiln.server.TILEKILN_THREADS] = str(num_threads)

    if base_url is not None:
        os.environ[tilekiln.dev.TILEKILN_URL] = base_url
    else:
        os.environ[tilekiln.dev.TILEKILN_URL] = (f"http://{bind_host}:{bind_port}")
    if storage_dbname is not None:
        os.environ["PGDATABASE"] = storage_dbname
    if storage_host is not None:
        os.environ["PGHOST"] = storage_host
    if storage_port is not None:
        os.environ["PGPORT"] = storage_port
    if storage_username is not None:
        os.environ["PGUSER"] = storage_username

    uvicorn.run("tilekiln.server:server", host=bind_host, port=bind_port, workers=num_threads)


@cli.group()
def storage():
    '''Commands working with tile storage'''
    pass


@storage.command()
@click.option('--config', required=True, type=click.Path(exists=True))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('--id', help='Override YAML config ID')
def init(config, storage_dbname, storage_host, storage_port, storage_username, id):
    ''' Initialize storage for tiles'''

    c = tilekiln.load_config(config)

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(pool)
    storage.create_schema()
    tileset = Tileset.from_config(storage, c)
    tileset.prepare_storage()
    pool.close()


@storage.command()
@click.option('--config', type=click.Path(exists=True))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('--id', help='Override YAML config ID')
def destroy(config, storage_dbname, storage_host, storage_port, storage_username, id):
    ''' Destroy storage for tiles'''
    if config is None and id is None:
        raise click.UsageError('''Missing one of '--id' or '--config' options''')

    # No id specified, so load the config for one. We know from above config is not none.
    c = None
    if id is None:
        c = tilekiln.load_config(config)

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(c, pool, id)
    storage.remove_tables()
    pool.close()


@storage.command()
@click.option('--config', type=click.Path(exists=True))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('-z', '--zoom', type=click.INT, multiple=True)
@click.option('--id', help='Override YAML config ID')
def delete(config, storage_dbname, storage_host, storage_port, storage_username, zoom, id):
    ''' Delete tiles from storage, optionally by-zoom'''
    if config is None and id is None:
        raise click.UsageError('''Missing one of '--id' or '--config' options''')

    # No id specified, so load the config for one. We know from above config is not none.
    c = None
    if id is None:
        c = tilekiln.load_config(config)

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(c, pool, id)

    if (zoom == ()):
        storage.truncate_tables()
    else:
        storage.truncate_tables(zoom)

    pool.close()


@storage.command()
@click.option('--config', type=click.Path(exists=True))
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('--id', help='Override YAML config ID')
def tiledelete(config, storage_dbname, storage_host, storage_port, storage_username, id):
    '''Delete specific tiles
       Pass a list of z/x/y to stdin to generate those tiles'''
    if config is None and id is None:
        raise click.UsageError('''Missing one of '--id' or '--config' options''')

    # No id specified, so load the config for one. We know from above config is not none.
    c = None
    if id is None:
        c = tilekiln.load_config(config)

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(c, pool, id)

    tiles = {Tile.from_string(t) for t in sys.stdin}
    click.echo(f"Deleting {len(tiles)} tiles")
    storage.delete_tiles(tiles)


@cli.group()
def generate():
    '''Commands for tile generation'''
    pass


@generate.command()
@click.option('--config', required=True, type=click.Path(exists=True))
@click.option('-n', '--num-threads', default=len(os.sched_getaffinity(0)),
              show_default=True, help='Number of worker processes.')
@click.option('-d', '--dbname')
@click.option('-h', '--host')
@click.option('-p', '--port')
@click.option('-U', '--username')
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
def tiles(config, num_threads, dbname, host, port, username,
          storage_dbname, storage_host, storage_port, storage_username):
    '''Generate specific tiles.
       Pass a list of z/x/y to stdin to generate those tiles'''

    c = tilekiln.load_config(config)

    tiles = {Tile.from_string(t) for t in sys.stdin}
    threads = min(num_threads, len(tiles))  # No point in more threads than tiles

    click.echo(f"Rendering {len(tiles)} tiles over {threads} threads")

    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(c, pool)

    gen_conn = psycopg.connect(dbname=dbname, host=host, port=port, username=username)
    kiln = Kiln(c, gen_conn)
    for tile in tiles:
        mvt = kiln.render(tile)
        storage.save_tile(tile, mvt)


@cli.command()
@click.option('--bind-host', default='0.0.0.0', show_default=True,
              help='Bind socket to this host. ')
@click.option('--bind-port', default=PROMETHEUS_PORT, show_default=True,
              type=click.INT, help='Bind socket to this port.')
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
# TODO: remove on storage refactor
@click.option('--id', help='Override YAML config ID', required=True)
def prometheus(bind_host, bind_port,
               storage_dbname, storage_host, storage_port, storage_username, id):
    ''' Run a prometheus exporter which can be accessed for gathering metrics
        on stored tiles. '''
    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(None, pool, id)
    # tilekiln.prometheus brings in a bunch of stuff, so only do this
    # for this command
    from tilekiln.prometheus import serve_prometheus
    # TODO: make sleep a parameter
    click.echo(f'Running prometheus exporter on http://{bind_host}:{bind_port}/')
    serve_prometheus(storage, bind_host, bind_port, 15)

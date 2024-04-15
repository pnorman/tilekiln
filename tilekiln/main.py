import os

import click
import psycopg_pool
import uvicorn

import tilekiln
import tilekiln.dev
import tilekiln.server
import tilekiln.scripts.config
import tilekiln.scripts.generate
import tilekiln.scripts.storage
from tilekiln.storage import Storage


# Allocated as per https://github.com/prometheus/prometheus/wiki/Default-port-allocations
PROMETHEUS_PORT = 10013


# We want click to print out commands in the order they are defined.
class OrderCommands(click.Group):
    def list_commands(self, ctx: click.Context) -> list[str]:
        return list(self.commands)


@click.group(cls=OrderCommands)
def cli() -> None:
    pass


cli.add_command(tilekiln.scripts.config.config)
cli.add_command(tilekiln.scripts.generate.generate)
cli.add_command(tilekiln.scripts.storage.storage)


@cli.command()
@click.option('--config', required=True, type=click.Path(exists=True, dir_okay=False))
@click.option('--bind-host', default='127.0.0.1', show_default=True,
              help='Bind socket to this host.')
@click.option('--bind-port', default=8000, show_default=True,
              type=click.INT, help='Bind socket to this port.')
@click.option('-n', '--num-threads', default=len(os.sched_getaffinity(0)),
              show_default=True, help='Number of worker processes.')
@click.option('--source-dbname')
@click.option('--source-host', type=click.INT)
@click.option('--source-port')
@click.option('--source-username')
@click.option('--base-url', help='Defaults to http://127.0.0.1:8000' +
              ' or the bind host and port')
@click.option('--id', help='Override YAML config ID')
def dev(config: str, bind_host: str, bind_port: int, num_threads: int,
        source_dbname: str, source_host: str, source_port: int, source_username: str,
        base_url: str, id: str) -> None:
    '''Starts a server for development
    '''
    os.environ[tilekiln.dev.TILEKILN_CONFIG] = config
    os.environ[tilekiln.dev.TILEKILN_ID] = id or tilekiln.load_config(config).id

    if base_url is not None:
        os.environ[tilekiln.dev.TILEKILN_URL] = base_url
    else:
        os.environ[tilekiln.dev.TILEKILN_URL] = (f"http://{bind_host}:{bind_port}")
    if source_dbname is not None:
        os.environ["PGDATABASE"] = source_dbname
    if source_host is not None:
        os.environ["PGHOST"] = source_host
    if source_port is not None:
        os.environ["PGPORT"] = str(source_port)
    if source_username is not None:
        os.environ["PGUSER"] = source_username

    uvicorn.run("tilekiln.dev:dev", host=bind_host, port=bind_port, workers=num_threads)


@cli.command()
@click.option('--config', required=True, type=click.Path(exists=True, dir_okay=False))
@click.option('--bind-host', default='127.0.0.1', show_default=True,
              help='Bind socket to this host. ')
@click.option('--bind-port', default=8000, show_default=True,
              type=click.INT, help='Bind socket to this port.')
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
@click.option('--base-url', help='Defaults to http://127.0.0.1:8000' +
              ' or the bind host and port')
def live(config: str, bind_host: str, bind_port: int, num_threads: int,
         source_dbname: str, source_host: str, source_port: int, source_username: str,
         storage_dbname: str, storage_host: str, storage_port: int, storage_username: str,
         base_url: str) -> None:
    '''Starts a server for pre-generated tiles from DB'''
    os.environ[tilekiln.server.TILEKILN_CONFIG] = config
    os.environ[tilekiln.server.TILEKILN_THREADS] = str(num_threads)

    if base_url is not None:
        os.environ[tilekiln.dev.TILEKILN_URL] = base_url
    else:
        os.environ[tilekiln.dev.TILEKILN_URL] = (f"http://{bind_host}:{bind_port}")
    if source_dbname is not None:
        os.environ["GENERATE_PGDATABASE"] = source_dbname
    if source_host is not None:
        os.environ["GENERATE_PGHOST"] = source_host
    if source_port is not None:
        os.environ["GENERATE_PGPORT"] = str(source_port)
    if source_username is not None:
        os.environ["GENERATE_PGUSER"] = source_username

    if storage_dbname is not None:
        os.environ["STORAGE_PGDATABASE"] = storage_dbname
    if storage_host is not None:
        os.environ["STORAGE_PGHOST"] = storage_host
    if storage_port is not None:
        os.environ["STORAGE_PGPORT"] = str(storage_port)
    if storage_username is not None:
        os.environ["STORAGE_PGUSER"] = storage_username

    uvicorn.run("tilekiln.server:live", host=bind_host, port=bind_port, workers=num_threads)


@cli.command()
@click.option('--bind-host', default='127.0.0.1', show_default=True,
              help='Bind socket to this host. ')
@click.option('--bind-port', default=8000, show_default=True,
              type=click.INT, help='Bind socket to this port.')
@click.option('-n', '--num-threads', default=len(os.sched_getaffinity(0)),
              type=click.INT, show_default=True, help='Number of worker processes.')
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port')
@click.option('--storage-username')
@click.option('--base-url', help='Defaults to http://127.0.0.1:8000' +
              ' or the bind host and port')
def serve(bind_host: str, bind_port: int, num_threads: int,
          storage_dbname: str, storage_host: str, storage_port: int, storage_username: str,
          base_url: str) -> None:
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
        os.environ["PGPORT"] = str(storage_port)
    if storage_username is not None:
        os.environ["PGUSER"] = storage_username

    uvicorn.run("tilekiln.server:server", host=bind_host, port=bind_port, workers=num_threads)


@cli.command()
@click.option('--bind-host', default='0.0.0.0', show_default=True,
              help='Bind socket to this host. ')
@click.option('--bind-port', default=PROMETHEUS_PORT, show_default=True,
              type=click.INT, help='Bind socket to this port.')
@click.option('--storage-dbname')
@click.option('--storage-host')
@click.option('--storage-port', type=click.INT)
@click.option('--storage-username')
def prometheus(bind_host: str, bind_port: int, storage_dbname: str, storage_host: str,
               storage_port: int, storage_username: str) -> None:
    ''' Run a prometheus exporter for metrics on tiles.'''
    pool = psycopg_pool.NullConnectionPool(kwargs={"dbname": storage_dbname,
                                                   "host": storage_host,
                                                   "port": storage_port,
                                                   "user": storage_username})
    storage = Storage(pool)

    # tilekiln.prometheus brings in a bunch of stuff, so only do this
    # for this command
    from tilekiln.prometheus import serve_prometheus
    # TODO: make sleep a parameter
    click.echo(f'Running prometheus exporter on http://{bind_host}:{bind_port}/')
    serve_prometheus(storage, bind_host, bind_port, 15)

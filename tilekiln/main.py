import click
import psycopg

import tilekiln
import tilekiln.dev
import tilekiln.server
import tilekiln.scripts.config
import tilekiln.scripts.generate
import tilekiln.scripts.serve
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
cli.add_command(tilekiln.scripts.serve.serve)


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
    '''Run a prometheus exporter for metrics on tiles.'''
    with psycopg.connect(dbname=storage_dbname,
                         host=storage_host,
                         port=storage_port,
                         user=storage_username) as conn:
        storage = Storage(conn)

        # tilekiln.prometheus brings in a bunch of stuff, so only do this
        # for this command
        from tilekiln.prometheus import serve_prometheus
        # TODO: make sleep a parameter
        click.echo(f'Running prometheus exporter on http://{bind_host}:{bind_port}/')
        serve_prometheus(storage, bind_host, bind_port, 15)

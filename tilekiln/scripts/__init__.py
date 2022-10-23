import click
import tilekiln
from tilekiln.tile import Tile
import sys


@click.group()
def cli():
    pass


@cli.group()
def config():
    pass


@config.command()
@click.argument('config', type=click.Path(exists=True))
def test(config):
    tilekiln.load_config(config)


@cli.command()
@click.argument('config', type=click.Path(exists=True))
@click.option('--layer', type=click.STRING, required=True)
@click.option('--zoom', '-z', type=click.INT, required=True)
@click.option('-x', type=click.INT, required=True)
@click.option('-y', type=click.INT, required=True)
def sql(config, layer, zoom, x, y):
    '''Prints the SQL for a layer
    '''
    # Iterate through the layers to find the right one
    for lc in tilekiln.load_config(config).layers:
        if lc.id == layer:
            sql = lc.render_sql(Tile(zoom, x, y))
            if sql is None:
                click.echo(f"Zoom {zoom} not between min zoom {lc.minzoom} "
                           f"and max zoom {lc.maxzoom} for layer {layer}.", err=True)
                sys.exit(1)
            click.echo(sql)
            sys.exit(0)
    click.echo(f"Layer '{layer}' not found in configuration", err=True)
    sys.exit(1)

import click

import tilekiln
from tilekiln.tile import Tile


@click.group()
def config() -> None:
    '''Commands to work with and check config files'''
    pass


@config.command()
@click.option('--config', required=True, type=click.Path(exists=True, dir_okay=False))
def test(config: str):
    '''Tests a config for validity.

    The process will exit with exit code 0 if tilekiln can load the config.

    This is intended for build and CI scripts used by configs.
    '''
    tilekiln.load_config(config)
    return 0


@config.command()
@click.option('--config', required=True, type=click.Path(exists=True, dir_okay=False))
@click.option('--layer', type=click.STRING)
@click.option('--zoom', '-z', type=click.INT, required=True)
@click.option('-x', type=click.INT, required=True)
@click.option('-y', type=click.INT, required=True)
def sql(config: str, layer: str, zoom: int, x: int, y: int):
    '''Print the SQL for a tile or layer.

    Prints the SQL that would be issued to generate a particular tile layer,
    or if no layer is given, the entire tile. This allows manual debugging of
    a tile query.
    '''

    c = tilekiln.load_config(config)

    if layer is None:
        for sql in c.layer_queries(Tile(zoom, x, y)):
            click.echo(sql)
        return 0
    else:
        # Iterate through the layers to find the right one
        for lc in c.layers:
            if lc.id == layer:
                sql = lc.render_sql(Tile(zoom, x, y))
                if sql is None:
                    click.echo((f"Zoom {zoom} not between min zoom {lc.minzoom} "
                                f"and max zoom {lc.maxzoom} for layer {layer}."), err=True)
                    return 1
                click.echo(sql)
                return 0
        click.echo(f"Layer '{layer}' not found in configuration", err=True)
        return 1

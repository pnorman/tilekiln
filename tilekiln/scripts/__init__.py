import click
import tilekiln


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

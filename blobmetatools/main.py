from pathlib import Path
import click
import shutil
import pyfuse3
from peewee import *
import subprocess

from blobmetatools import models

DATA_SIGNATURE = '.blobmetadata'

@click.command()
@click.argument('datadir', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--overwrite/--no-overwrite', default=True)
def initdb(datadir, overwrite):
    if datadir.exists():
        if not overwrite:
            raise click.Abort('data directory already exists')
        click.echo(f'recreating `{datadir}`')
        signature = datadir / DATA_SIGNATURE
        assert (datadir / DATA_SIGNATURE).exists()
        shutil.rmtree(datadir)
        datadir.mkdir()
        (datadir / DATA_SIGNATURE).touch()

    db = models.connect_sqlite(datadir / 'db.sqlite')
    db.create_tables([models.Blob])

    for index, name in enumerate('foo bar baz'.split(), 1):
        (Path('data')/name).write_text(name)
        models.Blob(id=pyfuse3.ROOT_INODE+index, name=name).save(force_insert=True)

    click.echo('done')

from pathlib import Path
import click
import shutil
import pyfuse3
import subprocess
import peewee
from playhouse.reflection import generate_models

DATA_SIGNATURE = '.blobmetadata'

@click.command()
@click.argument('datadir', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--overwrite/--no-overwrite', default=True)
@click.option('--metadata-column', default='metadata')
def initdb(datadir, overwrite, metadata_column):
    if datadir.exists():
        if not overwrite:
            raise click.Abort('data directory already exists')
        click.echo(f'recreating `{datadir}`')
        signature = datadir / DATA_SIGNATURE
        assert (datadir / DATA_SIGNATURE).exists()
        shutil.rmtree(datadir)
        datadir.mkdir()
        (datadir / DATA_SIGNATURE).touch()

    attrs = {
        'name': peewee.CharField(unique=True),
        metadata_column: peewee.IntegerField(),
    }
    Blob = type('Blob', (peewee.Model,), attrs)

    db = peewee.SqliteDatabase(datadir / 'db.sqlite')
    db.bind([Blob])
    db.connect()
    db.create_tables([Blob])

    values = {'foo': 1, 'bar': 2, 'baz': 2, 'qux': 1}
    for index, name in enumerate(values, 1):
        (Path('data')/name).write_text(name)
        kwargs = {
            'id': pyfuse3.ROOT_INODE+index,
            'name': name,
            metadata_column: values[name],
        }
        Blob(**kwargs).save(force_insert=True)

    click.echo('done')

@click.command()
@click.argument('datadir', type=click.Path(exists=True, file_okay=False, path_type=Path))
def dbshell(datadir):
    db = peewee.SqliteDatabase(datadir / 'db.sqlite')
    models = generate_models(db)
    import IPython
    IPython.embed()

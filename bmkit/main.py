from pathlib import Path
import json
import io
import csv
import click
import shutil
import pyfuse3
import subprocess
import peewee
from playhouse.reflection import generate_models

from bmkit import blobmeta

DATA_SIGNATURE = '.blobmetadata'

TYPE_TO_FIELD_TYPE = {
    str: peewee.CharField,
    int: peewee.IntegerField,
    float: peewee.FloatField,
    bool: peewee.BooleanField,
}

def model_from_dict(record):
    if 'path' not in record:
        raise ValueError("blobmetafs expects a field called 'path'")
    model_attrs = {
        'path': peewee.CharField(unique=True),
    }
    for field_name, sample_value in record.items():
        if field_name == 'path':
            continue
        try:
            model_attrs[field_name] = TYPE_TO_FIELD_TYPE[type(sample_value)]()
        except KeyError:
            raise RuntimeError(f'unknown type for field {field_name}')
    return type('Blob', (peewee.Model,), model_attrs)

@click.command()
@click.argument('datadir', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--overwrite/--no-overwrite', default=True)
@click.option('--sample-records', help="Path to JSON we'll use to populate database")
def initdata(datadir, overwrite, sample_records):
    if datadir.exists():
        if not overwrite:
            raise click.Abort('data directory already exists')
        click.echo(f'recreating `{datadir}`')
        signature = datadir / DATA_SIGNATURE
        assert (datadir / DATA_SIGNATURE).exists()
        shutil.rmtree(datadir)
        datadir.mkdir()
        (datadir / DATA_SIGNATURE).touch()

    if sample_records is None:
        data = []
        Blob = model_from_dict({'path': ''})
    else:
        with open(sample_records) as handle:
            data = json.load(handle)
        Blob = model_from_dict(data[0])

    config = {'pathdefs': {}}
    for field in Blob._meta.fields:
        config['pathdefs'][f'by-{field}'] = [field]
    with open(datadir / 'config.json', 'w') as handle:
        json.dump(config, handle, indent=4)


    db = peewee.SqliteDatabase(datadir / 'db.sqlite')
    db.bind([Blob])
    db.connect()
    db.create_tables([Blob])

    for record in data:
        (Path('data')/record['path']).write_text(json.dumps(record, indent=4))
        Blob(**record).save(force_insert=True)

    click.echo('done')

@click.command()
@click.argument('datadir', type=click.Path(exists=True, file_okay=False, path_type=Path))
def shell(datadir):
    db = peewee.SqliteDatabase(datadir / 'db.sqlite')
    models = generate_models(db)
    Blob = models['blob']

    with open(datadir / 'config.json') as handle:
        config = json.load(handle)
    bm = blobmeta.BlobMeta(Blob, config['pathdefs'])

    import IPython
    IPython.embed()

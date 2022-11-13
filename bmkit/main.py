from pathlib import Path
import json
import click
import pyfuse3
import peewee
from playhouse.reflection import generate_models

from bmkit import blobmeta, create
from bmkit import DATABASE, CONFIG

@click.command()
@click.argument('datadir', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--delete-existing/--no-delete-existing', default=True)
@click.option('--populate-from-json', help="Path to JSON we'll use to populate database")
def initdata(datadir, delete_existing, populate_from_json):
    create.create_datadir(datadir, delete_existing=delete_existing)

    if populate_from_json is None:
        records = []
        Blob = create.create_schema_from_sample_record(datadir, {'path': ''})
    else:
        with open(populate_from_json) as handle:
            records = json.load(handle)
        Blob = create.create_schema_from_sample_record(datadir, records[0])

    config = create.autogenerate_config_from_model(Blob)
    with open((datadir / CONFIG), 'w') as handle:
        json.dump(config, handle, indent=4)

    create.populate_sample_records(datadir, Blob, records)

@click.command()
@click.argument('datadir', type=click.Path(exists=True, file_okay=False, path_type=Path))
def shell(datadir):
    db = peewee.SqliteDatabase(datadir / DATABASE)
    models = generate_models(db)
    Blob = models['blob']

    with open(datadir / CONFIG) as handle:
        config = json.load(handle)
    bm = blobmeta.BlobMeta(Blob, config['pathdefs'])

    import IPython
    IPython.embed()

import json
import shutil
import peewee

from bmkit import SIGNATURE, DATABASE

TYPE_TO_FIELD_TYPE = {
    str: peewee.CharField,
    int: peewee.IntegerField,
    float: peewee.FloatField,
    bool: peewee.BooleanField,
}

def create_datadir(path, delete_existing):
    signature = path / SIGNATURE
    if path.exists():
        if not delete_existing:
            raise FileExistsError(f'{path} already exists')
        if not signature.exists():
            raise RuntimeError(f'{signature} not found; refusing to delete parent')
        shutil.rmtree(path)
    path.mkdir()
    signature.touch()
    return path

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

def create_schema_from_sample_record(datadir, sample_record):
    Blob = model_from_dict(sample_record)
    db = peewee.SqliteDatabase(datadir / DATABASE)
    db.bind([Blob])
    db.connect()
    db.create_tables([Blob])
    return Blob

def mangle_pythonic_to_filesystem(name):
    return name.replace('_', '-')

def autogenerate_config_from_model(Blob):
    config = {'pathdefs': {}}
    for field_name in Blob._meta.fields:
        if field_name in ('path', 'id'):
            continue
        config['pathdefs'][f'by-{mangle_pythonic_to_filesystem(field_name)}'] = [field_name]
    return config

def populate_sample_records(datadir, Blob, records):
    for record in records:
        with open((datadir / record['path']), 'w') as handle:
            json.dump(record, handle, indent=4)
        Blob(**record).save(force_insert=True)

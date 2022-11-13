import pytest
import peewee
from bmkit import create
from bmkit import DATABASE

def test_create_datadir(tmp_path):
    with pytest.raises(FileExistsError):
        # check we won't clobber a directory unless delete_existing=True
        create.create_datadir(tmp_path, delete_existing=False)
    with pytest.raises(RuntimeError):
        # check that even with delete_existing we won't delete without the signature
        create.create_datadir(tmp_path, delete_existing=True)
    # remove existing
    tmp_path.rmdir()
    create.create_datadir(tmp_path, delete_existing=False)
    # ensure that delete_existing works with the signature in place
    myfile = tmp_path / 'foo'
    myfile.touch()
    create.create_datadir(tmp_path, delete_existing=True)
    assert not myfile.exists()

def test_model_from_dict():
    with pytest.raises(ValueError):
        create.model_from_dict({})
    model = create.model_from_dict({'path': ''})
    assert set(model._meta.fields) == {'id', 'path'}
    model = create.model_from_dict({'path': '', 'int': 1, 'float': 2.0, 'str': 'foo'})
    assert type(model.int) is peewee.IntegerField
    assert type(model.float) is peewee.FloatField
    assert type(model.str) is peewee.CharField

def test_create_schema_from_sample_record(tmp_path):
    datadir = create.create_datadir(tmp_path / 'datadir', True)
    create.create_schema_from_sample_record(datadir, {'path': ''})
    assert (datadir / DATABASE).exists()

def test_autogenerate_config_from_model():
    class Blob(peewee.Model):
        path = peewee.CharField(unique=True)
        foo = peewee.IntegerField()
        bar_with_underscores = peewee.FloatField()
    config = create.autogenerate_config_from_model(Blob)
    assert config['pathdefs']['by-foo'] == ['foo']
    assert config['pathdefs']['by-bar-with-underscores'] == ['bar_with_underscores']
    assert 'by-path' not in config['pathdefs']

import peewee

class Blob(peewee.Model):
    name = peewee.CharField(unique=True)

def connect_sqlite(sqlite_path):
    db = peewee.SqliteDatabase(sqlite_path)
    db.bind([Blob])
    db.connect()
    return db

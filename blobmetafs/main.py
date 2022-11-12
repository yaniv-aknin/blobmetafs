#!/usr/bin/env python3

from pathlib import Path 
import click
import errno
import functools
import logging
import os
import stat

# I've made the questionable design choice to use the synchronouse version of
# peewee/sqlite with the other async trio/pyfuse3. The tl;dr is that SQLite
# itself is C synchronous sync code, and libraries like aiosqlite merely wrap
# it in a thread and synchronize all use of the database via that thread.
# After running into threading related quirks [0] I've decided it's not worth
# pulling in threading related headaches given SQLite's synchronous nature.
#
# [0]: https://github.com/omnilib/aiosqlite/issues/74
import peewee
from playhouse.reflection import generate_models
import pyfuse3
import trio

from blobmetatools.main import DATA_SIGNATURE

def not_exist_to_no_ent(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except peewee.DoesNotExist:
            raise pyfuse3.FUSEError(errno.ENOENT)
    return wrapper

class BlobMetaFs(pyfuse3.Operations):

    def __init__(self, Blob):
        super(BlobMetaFs, self).__init__()
        self.Blob = Blob

    @not_exist_to_no_ent
    def blob_by_id(self, id):
        return self.Blob.get(self.Blob.id == id)

    @not_exist_to_no_ent
    def blob_by_name(self, name):
        return self.Blob.get(self.Blob.name == name)

    async def getattr(self, inode, ctx=None):
        entry = pyfuse3.EntryAttributes()
        if inode == pyfuse3.ROOT_INODE:
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_size = 0
        else:
            blob = self.blob_by_id(inode)
            entry.st_mode = (stat.S_IFLNK | 0o777)
            entry.st_size = len(blob.name)

        stamp = int(1438467123.985654 * 1e9)
        entry.st_atime_ns = stamp
        entry.st_ctime_ns = stamp
        entry.st_mtime_ns = stamp
        entry.st_gid = os.getgid()
        entry.st_uid = os.getuid()
        entry.st_ino = inode

        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        if parent_inode != pyfuse3.ROOT_INODE:
            raise pyfuse3.FUSEError(errno.ENOENT)
        blob = self.blob_by_name(name)
        return self.getattr(blob.id)

    async def opendir(self, inode, ctx):
        if inode != pyfuse3.ROOT_INODE:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return inode

    async def readdir(self, fh, start_id, token):
        assert fh == pyfuse3.ROOT_INODE
        for blob in self.Blob.select().offset(start_id):
            start_id += 1
            attr = await self.getattr(blob.id)
            if pyfuse3.readdir_reply(token, os.fsencode(blob.name), attr, start_id) is False:
                return

    async def readlink(self, inode, ctx):
        blob = self.blob_by_id(inode)
        return os.fsencode(blob.name)

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

@click.command
@click.argument('mountpoint', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument('datadir', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--debug', type=click.BOOL, default=False)
@click.option('--debug-fuse', type=click.BOOL, default=False)
def main(mountpoint, datadir, debug, debug_fuse):
    if not (datadir / DATA_SIGNATURE).exists():
        raise click.Abort(f"can't find {DATA_SIGNATURE} in {datadir}")
    init_logging(debug)

    db = peewee.SqliteDatabase(datadir / 'db.sqlite')
    db.connect()
    models = generate_models(db)
    if 'blob' not in models:
        raise click.Abort("can't find Blob table")

    bmfs = BlobMetaFs(models['blob'])
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=blobmetafs')
    if debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(bmfs, str(mountpoint), fuse_options)
    try:
        trio.run(pyfuse3.main)
    except KeyboardInterrupt:
        pass
    except:
        pyfuse3.close(unmount=False)
        raise

    pyfuse3.close()


if __name__ == '__main__':
    main()

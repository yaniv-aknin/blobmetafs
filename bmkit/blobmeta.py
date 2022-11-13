from pathlib import Path
import collections

FilesystemQuery = collections.namedtuple('FilesystemQuery', 'is_directory query')

class FilenameMapper:
    @classmethod
    def record_to_name(cls, record):
        return record.id
    @classmethod
    def name_to_id(cls, name):
        return name

class BlobMeta:
    def __init__(self, Blob, pathdefs):
        self.Blob = Blob
        self.pathdefs = pathdefs
    def path_to_query(self, path):
        current_pathdef = self.pathdefs
        path = Path(path)
        query = self.Blob.select()
        for index, part in enumerate(path.parts, 1):
            if isinstance(current_pathdef, collections.abc.Mapping):
                if part not in current_pathdef:
                    raise LookupError(f"{part} in {path} doesn't map to {tuple(current_pathdef)}")
                current_pathdef = current_pathdef[part]
                continue
            if not isinstance(current_pathdef, collections.abc.Iterator):
                current_pathdef = iter(current_pathdef)
            try:
                field = next(current_pathdef)
            except StopIteration:
                if index < len(path.parts):
                    raise LookupError(f"{path} has too many parts")
                return FilesystemQuery(
                    is_directory = False,
                    query = query.where(self.Blob.id == FilenameMapper.name_to_id(part))
                )
            query = query.where(self.Blob._meta.fields[field] == part)
        return FilesystemQuery(is_directory=True, query=query)


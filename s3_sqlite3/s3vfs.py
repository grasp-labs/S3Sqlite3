"""
SQLite 3.6 has new VFS functionality which defines the interface between the SQLite core and the underlying operating system.
More infomation can be found here: https://www.sqlite.org/vfs.html.
This S3VFS allows users to access slite3 database file in the aws s3 storage directly.
"""
import apsw
import boto3

from typing import Union, Iterator

from .s3vfsfile import S3VFSFile

class S3VFS(apsw.VFS):
    """
    Implement Sqlite3 VFS interface. https://www.sqlite.org/c3ref/vfs.html
    """
    def __init__(
        self,
        bucket: str,
        block_size: int=4096
    ):
        s3 = boto3.resource("s3")
        self.name = "s3vfs"
        self._bucket = s3.Bucket(bucket)
        self._block_size = block_size
        super().__init__(name=self.name, base='')

    def xAccess(
        self,
        pathname: str,
        flags: Union[str, int]
    ) -> bool:
        """
        Simply check whether the sqlite3 db directory is reachable.
        """
        return flags == apsw.mapping_access["SQLITE_ACCESS_EXISTS"] and any(self._bucket.objects.filter(Prefix=f'{pathname}/')) or flags != apsw.mapping_access["SQLITE_ACCESS_EXISTS"]

    def xFullPathname(
        self,
        filename: str
    ) -> str:
        """
        Get sqlite3 db full path name.
        In s3, it is identical to the object key.
        """
        return filename

    def xDelete(
        self,
        filename: str,
        syncdir: int
    ):
        """
        VFS method to delete the file db.
        """
        self._bucket.objects.filter(Prefix=f"{filename}/").delete()

    def xOpen(
        self,
        name: apsw.URIFilename,
        flags: int
    ) -> S3VFSFile:
        """
        Open the sqlite3 file db.
        :param name: object key for sqlite3 database name.
        :return:
        """
        return S3VFSFile(name, flags, self._bucket, self._block_size)

    def serialize_iter(
        self,
        key_prefix: str
    ) -> Iterator:
        """Seralize file database iterator"""
        for obj in self._bucket.objects.filter(Prefix=f'{key_prefix}/'):
            yield from obj.get()['Body'].iter_chunks()

    def serialize_fileobj(
        self,
        key_prefix: str
    ):
        """
        Serailze file content of given object in s3.
        :param key_prefix: s3 object key
        """
        chunk: bytes = b''
        offset: int = 0
        it = iter(self.serialize_iter(key_prefix))

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        class FileLikeObj:
            def read(self, n=-1):
                n = \
                    n if n != -1 else \
                    4294967294 * 65536  # max size of SQLite file
                return b''.join(up_to_iter(n))

        return FileLikeObj()

    def deserialize_iter(
        self,
        key_prefix: str,
        bytes_iter: Iterator
    ):
        """
        Deserialize to given s3 object.
        :param key_prefix: object key in s3.
        """
        chunk = b''
        offset = 0
        it = iter(bytes_iter)

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        def block_bytes_iter():
            while True:
                block = b''.join(up_to_iter(self._block_size))
                if not block:
                    break
                yield block

        for block, block_bytes in enumerate(block_bytes_iter()):
            """Here we need to convert the block into a 0-padded string with 10 characters."""
            self._bucket.Object(f'{key_prefix}/{block:010d}').put(Body=block_bytes)
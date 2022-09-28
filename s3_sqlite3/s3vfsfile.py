"""
Handle S3 object as file.
"""
import apsw

from typing import Iterator, Tuple


class S3VFSFile:
    """
    S3VFSFile to support vfs interface in the sqlite3.
    """
    def __init__(
        self,
        name: apsw.URIFilename,
        flags: int,
        bucket,
        block_size: int = 4096
    ):
        self._key_prefix = name.filename() if isinstance(name, apsw.URIFilename) else name
        self._bucket = bucket
        self._flags = flags
        self._block_size = block_size

    def _blocks(
        self,
        offset: int,
        amount: int
    ) -> Iterator[Tuple]:
        """
        Iterator for blocks in file obj.
        :param offset: starting position to read data.
        :param amount: size of the data to be read.
        :return: Tuple object indicating (index of the block, starting position and size to be loaded).
        """
        while amount > 0:
            block:int = offset // self._block_size  # which block to get
            start:int = offset % self._block_size   # place in block to start
            consume:int = min(self._block_size - start, amount)
            yield (block, start, consume)
            amount -= consume
            offset += consume

    def _block_object(self, block):
        return self._bucket.Object(f'{self._key_prefix}/{block:010d}')

    def _block_bytes(self, block):
        try:
            block_bytes = self._block_object(block).get()["Body"].read()
        except self._bucket.meta.client.exceptions.NoSuchKey as e:
            block_bytes = b''

        return block_bytes

    def xRead(self, amount, offset):
        def _read():
            for block, start, consume in self._blocks(offset, amount):
                block_bytes = self._block_bytes(block)
                yield block_bytes[start:start+consume]

        return b"".join(_read())

    def xFileControl(self, *args):
        return False

    def xCheckReservedLock(self):
        return False

    def xLock(self, level):
        pass

    def xUnlock(self, level):
        pass

    def xClose(self):
        pass

    def xFileSize(self):
        return sum(o.size for o in self._bucket.objects.filter(Prefix=f"{self._key_prefix}/"))

    def xSync(self, flags):
        return True

    def xTruncate(self, newsize):
        total = 0

        for obj in self._bucket.objects.filter(Prefix=f"{self._key_prefix}/"):
            total += obj.size
            to_keep = max(obj.size - total + newsize, 0)

            if to_keep == 0:
                obj.delete()
            elif to_keep < obj.size:
                obj.put(Body=obj.get()['Body'].read()[:to_keep])

        return True

    def xWrite(self, data, offset):
        lock_page_offset = 1073741824
        page_size = len(data)

        if offset == lock_page_offset + page_size:
            # Ensure the previous blocks have enough bytes for size calculations and serialization.
            # SQLite seems to always write pages sequentially, except that it skips the byte-lock
            # page, so we only check previous blocks if we know we're just after the byte-lock
            # page.
            data_first_block = offset // self._block_size
            lock_page_block = lock_page_offset // self._block_size
            for block in range(data_first_block - 1, lock_page_block - 1, -1):
                original_block_bytes = self._block_bytes(block)
                if len(original_block_bytes) == self._block_size:
                    break
                self._block_object(block).put(Body=original_block_bytes + bytes(
                    self._block_size - len(original_block_bytes)
                ))

        data_offset = 0
        for block, start, write in self._blocks(offset, len(data)):

            data_to_write = data[data_offset:data_offset+write]

            if start != 0 or len(data_to_write) != self._block_size:
                original_block_bytes = self._block_bytes(block)
                original_block_bytes = original_block_bytes + bytes(max(start - len(original_block_bytes), 0))
                data_to_write = (original_block_bytes[:start] + data_to_write) + original_block_bytes[start + write :]

            data_offset += write
            self._block_object(block).put(Body=data_to_write)
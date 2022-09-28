"""
Wrapper for accessing sqlite3 database in aws s3.
"""
import apsw
from .s3vfs import S3VFS


class S3Sqlite3:
    """
    Accessor for sqlite3 database in aws s3.
    Usage:
    >> from s3_sqlite3 import S3Sqlite3
    >> with S3Sqlite3(bucket_name="mock_bucket_name", db_name="mock_db_name") as db:
    >>  cursor = db.cursort()
    >>  cursor.execute("CREATE TABLE mock-table(x, y)")
    >>  cursor.execute("INSERT INTO foo VALUES(1,2)")
    >>  cusort.execute("SELECT * FROM foo")
    """
    def __init__(
        self,
        bucket_name: str,
        db_name: str,
    ):
        """
        Constructor for S3Sqlite3 wrapper.
        :param bucket_name: name of the bucket.
        :param db_name: name of the sqlite3 file database.
        """
        self._s3vfs: S3VFS = S3VFS(bucket=bucket_name)
        self._db_key: str = db_name
        self._conn: apsw.Connection = apsw.Connection(
            filename=db_name,
            vfs=self._s3vfs.name,
        )

    def __enter__(self) -> apsw.Connection:
        """
        Get hold of apsw connection object which is wrapper to the slqite pointer.
        """
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close database connection if it is still alive.
        :return:
        """
        if self._conn:
            self._conn.close()
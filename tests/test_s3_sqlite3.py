import pytest
import boto3

from moto import mock_s3

from .generate_dummy_data import generate_dummy_data

from s3_sqlite3 import S3Sqlite3


@pytest.fixture(autouse=True)
def moto_boto():
    s3 = mock_s3()
    s3.start()
    
    resource = boto3.resource("s3", region_name="us-east-1")
    resource.create_bucket(Bucket="test")

    yield
    
    s3.stop()

@pytest.fixture
def schema():
    return {
        "uid": "uuid",
        "id": "int",
        "hash": "md5",
        "modified_at": "datetime",
    }

def prepare_db(schema, data):
    records = [tuple(item.values()) for item in data]
    with S3Sqlite3(bucket_name="test", db_name="test.db") as db:
        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS temporal (uid CHAR, id INT, hash CHAR, modified_at CHAR);")
        cursor.execute("BEGIN TRANSACTION;")
        cursor.executemany("INSERT INTO temporal VALUES(?, ?, ?, ?);", records)
        cursor.execute("COMMIT;")

def test_insert(schema):
    data = generate_dummy_data(schema, 10)
    assert len(data) == 10

    prepare_db(schema, data)

    with S3Sqlite3(bucket_name="test", db_name="test.db") as db:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM temporal;")
        rows = cursor.fetchall()
        for idx, row in enumerate(rows):
            uid, id, hash, modified_at = row
            assert uid == data[idx]["uid"]
            assert id == data[idx]["id"]
            assert hash == data[idx]["hash"]
            assert modified_at == data[idx]["modified_at"]
        assert len(rows) == 10


def test_update(schema):
    data = generate_dummy_data(schema, 1000)
    prepare_db(schema, data)

    record_to_be_udpated = data[3]
    new_id = record_to_be_udpated["id"] + 1000

    with S3Sqlite3(bucket_name="test", db_name="test.db") as db:
        cursor = db.cursor()
        cursor.execute("UPDATE temporal SET ID=? where UID=?;", (new_id, record_to_be_udpated["uid"]))
        cursor.execute("SELECT ID FROM temporal WHERE UID=?;", (record_to_be_udpated["uid"],))
        current_id = cursor.fetchone()[0]
        assert current_id == new_id


def test_delete(schema):
    data = generate_dummy_data(schema, 100)
    prepare_db(schema, data)

    record_to_be_deleted = data[33]

    with S3Sqlite3(bucket_name="test", db_name="test.db") as db:
        cursor = db.cursor()
        cursor.execute("DELETE FROM temporal WHERE UID=?;", (record_to_be_deleted["uid"], ))

        cursor.execute("SELECT * FROM temporal;")
        rows = cursor.fetchall()
        assert len(rows) == 99

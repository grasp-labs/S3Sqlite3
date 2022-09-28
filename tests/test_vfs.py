import boto3
from moto import mock_s3
from s3_sqlite3 import S3Sqlite3


@mock_s3
def test_create_sqlite3_db():
    bucket_name ="mock_bucket"
    db_name= "mock.db"

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=bucket_name)

    with S3Sqlite3(bucket_name=bucket_name, db_name=db_name) as db:
        cursor = db.cursor()
        cursor.execute("CREATE TABLE foo(x,y)")
        cursor.execute("INSERT INTO foo values(1, 2)")

    #object = conn.Object(bucket_name, db_name).load()
    #assert object.exists()
    #client = boto3.client("s3", region_name="us-east-1")
    #client.head_object(Bucket=bucket_name, Key="mock.db/")
    bucket = conn.Bucket(bucket_name)
    for obj in bucket.objects.all():
        assert obj._key.startswith(db_name)

import uuid

import pytest
import s3fs


@pytest.fixture(scope="session")
def s3_client():
    fs = s3fs.S3FileSystem(
        key="minioadmin",
        secret="minioadmin",
        client_kwargs={"endpoint_url": "http://localhost:9000"},
    )
    yield fs


@pytest.fixture(scope="function")
def temp_bucket(s3_client):
    bucket_name: str = f"test-bucket-{uuid.uuid4().hex}"

    s3_client.mkdir(bucket_name)

    yield bucket_name, s3_client

    try:
        if s3_client.exists(bucket_name):
            files = s3_client.ls(bucket_name)
            for f in files:
                s3_client.rm(f)
            s3_client.rmdir(bucket_name)
    except Exception as e:
        print(f"Error cleaning up bucket {bucket_name}: {e}")


def test_connection(temp_bucket):
    bucket_name, fs = temp_bucket

    msg = "Hello from Python!"

    with fs.open(f"{bucket_name}/hello.txt", "w") as f:
        f.write(msg)

    with fs.open(f"{bucket_name}/hello.txt", "r") as f:
        assert f.read() == msg

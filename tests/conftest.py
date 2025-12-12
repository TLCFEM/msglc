import uuid

import pytest
import s3fs


@pytest.fixture(scope="function")
def json_base():
    return {
        "title": "example glossary",
        "GlossDiv": {
            "title": "S",
            "GlossList": {
                "GlossEntry": {
                    "ID": "SGML",
                    "SortAs": "SGML",
                    "GlossTerm": "Standard Generalized Markup Language",
                    "Acronym": "SGML",
                    "Abbrev": "ISO 8879:1986",
                    "GlossDef": {
                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
                        "GlossSeeAlso": ["GML", "XML"],
                    },
                    "GlossSee": "markup",
                }
            },
        },
        "empty_list": [],
        "none_list": [None],
    }


@pytest.fixture(scope="function")
def json_before(json_base):
    return {
        "glossary": json_base,
        "some_tuple": (1, 2, 3),
        "some_set": {1, 2, 3},
    }


@pytest.fixture(scope="function")
def json_after(json_base):
    return {
        "glossary": json_base,
        "some_tuple": [1, 2, 3],
        "some_set": [1, 2, 3],
    }


@pytest.fixture(scope="session")
def s3_client():
    fs = s3fs.S3FileSystem(
        key="rustfsadmin",
        secret="rustfsadmin",
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

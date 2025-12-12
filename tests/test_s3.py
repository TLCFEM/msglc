import uuid

import pytest
from s3fs import S3FileSystem

from msglc import LazyWriter
from msglc.reader import LazyReader, LazyStats


def test_connection(temp_bucket):
    fs: S3FileSystem
    bucket_name, fs = temp_bucket

    msg = "Hello from Python!"

    with fs.open(f"{bucket_name}/hello.txt", "w") as f:
        f.write(msg)

    with fs.open(f"{bucket_name}/hello.txt", "r") as f:
        assert f.read() == msg


def test_s3_write_read(temp_bucket, json_before, json_after):
    fs: S3FileSystem
    bucket_name, fs = temp_bucket

    target: str = f"{bucket_name}/{str(uuid.uuid4())}"

    with LazyWriter(target, s3fs=fs) as writer:
        writer.write(json_before)
        with pytest.raises(ValueError):
            writer.write(json_before)

    stats = LazyStats()

    with LazyReader(target, counter=stats, s3fs=fs) as reader:
        assert (
            reader.read(
                "glossary/GlossDiv/GlossList/GlossEntry/GlossDef/GlossSeeAlso/1"
            )
            == "XML"
        )
        assert reader.read("glossary/empty_list") == []
        assert reader.read("glossary/none_list/0") is None
        assert reader.read() == json_after
        assert reader == json_after

        dict_container = reader.read("glossary/GlossDiv")
        assert len(dict_container) == 2
        assert dict_container.get("invalid_key") is None
        assert "invalid_key" not in dict_container
        assert set(dict_container.keys()) == {"title", "GlossList"}
        for x, _ in dict_container.items():
            assert x in ["title", "GlossList"]

        list_container = reader.read(
            "glossary/GlossDiv/GlossList/GlossEntry/GlossDef/GlossSeeAlso"
        )
        assert len(list_container) == 2
        for x in list_container:
            assert x in ["GML", "XML"]
        assert set(list_container) == {"GML", "XML"}

    str(stats)

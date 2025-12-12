import uuid
from io import BytesIO

import pytest
from s3fs import S3FileSystem

from msglc import FileInfo, LazyWriter, append, combine
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


def test_s3_combine_append(tmpdir, temp_bucket, json_after):
    fs: S3FileSystem
    bucket_name, fs = temp_bucket

    target: str = f"{bucket_name}/{str(uuid.uuid4())}"

    with tmpdir.as_cwd():
        with LazyWriter("test_list.msg") as writer:
            writer.write([x for x in range(30)])
        with LazyWriter("test_dict.msg") as writer:
            writer.write(json_after)

        combine(target, [FileInfo("test_dict.msg")], s3fs=fs)
        combine(target, [FileInfo("test_list.msg")], mode="a", s3fs=fs)

        with LazyReader(target, s3fs=fs) as reader:
            assert reader.read("0/glossary/title") == "example glossary"
            assert reader.read("1/2") == 2
            assert reader.read("1/-1") == 29
            assert reader.read("1/0:2") == [0, 1]
            assert reader.read("1/:2") == [0, 1]
            assert reader.read("1/28:") == [28, 29]
            assert reader.read("1/24:2:30") == [24, 26, 28]
            assert reader.read("1/:2:5") == [0, 2, 4]
            assert reader.read("1/24:2:") == [24, 26, 28]
            assert reader.visit("0/glossary/title") == "example glossary"
            assert reader.visit("1/2") == 2
            assert reader.visit("1/-1") == 29
            assert reader.visit("1/0:2") == [0, 1]
            assert reader.visit("1/:2") == [0, 1]
            assert reader.visit("1/28:") == [28, 29]
            assert reader.visit("1/24:2:30") == [24, 26, 28]
            assert reader.visit("1/:2:5") == [0, 2, 4]
            assert reader.visit("1/24:2:") == [24, 26, 28]
            with LazyReader("test_dict.msg") as inner_reader:
                assert reader[0] == inner_reader
            with LazyReader("test_list.msg") as inner_reader:
                assert reader[1] == inner_reader

        with pytest.raises(ValueError):
            append(target, FileInfo("test_list.msg", "no_name"), s3fs=fs)

        with pytest.raises(ValueError):
            combine(target, FileInfo("test_list.msg", "no_name"), s3fs=fs)
            append(target, FileInfo("test_list.msg", "no_name"), s3fs=fs)

        with pytest.raises(ValueError):
            combine(target, FileInfo("test_list.msg", "no_name"), s3fs=fs)
            append(target, FileInfo("test_list.msg"), s3fs=fs)

        with pytest.raises(ValueError):
            combine(target, FileInfo(BytesIO(b"0" * 100), "no_name"), s3fs=fs)

        with pytest.raises(ValueError):
            with open("trivial.msg", "wb") as trivial:
                trivial.write(b"0" * 300)
            combine(target, FileInfo("trivial.msg", "no_name"), s3fs=fs)

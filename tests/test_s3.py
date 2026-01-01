#  Copyright (C) 2024-2026 Theodore Chang
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

import uuid
from io import BytesIO

import pytest
from fsspec.implementations.arrow import ArrowFSWrapper
from upath import UPath

from msglc import FileInfo, LazyWriter, append, combine
from msglc.reader import LazyReader, LazyStats


@pytest.fixture(scope="session", params=["s3fs", "pyarrow"])
def s3_client(request):
    if request.param == "s3fs":
        from s3fs import S3FileSystem

        yield S3FileSystem(
            key="rustfsadmin",
            secret="rustfsadmin",
            client_kwargs={"endpoint_url": "http://localhost:9000"},
        )

    elif request.param == "pyarrow":
        from pyarrow.fs import S3FileSystem

        yield ArrowFSWrapper(
            S3FileSystem(
                access_key="rustfsadmin",
                secret_key="rustfsadmin",
                endpoint_override="localhost:9000",
                scheme="http",
                allow_bucket_creation=True,
                allow_bucket_deletion=True,
            )
        )


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


@pytest.mark.parametrize("is_upath", [True, False])
@pytest.mark.parametrize("in_memory", [True, False])
def test_s3_write_read(temp_bucket, json_before, json_after, is_upath, in_memory):
    bucket_name, fs = temp_bucket

    target: str | UPath = f"{bucket_name}/{str(uuid.uuid4())}"
    if in_memory:
        target = UPath(f"memory://{target}")

    with LazyWriter(target, fs=fs) as writer:
        writer.write(json_before)
        with pytest.raises(ValueError):
            writer.write(json_before)

    stats = LazyStats()

    if is_upath and isinstance(target, str):
        target = UPath(
            f"s3://{target}",
            endpoint_url="http://localhost:9000",
            key="rustfsadmin",
            secret="rustfsadmin",
        )

    with LazyReader(target, counter=stats, fs=fs) as reader:
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


@pytest.mark.parametrize("remote", [True, False])
def test_s3_combine_append(tmpdir, temp_bucket, json_after, remote):
    bucket_name, fs = temp_bucket

    target: str = f"{bucket_name}/{str(uuid.uuid4())}"

    with tmpdir.as_cwd():
        list_name = "test_list.msg"
        dict_name = "test_dict.msg"
        if remote:
            list_name = f"{bucket_name}/{list_name}"
            dict_name = f"{bucket_name}/{dict_name}"
        s3fs = fs if remote else None

        with LazyWriter(list_name, fs=s3fs) as writer:
            writer.write([x for x in range(30)])
        with LazyWriter(dict_name, fs=s3fs) as writer:
            writer.write(json_after)

        combine(target, [FileInfo(dict_name, fs=s3fs)], fs=fs)
        combine(target, [FileInfo(list_name, fs=s3fs)], mode="a", fs=fs)

        with LazyReader(target, fs=fs) as reader:
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
            with LazyReader(dict_name, fs=s3fs) as inner_reader:
                assert reader[0] == inner_reader
            with LazyReader(list_name, fs=s3fs) as inner_reader:
                assert reader[1] == inner_reader

        with pytest.raises(ValueError):
            append(target, FileInfo(list_name, "no_name", fs=s3fs), fs=fs)

        with pytest.raises(ValueError):
            combine(target, FileInfo(list_name, "no_name", fs=s3fs), fs=fs)
            append(target, FileInfo(list_name, "no_name", fs=s3fs), fs=fs)

        with pytest.raises(ValueError):
            combine(target, FileInfo(list_name, "no_name", fs=s3fs), fs=fs)
            append(target, FileInfo(list_name, fs=s3fs), fs=fs)

        with pytest.raises(ValueError):
            combine(target, FileInfo(BytesIO(b"0" * 100), "no_name"), fs=fs)

        with pytest.raises(ValueError):
            with open("trivial.msg", "wb") as trivial:
                trivial.write(b"0" * 300)
            combine(target, FileInfo("trivial.msg", "no_name"), fs=fs)

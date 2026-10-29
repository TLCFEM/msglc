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

from __future__ import annotations

from importlib import import_module
from io import BytesIO
from uuid import uuid4

import pytest
from fsspec.implementations.memory import MemoryFileSystem
from upath import UPath

from msglc import FileInfo, LazyWriter, append, combine, dump
from msglc.codec import (
    CBORCodec,
    LazyCodec,
    MsgpackCodec,
    MsgspecCodec,
    acquire_codec,
)
from msglc.config import config, configure, decrement_gc_counter
from msglc.reader import (
    LazyDict,
    LazyItem,
    LazyList,
    LazyReader,
    LazyStats,
    async_to_obj,
    to_obj,
)
from msglc.utility import MockIO
from msglc.writer import LazyCombiner, _copy_obj

# ---------------------------------------------------------------------------
# codecs
# ---------------------------------------------------------------------------


def test_codec_string_and_acquire():
    assert str(MsgpackCodec()) == "MsgpackCodec"

    instance = MsgspecCodec()
    assert acquire_codec(instance) is instance
    assert isinstance(acquire_codec(MsgpackCodec), MsgpackCodec)
    assert isinstance(acquire_codec(None), LazyCodec)

    with pytest.raises(TypeError):
        acquire_codec(object())


@pytest.mark.parametrize(
    ("size", "length"),
    [
        (0, 1),
        (23, 1),
        (24, 2),
        (0xFF, 2),
        (0x100, 3),
        (0xFFFF, 3),
        (0x10000, 5),
        (0xFFFFFFFF, 5),
        (0x100000000, 9),
        (0xFFFFFFFFFFFFFFFF, 9),
    ],
)
def test_cbor_header_sizes(size, length):
    codec = CBORCodec()
    assert len(codec.write_array_header(size)) == length
    assert len(codec.write_map_header(size)) == length


def test_cbor_header_overflow():
    codec = CBORCodec()
    with pytest.raises(ValueError, match="Array is too large"):
        codec.write_array_header(2**64)
    with pytest.raises(ValueError, match="Dict is too large"):
        codec.write_map_header(2**64)


@pytest.mark.parametrize(
    ("size", "length"),
    [
        (0, 1),
        (15, 1),
        (16, 3),
        (0xFFFF, 3),
        (0x10000, 5),
        (0xFFFFFFFF, 5),
    ],
)
def test_msgpack_header_sizes(size, length):
    codec = MsgpackCodec()
    assert len(codec.write_array_header(size)) == length
    assert len(codec.write_map_header(size)) == length


def test_msgpack_header_overflow():
    codec = MsgpackCodec()
    with pytest.raises(ValueError, match="Array is too large"):
        codec.write_array_header(2**32)
    with pytest.raises(ValueError, match="Dict is too large"):
        codec.write_map_header(2**32)


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


def test_configure_optional_flags_and_threshold_coupling():
    fields = (
        "small_obj_optimization_threshold",
        "trivial_size",
        "numpy_encoder",
        "numpy_fast_int_pack",
        "enable_custom_container",
        "has_set",
        "has_tuple",
        "has_numpy",
        "fs",
        "compatibility_check",
    )
    original = {field: getattr(config, field) for field in fields}

    try:
        # A trivial size below the small object threshold lowers the latter.
        configure(trivial_size=10, small_obj_optimization_threshold=5)
        assert config.trivial_size == 10
        assert config.small_obj_optimization_threshold == 10

        # A trivial size above the small object threshold lowers the former.
        configure(trivial_size=200, small_obj_optimization_threshold=100)
        assert config.small_obj_optimization_threshold == 200
        assert config.trivial_size == 200

        configure(
            numpy_encoder=True,
            numpy_fast_int_pack=True,
            enable_custom_container=False,
            has_set=False,
            has_tuple=False,
            has_numpy=False,
        )
        assert config.numpy_encoder is True
        assert config.numpy_fast_int_pack is True
        assert config.enable_custom_container is False
        assert config.has_set is False
        assert config.has_tuple is False
        assert config.has_numpy is False

        configure(compatibility_check=lambda magic: magic == b"custom")
        assert config.check_compatibility(b"custom") is True
        assert config.check_compatibility(b"other") is False
    finally:
        for field, value in original.items():
            setattr(config, field, value)


def test_gc_counter_underflow(monkeypatch):
    config_module = import_module("msglc.config")

    monkeypatch.setattr(config, "disable_gc", True)
    monkeypatch.setattr(config_module, "_gc_counter", 0)

    with pytest.raises(RuntimeError, match="GC counter underflow"):
        decrement_gc_counter()


# ---------------------------------------------------------------------------
# utility
# ---------------------------------------------------------------------------


def test_mock_io_read_speed_table():
    buffer = MockIO(
        BytesIO(b"payload"), "rb", read_speed=[(4, 100), (8, 200), (16, 300)]
    )

    assert buffer._actual_speed(1) == 100
    assert buffer._actual_speed(4) == 200
    assert buffer._actual_speed(10) == 300
    assert buffer._actual_speed(100) == 300

    assert buffer.tell() == 0
    assert buffer.read(1) == b"p"


# ---------------------------------------------------------------------------
# package level helpers
# ---------------------------------------------------------------------------


def test_dump_rejects_invalid_backend(tmp_path):
    with pytest.raises(ValueError, match="Invalid backend"):
        dump(str(tmp_path / "data.msg"), {}, backend="invalid")


def test_combine_rejects_invalid_backend_and_mode():
    with pytest.raises(ValueError, match="Invalid backend"):
        combine(BytesIO(), [], backend="invalid")

    with pytest.raises(ValueError, match="Invalid mode"):
        combine(BytesIO(), [], mode="invalid")


def test_dump_rust_backend_with_upath(tmp_path):
    data = {"numbers": list(range(100)), "text": "x" * 100}
    path = UPath(tmp_path / "data.msg")

    dump(path, data, backend="rust")

    with LazyReader(path) as reader:
        assert reader.to_obj() == data


def test_dump_rust_backend_with_remote_filesystem():
    fs = MemoryFileSystem()
    name = f"rust-{uuid4().hex}.msg"
    data = {"numbers": list(range(100)), "text": "x" * 100}

    dump(name, data, backend="rust", fs=fs)

    assert fs.exists(name)
    with LazyReader(name, fs=fs) as reader:
        assert reader.to_obj() == data


def test_dump_python_backend_with_remote_filesystem():
    fs = MemoryFileSystem()
    name = f"python-{uuid4().hex}.msg"
    data = [{"index": index} for index in range(50)]

    dump(name, data, fs=fs)

    assert fs.exists(name)
    with LazyReader(name, fs=fs) as reader:
        assert reader.to_obj() == data


def test_file_info_without_path():
    info = FileInfo(None)

    assert info._exists() is True
    with pytest.raises(RuntimeError):
        info._open()


# ---------------------------------------------------------------------------
# writer / combiner
# ---------------------------------------------------------------------------


def test_copy_obj_can_rewind_destination():
    destination = BytesIO()

    _copy_obj(BytesIO(b"content"), destination, seek=True)

    assert destination.tell() == 0
    assert destination.read() == b"content"


def test_writer_and_combiner_reject_invalid_targets():
    with pytest.raises(ValueError, match="Expecting a buffer or path"):
        LazyWriter(123).__enter__()

    with pytest.raises(ValueError, match="Invalid mode"):
        LazyCombiner(BytesIO(), mode="invalid")

    with pytest.raises(ValueError, match="Expecting a buffer or path"):
        LazyCombiner(123).__enter__()


def test_combine_append_with_remote_filesystem():
    fs = MemoryFileSystem()
    first_name = f"first-{uuid4().hex}.msg"
    second_name = f"second-{uuid4().hex}.msg"
    archive_name = f"archive-{uuid4().hex}.msg"

    first = [1, 2, 3]
    second = {"key": "value"}
    dump(first_name, first, fs=fs)
    dump(second_name, second, fs=fs)

    combine(archive_name, [FileInfo(first_name, fs=fs)], fs=fs)
    append(archive_name, [FileInfo(second_name, fs=fs)], fs=fs)

    with LazyReader(archive_name, fs=fs) as reader:
        assert reader[0].to_obj() == first
        assert reader[1].to_obj() == second


class _NonSeekable:
    def __init__(self):
        self.closed = False

    def seekable(self):
        return False

    def close(self):
        self.closed = True


def test_writer_handles_unseekable_upath(monkeypatch):
    original_open = UPath.open

    def fake_open(self, mode="r", *args, **kwargs):
        if mode in ("wb", "r+b"):
            return _NonSeekable()
        return original_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(UPath, "open", fake_open)

    path = UPath(f"memory://{uuid4().hex}.msg")
    data = {"key": "value", "numbers": list(range(20))}

    dump(path, data)

    monkeypatch.setattr(UPath, "open", original_open)
    with LazyReader(path) as reader:
        assert reader.to_obj() == data


def test_combiner_handles_unseekable_upath(monkeypatch):
    original_open = UPath.open
    first = UPath(f"memory://first-{uuid4().hex}.msg")
    second = UPath(f"memory://second-{uuid4().hex}.msg")
    archive = UPath(f"memory://archive-{uuid4().hex}.msg")

    first_data = [1, 2, 3]
    second_data = {"key": "value"}
    dump(first, first_data)
    dump(second, second_data)

    combine(archive, FileInfo(first))

    def fake_open(self, mode="r", *args, **kwargs):
        if mode in ("wb", "r+b"):
            return _NonSeekable()
        return original_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(UPath, "open", fake_open)

    append(archive, FileInfo(second))

    monkeypatch.setattr(UPath, "open", original_open)
    with LazyReader(archive) as reader:
        assert reader[0].to_obj() == first_data
        assert reader[1].to_obj() == second_data


def test_combiner_handles_upath_without_random_write(monkeypatch):
    original_open = UPath.open
    source = UPath(f"memory://source-{uuid4().hex}.msg")
    archive = UPath(f"memory://archive-{uuid4().hex}.msg")

    data = {"key": "value", "numbers": list(range(20))}
    dump(source, data)

    def fake_open(self, mode="r", *args, **kwargs):
        if mode in ("wb", "r+b"):
            raise NotImplementedError
        return original_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(UPath, "open", fake_open)

    combine(archive, FileInfo(source))

    monkeypatch.setattr(UPath, "open", original_open)
    with LazyReader(archive) as reader:
        assert reader[0].to_obj() == data


def _append_archive(toc) -> BytesIO:
    codec = MsgspecCodec()
    packed_toc = codec.encode({"t": toc})

    stream = BytesIO()
    stream.write(LazyWriter.magic)
    stream.write(codec.encode(1).rjust(10, b"\0"))
    stream.write(codec.encode(len(packed_toc)).rjust(10, b"\0"))
    stream.write(b"\0")  # one byte of data, the TOC starts at offset 1
    stream.write(packed_toc)
    stream.seek(0)
    return stream


def test_combiner_rejects_invalid_append_archives():
    with pytest.raises(ValueError, match="Invalid file format"):
        LazyCombiner(BytesIO(b"not an archive" * 10), mode="a").__enter__()

    with pytest.raises(ValueError, match="not a valid combined file"):
        LazyCombiner(_append_archive(["not-an-int"]), mode="a").__enter__()

    with pytest.raises(ValueError, match="not a valid combined file"):
        LazyCombiner(_append_archive({"name": "not-an-int"}), mode="a").__enter__()

    with pytest.raises(ValueError, match="not a valid combined file"):
        LazyCombiner(_append_archive("invalid"), mode="a").__enter__()


# ---------------------------------------------------------------------------
# reader
# ---------------------------------------------------------------------------


def test_lazy_stats_call_and_clear():
    stats = LazyStats()

    stats += 10
    stats += 5

    assert stats() == 15
    assert stats.bytes_per_call() == 7.5
    assert "2 calls" in str(stats)

    stats.clear()

    assert stats() == 0
    assert stats.bytes_per_call() == 0


def test_to_obj_passes_plain_values_through():
    value = {"plain": "value"}
    assert to_obj(value) is value


@pytest.mark.asyncio
async def test_async_to_obj_passes_plain_values_through():
    value = {"plain": "value"}
    assert await async_to_obj(value) is value


def test_lazy_item_base_class():
    item = LazyItem(BytesIO(), 0)

    with pytest.raises(NotImplementedError):
        len(item)
    with pytest.raises(NotImplementedError):
        item.to_obj()
    with pytest.raises(NotImplementedError):
        item[0]

    assert item.unwrap() is item

    class InvalidLengthItem(LazyItem):
        def __len__(self):
            return "invalid"

    # A broken ``__len__`` must disable fast loading rather than propagate.
    assert InvalidLengthItem(BytesIO(), 0)._fast_loading is False


def test_lazy_reader_misc_api(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 64)
    monkeypatch.setattr(config, "trivial_size", 10)

    data = {
        "title": "example",
        "items": [{"id": index, "payload": "x" * 100} for index in range(20)],
        "numbers": list(range(20)),
    }
    path = tmp_path / "data.msg"
    dump(str(path), data)

    with LazyReader(str(path)) as reader:
        assert repr(reader).startswith("LazyReader (")
        assert len(reader) == 3

        assert reader.read(["items", 0, "id"]) == 0
        assert reader.read("items/0/id") == 0

        dict_container = reader.read("items/0")
        assert isinstance(dict_container, LazyDict)
        assert list(dict_container.values()) == [0, "x" * 100]

        assert "example" in str(reader)
        assert reader.read() == data
        assert reader.raw_data(chunked=False) == path.read_bytes()

        with pytest.raises(ValueError, match="Invalid"):
            reader._child({"t": "invalid"})


def test_lazy_reader_accepts_non_string_paths(tmp_path):
    data = [{"value": index} for index in range(5)]
    path = tmp_path / "list.msg"
    dump(str(path), data)

    with LazyReader(str(path)) as reader:
        assert reader.read([0, "value"]) == 0
        assert reader.read(1) == {"value": 1}
        assert reader.read(slice(1, 3)) == [{"value": 1}, {"value": 2}]


def test_lazy_reader_rejects_invalid_inputs(tmp_path):
    with pytest.raises(ValueError, match="Expecting a buffer or path"):
        LazyReader(123)

    path = tmp_path / "invalid.msg"
    path.write_bytes(b"not a msglc archive" * 10)

    with pytest.raises(ValueError, match="Invalid file format"):
        LazyReader(str(path))


def test_lazy_reader_reports_closed_buffer(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 1)
    monkeypatch.setattr(config, "trivial_size", 1)

    path = tmp_path / "list.msg"
    dump(str(path), [1, 2, 3])

    with LazyReader(str(path), cached=False) as reader:
        pass

    with pytest.raises(ValueError, match="closed"):
        reader.read(0)
    with pytest.raises(ValueError, match="closed"):
        b"".join(reader.raw_data())


def test_raw_data_stops_at_truncated_end(tmp_path):
    path = tmp_path / "truncated.msg"
    dump(str(path), {"values": list(range(100)), "text": "x" * 1000})

    with LazyReader(str(path)) as reader:
        original_size = path.stat().st_size
        path.write_bytes(path.read_bytes()[:-50])

        raw = b"".join(reader.raw_data())

    assert len(raw) == original_size - 50


def test_lazy_list_fast_to_obj(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 64)
    monkeypatch.setattr(config, "trivial_size", 10)

    data = [{"text": "x" * 100} for _ in range(10)]
    path = tmp_path / "list.msg"
    dump(str(path), data)

    with LazyReader(str(path)) as reader:
        container = reader.unwrap()
        assert isinstance(container, LazyList)
        assert container.to_obj() == data


def test_lazy_list_to_obj_without_position_data():
    buffer = BytesIO(b"\x01")
    container = LazyList({"t": [{"p": [0, 1]}]}, buffer, 0)

    assert container.to_obj() == [1]


@pytest.mark.asyncio
async def test_async_visit_with_nested_path(tmp_path):
    data = [{"id": index} for index in range(5)]
    path = tmp_path / "data.msg"
    dump(str(path), data)

    with LazyReader(str(path)) as reader:
        assert await reader.async_visit("3/id") == 3
        assert await reader.async_read([3, "id"]) == 3
        assert await reader.async_read(3) == {"id": 3}
        assert await reader.async_read(slice(1, 3)) == [{"id": 1}, {"id": 2}]


def test_numpy_uniform_array_is_grouped(tmp_path, monkeypatch):
    numpy = pytest.importorskip("numpy")
    monkeypatch.setattr(config, "numpy_encoder", False)
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 64)
    monkeypatch.setattr(config, "trivial_size", 10)

    array = numpy.arange(1000, dtype=numpy.float64)
    path = tmp_path / "numpy-uniform.msg"
    dump(str(path), {"array": array})

    with LazyReader(str(path)) as reader:
        assert reader.to_obj() == {"array": array.tolist()}

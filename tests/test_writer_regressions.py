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

from contextlib import closing
from io import BytesIO
from tempfile import TemporaryFile

import pytest

from msglc import FileInfo, combine, dump
from msglc.config import config
from msglc.reader import LazyReader
from msglc.writer import LazyCombiner, LazyWriter

BUFFER_FACTORIES = [
    pytest.param(
        lambda tmp_path: open(tmp_path / "buffered-random.msg", "w+b"),  # noqa: SIM115
        id="buffered-random",
    ),
    pytest.param(lambda tmp_path: TemporaryFile(), id="temporary-file"),  # noqa: SIM115
    pytest.param(lambda tmp_path: BytesIO(), id="bytes-io"),
]


@pytest.mark.parametrize("make_buffer", BUFFER_FACTORIES)
def test_lazy_writer_accepts_generic_iobase(tmp_path, make_buffer):
    """Any seekable ``IOBase`` should be usable as a writer target."""
    data = {"numbers": list(range(100)), "nested": {"value": b"payload"}}

    with closing(make_buffer(tmp_path)) as buffer:
        with LazyWriter(buffer) as writer:
            writer.write(data)

        buffer.flush()
        buffer.seek(0)

        with LazyReader(buffer) as reader:
            assert reader.to_obj() == data


@pytest.mark.parametrize("make_buffer", BUFFER_FACTORIES)
def test_dump_accepts_generic_iobase(tmp_path, make_buffer):
    data = [{"index": index, "value": index * 2} for index in range(50)]

    with closing(make_buffer(tmp_path)) as buffer:
        dump(buffer, data)

        buffer.flush()
        buffer.seek(0)

        with LazyReader(buffer) as reader:
            assert reader.to_obj() == data


@pytest.mark.parametrize("make_buffer", BUFFER_FACTORIES)
def test_lazy_combiner_accepts_generic_iobase(tmp_path, make_buffer):
    first = tmp_path / "first.msg"
    second = tmp_path / "second.msg"
    first_data = list(range(100))
    second_data = {"key": "value"}
    dump(str(first), first_data)
    dump(str(second), second_data)

    files = [FileInfo(str(first)), FileInfo(str(second))]

    with closing(make_buffer(tmp_path)) as buffer:
        with LazyCombiner(buffer) as combiner:
            for file in files:
                combiner.write(file.chunking("python", None), file.name)

        buffer.flush()
        buffer.seek(0)

        with LazyReader(buffer) as reader:
            assert reader[0].to_obj() == first_data
            assert reader[1].to_obj() == second_data


@pytest.mark.parametrize("make_buffer", BUFFER_FACTORIES)
def test_combine_and_append_with_generic_iobase(tmp_path, make_buffer):
    first = tmp_path / "first.msg"
    second = tmp_path / "second.msg"
    first_data = list(range(100))
    second_data = {"key": "value"}
    dump(str(first), first_data)
    dump(str(second), second_data)

    with closing(make_buffer(tmp_path)) as buffer:
        combine(buffer, FileInfo(str(first)))
        buffer.flush()
        combine(buffer, FileInfo(str(second)), mode="a")
        buffer.flush()
        buffer.seek(0)

        with LazyReader(buffer) as reader:
            assert reader[0].to_obj() == first_data
            assert reader[1].to_obj() == second_data


@pytest.mark.parametrize(
    "make_array",
    [
        pytest.param(lambda numpy: numpy.arange(10000), id="mixed-int-widths"),
        pytest.param(
            lambda numpy: numpy.array([0, 1000, 100000] * 1000), id="non-uniform"
        ),
        pytest.param(
            lambda numpy: numpy.full(10000, 7, dtype=numpy.int64), id="uniform"
        ),
    ],
)
def test_numpy_fast_int_pack_fallback_roundtrip(tmp_path, monkeypatch, make_array):
    """Non-uniform integers must fall back to the regular encoder correctly."""
    numpy = pytest.importorskip("numpy")
    monkeypatch.setattr(config, "numpy_encoder", False)
    monkeypatch.setattr(config, "numpy_fast_int_pack", True)

    array = make_array(numpy)
    path = tmp_path / "numpy-fast-int.msg"
    dump(str(path), {"array": array})

    with LazyReader(str(path)) as reader:
        assert reader.to_obj()["array"] == array.tolist()


def test_numpy_fast_int_pack_after_previous_value(tmp_path, monkeypatch):
    """The fallback must rewind relative to the start of the data section."""
    numpy = pytest.importorskip("numpy")
    monkeypatch.setattr(config, "numpy_encoder", False)
    monkeypatch.setattr(config, "numpy_fast_int_pack", True)

    data = {
        "before": "some value",
        "array": numpy.arange(10000),
        "after": list(range(100)),
    }
    path = tmp_path / "numpy-fast-int-offset.msg"
    dump(str(path), data)

    with LazyReader(str(path)) as reader:
        assert reader["before"] == data["before"]
        restored = reader.to_obj()
        assert restored["array"] == data["array"].tolist()
        assert restored["after"] == data["after"]
        assert restored == {
            "before": "some value",
            "array": data["array"].tolist(),
            "after": data["after"],
        }

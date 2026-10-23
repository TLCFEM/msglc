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

import signal

import pytest

from msglc import dump
from msglc.config import config
from msglc.reader import LazyList, LazyReader, LazyStats


@pytest.mark.parametrize("cached", [True, False])
@pytest.mark.parametrize("threshold", [10, 64])
def test_negative_index_on_blocked_list(tmp_path, monkeypatch, cached, threshold):
    """Negative indices must also work for block-indexed (trivial) lists."""
    monkeypatch.setattr(config, "small_obj_optimization_threshold", threshold)
    monkeypatch.setattr(config, "trivial_size", min(10, threshold))

    data = list(range(200))
    path = tmp_path / "blocked.msg"
    dump(str(path), data)

    with LazyReader(str(path), cached=cached) as reader:
        # A small threshold turns this list into a blocked lazy list.
        assert isinstance(reader.unwrap(), LazyList)

        for index in (-1, -5, -20, -70, -len(data)):
            assert reader[index] == data[index]
            assert reader[str(index)] == data[index]
            assert reader.read(str(index)) == data[index]

        # Reading negative indices must not corrupt the cache / block lookup.
        assert reader[0] == data[0]
        assert reader[100] == data[100]
        assert reader.to_obj() == data


@pytest.mark.skipif(
    not hasattr(signal, "SIGALRM"), reason="requires a POSIX signal implementation"
)
def test_slice_of_empty_list_is_empty(tmp_path):
    """A string slice of an empty list must not loop forever."""

    def _timeout(*_):
        raise TimeoutError("string slice of an empty list did not terminate")

    path = tmp_path / "empty.msg"
    dump(str(path), [])

    old_handler = signal.signal(signal.SIGALRM, _timeout)
    signal.setitimer(signal.ITIMER_REAL, 2)
    try:
        with LazyReader(str(path)) as reader:
            assert reader.read(":") == []
            assert reader.read("0:") == []
            assert reader.read("::-1") == []
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


@pytest.mark.parametrize(
    "payload",
    [
        b"multiarray",
        b"multiarray-not-a-pickle",
        b"multiarray" + b"x" * 100,
        b"\x80multiarray-not-a-pickle",
    ],
)
def test_bytes_that_resemble_numpy_pickles_roundtrip(tmp_path, payload):
    """Arbitrary bytes must not be passed to pickle just because of their content."""
    path = tmp_path / "bytes.msg"
    dump(str(path), payload)

    with LazyReader(str(path)) as reader:
        assert reader.to_obj() == payload


def test_numpy_encoded_array_is_still_unpickled(tmp_path, monkeypatch):
    """The refined bytes check must keep the numpy_encoder feature working."""
    numpy = pytest.importorskip("numpy")
    monkeypatch.setattr(config, "numpy_encoder", True)
    # Force the dictionary to be lazy so the per-child unpickling path is used.
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 32)
    monkeypatch.setattr(config, "trivial_size", 20)

    array = numpy.arange(100, dtype=numpy.int64)
    path = tmp_path / "numpy.msg"
    dump(str(path), {"array": array})

    with LazyReader(str(path)) as reader:
        value = reader["array"]
        assert isinstance(value, numpy.ndarray)
        numpy.testing.assert_array_equal(value, array)


def test_lazy_stats_without_reads():
    stats = LazyStats()

    assert stats.bytes_per_call() == 0
    assert "0 calls" in str(stats)


@pytest.mark.parametrize("cached", [True, False])
def test_nested_iteration_is_independent(tmp_path, monkeypatch, cached):
    """Iterating a lazy list must not disturb another active iteration."""
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 16)
    monkeypatch.setattr(config, "trivial_size", 10)

    data = list(range(200))
    path = tmp_path / "iteration.msg"
    dump(str(path), data)

    with LazyReader(str(path), cached=cached) as reader:
        container = reader.unwrap()
        assert isinstance(container, LazyList)

        outer = []
        for value in container:
            if len(outer) < 3:
                assert list(container) == data
            outer.append(value)

        assert outer == data

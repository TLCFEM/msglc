from __future__ import annotations

import pytest

from msglc import dump
from msglc.config import config, configure
from msglc.reader import LazyReader
from msglc.writer import LazyWriter


@pytest.fixture(autouse=True)
def reset_writer_config():
    original_engine = config.writer_engine
    original_magic = LazyWriter.magic
    try:
        yield
    finally:
        configure(writer_engine=original_engine)
        LazyWriter.magic = original_magic


def _roundtrip(tmpdir, obj, engine: str):
    configure(writer_engine=engine)
    target = tmpdir.join(f"{engine}.msg").strpath
    dump(target, obj)
    with LazyReader(target) as reader:
        return reader.to_obj()


@pytest.mark.parametrize(
    "obj",
    [
        {1: "one", 2: "two", "three": 3, b"bin": b"value", None: "nil"},
        {
            "nested": [{"a": 1}, {"b": 2}],
            "set_like": {3, 1, 2},
            "tuple_like": (1, "x", 3.5),
            "blob": b"\x00\x01\x02",
            "big": 2**62,
        },
    ],
)
def test_writer_engine_parity(tmpdir, obj):
    baseline = _roundtrip(tmpdir, obj, "python")
    native_toc = _roundtrip(tmpdir, obj, "native_toc")

    assert native_toc == baseline


def test_writer_engine_native_toc_respects_custom_magic(tmpdir):
    configure(writer_engine="native_toc", magic=b"custom-magic")
    target = tmpdir.join("custom_magic.msg").strpath
    dump(target, {"hello": "world"})

    with open(target, "rb") as fh:
        header = fh.read(LazyWriter.magic_len())

    assert header == LazyWriter.magic


def test_writer_engine_numpy_array_tolist_mode(tmpdir):
    numpy = pytest.importorskip("numpy")
    obj = {"arr": numpy.arange(12, dtype=numpy.int64).reshape(3, 4)}

    configure(writer_engine="python", numpy_encoder=False)
    baseline_path = tmpdir.join("numpy_python.msg").strpath
    dump(baseline_path, obj)
    with LazyReader(baseline_path) as reader:
        baseline = reader.to_obj()

    configure(writer_engine="native_toc", numpy_encoder=False)
    native_toc_path = tmpdir.join("numpy_native_toc.msg").strpath
    dump(native_toc_path, obj)
    with LazyReader(native_toc_path) as reader:
        native_toc_obj = reader.to_obj()

    assert native_toc_obj == baseline


def test_writer_engine_numpy_array_binary_mode(tmpdir):
    numpy = pytest.importorskip("numpy")
    arr = numpy.random.random((2, 3, 4))
    obj = {"arr": arr}

    configure(writer_engine="python", numpy_encoder=True)
    baseline_path = tmpdir.join("numpy_bin_python.msg").strpath
    dump(baseline_path, obj)
    with LazyReader(baseline_path) as reader:
        baseline = reader.to_obj()

    configure(writer_engine="native_toc", numpy_encoder=True)
    native_toc_path = tmpdir.join("numpy_bin_native_toc.msg").strpath
    dump(native_toc_path, obj)
    with LazyReader(native_toc_path) as reader:
        native_toc_obj = reader.to_obj()

    # binary mode stores the ndarray payload as bytes; compare exact payload
    assert native_toc_obj["arr"] == baseline["arr"]

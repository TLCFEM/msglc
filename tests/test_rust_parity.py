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

import pytest

from msglc import dump
from msglc.codec import CBORCodec, MsgspecCodec, acquire_codec
from msglc.config import config


def _find_item_size(packer, threshold: int) -> int:
    """Find the bytes length so that ``[bytes]`` encodes to exactly ``threshold``."""
    codec = acquire_codec(packer)
    # A single-element array uses a one-byte array header.
    for size in range(threshold):
        if 1 + len(codec.encode(b"x" * size)) == threshold:
            return size
    raise AssertionError(f"cannot build an item of exactly {threshold} bytes")


def _assert_identical_bytes(tmp_path, data, packer):
    python_path = tmp_path / "python.msg"
    rust_path = tmp_path / "rust.msg"
    dump(str(python_path), data, backend="python", packer=packer)
    dump(str(rust_path), data, backend="rust", packer=packer)

    assert python_path.read_bytes() == rust_path.read_bytes()


@pytest.mark.parametrize("packer", [MsgspecCodec(), CBORCodec], ids=["msgspec", "cbor"])
@pytest.mark.parametrize("threshold", [64, 256])
def test_leaf_threshold_boundary_parity(tmp_path, monkeypatch, packer, threshold):
    """A container of exactly the threshold size must not be a leaf."""
    monkeypatch.setattr(config, "small_obj_optimization_threshold", threshold)
    monkeypatch.setattr(config, "trivial_size", min(20, threshold))

    item_size = _find_item_size(packer, threshold)
    data = [b"x" * item_size]

    # The encoded object (one-byte array header plus the item) is exactly the
    # size of the threshold, which is the boundary under test.
    assert 1 + len(acquire_codec(packer).encode(b"x" * item_size)) == threshold

    _assert_identical_bytes(tmp_path, data, packer)


@pytest.mark.parametrize("packer", [MsgspecCodec(), CBORCodec], ids=["msgspec", "cbor"])
@pytest.mark.parametrize("threshold", [64, 256])
def test_block_threshold_boundary_parity(tmp_path, monkeypatch, packer, threshold):
    """Block boundaries must agree when a block is exactly the threshold size."""
    monkeypatch.setattr(config, "small_obj_optimization_threshold", threshold)
    monkeypatch.setattr(config, "trivial_size", min(20, threshold))

    # Values below 128 are encoded as one-byte positive fixints, so the first
    # block reaches the threshold exactly after ``threshold`` items.
    data = list(range(threshold + 6))

    _assert_identical_bytes(tmp_path, data, packer)


@pytest.mark.parametrize("packer", [MsgspecCodec(), CBORCodec], ids=["msgspec", "cbor"])
def test_small_object_parity_across_thresholds(tmp_path, monkeypatch, packer):
    """Objects just around the thresholds must be encoded identically."""
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 64)
    monkeypatch.setattr(config, "trivial_size", 20)

    for item_size in range(0, 65):
        data = [b"x" * item_size]
        _assert_identical_bytes(tmp_path, data, packer)

    for count in range(0, 70):
        _assert_identical_bytes(tmp_path, list(range(count)), packer)

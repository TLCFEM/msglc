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

from collections import UserDict

import pytest

from msglc import LazyReader, dump
from msglc.utility import has_cycle


def _self_referential_list():
    value = []
    value.append(value)
    return value


def _self_referential_dict():
    value = {}
    value["self"] = value
    return value


def _self_referential_mapping():
    value = UserDict()
    value["self"] = value
    return value


def _indirect_cycle():
    items = []
    node = {"items": items}
    items.append(node)
    return items


def _cycle_through_tuple():
    items = []
    value = (items,)
    items.append(value)
    return value


def _deeply_nested_cycle():
    value = {"outer": [{"inner": []}]}
    value["outer"][0]["inner"].append(value)
    return value


@pytest.mark.parametrize(
    "factory",
    [
        _self_referential_list,
        _self_referential_dict,
        _self_referential_mapping,
        _indirect_cycle,
        _cycle_through_tuple,
        _deeply_nested_cycle,
    ],
    ids=["list", "dict", "mapping", "indirect", "tuple", "deep"],
)
def test_has_cycle_detects_cycles(factory):
    assert has_cycle(factory())


@pytest.mark.parametrize("data", [None, 1, "text", b"bytes", [], {}, (), set()])
def test_has_cycle_accepts_scalars_and_empty_containers(data):
    assert not has_cycle(data)


@pytest.mark.parametrize("backend", ["python", "rust"])
def test_shared_acyclic_references_are_allowed(tmpdir, backend):
    shared = {"value": 1}
    data = [shared, shared, {"nested": [shared]}]

    assert not has_cycle(data)

    with tmpdir.as_cwd():
        dump("data", data, backend=backend)

        with LazyReader("data") as reader:
            assert reader.to_obj() == data

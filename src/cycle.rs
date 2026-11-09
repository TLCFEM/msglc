//  Copyright (C) 2026 Theodore Chang
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFrozenSet, PyList, PySet, PyTuple};
use std::collections::HashSet;

fn is_container(obj: &Bound<'_, PyAny>, mapping_type: &Bound<'_, PyAny>) -> PyResult<bool> {
    if obj.is_instance_of::<PyDict>()
        || obj.is_instance_of::<PyList>()
        || obj.is_instance_of::<PyTuple>()
        || obj.is_instance_of::<PySet>()
        || obj.is_instance_of::<PyFrozenSet>()
    {
        return Ok(true);
    }

    obj.is_instance(mapping_type)
}

fn children<'py>(
    obj: &Bound<'py, PyAny>,
    mapping_type: &Bound<'py, PyAny>,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if obj.is_instance_of::<PyDict>() || obj.is_instance(mapping_type)? {
        return obj.call_method0("values")?.try_iter()?.collect();
    }

    obj.try_iter()?.collect()
}

/// Return `True` if the given object contains a circular reference.
#[pyfunction]
pub fn has_cycle_rust_impl(py: Python<'_>, obj: Bound<'_, PyAny>) -> PyResult<bool> {
    let mapping_type = py.import("collections.abc")?.getattr("Mapping")?;

    let mut active: HashSet<usize> = HashSet::new();
    let mut stack: Vec<(Bound<'_, PyAny>, bool)> = vec![(obj, false)];

    while let Some((value, exiting)) = stack.pop() {
        if !is_container(&value, &mapping_type)? {
            continue;
        }

        let value_id = value.as_ptr() as usize;

        if exiting {
            active.remove(&value_id);
            continue;
        }

        if !active.insert(value_id) {
            return Ok(true);
        }

        let children = children(&value, &mapping_type)?;
        stack.push((value, true));
        stack.extend(children.into_iter().map(|child| (child, false)));
    }

    Ok(false)
}

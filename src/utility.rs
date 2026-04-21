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

use pyo3::{Py, PyAny, PyErr};

pub const HEADER_FIELD_LEN: usize = 10;
pub const HEADER_TOTAL_LEN: usize = 2 * HEADER_FIELD_LEN;

pub fn to_py(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

pub enum LazyTOC {
    Leaf {
        pos: [u64; 2],
    },
    Blocked {
        blocks: Vec<(u64, u64, u64)>,
    },
    Normal {
        pos: [u64; 2],
        container: LazyContainer,
    },
}

pub enum LazyContainer {
    Map(Vec<(Py<PyAny>, LazyTOC)>),
    Array(Vec<LazyTOC>),
}

impl LazyTOC {
    pub fn is_trivial(&self, threshold: u64) -> bool {
        matches!(self, LazyTOC::Leaf { pos } if (pos[1] - pos[0]) <= threshold)
    }
}

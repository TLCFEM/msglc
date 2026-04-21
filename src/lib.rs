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

mod cbor;
mod msgpack;
pub mod utility;
use cbor::dump_rust_impl_cbor;
use msgpack::dump_rust_impl_msgpack;
use pyo3::prelude::*;

#[pymodule]
fn msglc_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(dump_rust_impl_cbor, m)?)?;
    m.add_function(wrap_pyfunction!(dump_rust_impl_msgpack, m)?)?;
    Ok(())
}

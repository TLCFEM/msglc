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

use minicbor::data::Int;
use minicbor::encode::Write as CBORWrite;
use minicbor::Encoder;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PySet, PyTuple};
use std::fs::File;
use std::io::{BufWriter, Seek, Write};

const HEADER_FIELD_LEN: usize = 10;
const HEADER_TOTAL_LEN: usize = 2 * HEADER_FIELD_LEN;

fn to_py(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

enum LazyTOC {
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

enum LazyContainer {
    Map(Vec<(Py<PyAny>, LazyTOC)>),
    Array(Vec<LazyTOC>),
}

fn write_primitive(obj: &Bound<'_, PyAny>, out: &mut Encoder<LazyBuffer>) -> PyResult<()> {
    let unsupported = Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Unsupported type.",
    ));

    if obj.is_none() {
        out.null().map_err(to_py)?;
    } else if let Ok(value) = obj.cast::<pyo3::types::PyBool>() {
        out.bool(value.is_true()).map_err(to_py)?;
    } else if let Ok(value) = obj.cast::<pyo3::types::PyFloat>() {
        out.f64(value.value()).map_err(to_py)?;
    } else if obj.is_instance_of::<pyo3::types::PyInt>() {
        if let Ok(value) = obj.extract::<i64>() {
            out.int(Int::from(value)).map_err(to_py)?;
        } else if let Ok(value) = obj.extract::<u64>() {
            out.int(Int::from(value)).map_err(to_py)?;
        } else {
            return unsupported;
        }
    } else if let Ok(value) = obj.cast::<PyBytes>() {
        out.bytes(value.as_bytes()).map_err(to_py)?;
    } else if let Ok(value) = obj.cast::<pyo3::types::PyByteArray>() {
        let bytes = value.to_vec();
        out.bytes(&bytes).map_err(to_py)?;
    } else if let Ok(value) = obj.cast::<pyo3::types::PyString>() {
        out.str(value.to_str()?).map_err(to_py)?;
    } else {
        return unsupported;
    }

    Ok(())
}

impl LazyTOC {
    fn is_trivial(&self, threshold: u64) -> bool {
        matches!(self, LazyTOC::Leaf { pos } if (pos[1] - pos[0]) <= threshold)
    }

    fn encode(&self, py: Python<'_>, out: &mut Encoder<LazyBuffer>) -> PyResult<()> {
        match self {
            LazyTOC::Leaf { pos } => {
                out.map(1).map_err(to_py)?;
                out.str("p").map_err(to_py)?;
                out.array(2).map_err(to_py)?;
                out.int(Int::from(pos[0])).map_err(to_py)?;
                out.int(Int::from(pos[1])).map_err(to_py)?;
            }
            LazyTOC::Blocked { blocks } => {
                out.map(1).map_err(to_py)?;
                out.str("p").map_err(to_py)?;
                out.array(blocks.len() as u64).map_err(to_py)?;
                for &(count, start, end) in blocks {
                    out.array(3).map_err(to_py)?;
                    out.int(Int::from(count)).map_err(to_py)?;
                    out.int(Int::from(start)).map_err(to_py)?;
                    out.int(Int::from(end)).map_err(to_py)?;
                }
            }
            LazyTOC::Normal { pos, container } => {
                out.map(2).map_err(to_py)?;
                out.str("p").map_err(to_py)?;
                out.array(2).map_err(to_py)?;
                out.int(Int::from(pos[0])).map_err(to_py)?;
                out.int(Int::from(pos[1])).map_err(to_py)?;
                out.str("t").map_err(to_py)?;
                match container {
                    LazyContainer::Map(items) => {
                        out.map(items.len() as u64).map_err(to_py)?;
                        for (key, child) in items {
                            write_primitive(key.bind(py), out)?;
                            child.encode(py, out)?;
                        }
                    }
                    LazyContainer::Array(items) => {
                        out.array(items.len() as u64).map_err(to_py)?;
                        for child in items {
                            child.encode(py, out)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn build_tree(
    start_pos: u64,
    end_pos: u64,
    all_trivial: bool,
    children: LazyContainer,
    small_obj_threshold: u64,
) -> LazyTOC {
    if end_pos <= small_obj_threshold + start_pos {
        return LazyTOC::Leaf {
            pos: [start_pos, end_pos],
        };
    }

    if !all_trivial {
        return LazyTOC::Normal {
            pos: [start_pos, end_pos],
            container: children,
        };
    }

    if let LazyContainer::Array(ref items) = children {
        let mut blocks = Vec::new();
        let mut count = 0u64;
        let mut size = 0u64;
        let mut block_start = 0u64;

        for (index, item) in items.iter().enumerate() {
            if let LazyTOC::Leaf { pos } = item {
                if count == 0 {
                    block_start = pos[0];
                }
                count += 1;
                size += pos[1] - pos[0];
                if size > small_obj_threshold || index + 1 == items.len() {
                    blocks.push((count, block_start, pos[1]));
                    count = 0;
                    size = 0;
                }
            }
        }

        if blocks.len() > 1 {
            return LazyTOC::Blocked { blocks };
        }
    }

    LazyTOC::Leaf {
        pos: [start_pos, end_pos],
    }
}

struct LazyBuffer {
    buffer: BufWriter<File>,
    current_pos: u64, // do not use `stream_position` to reduce syscalls
    initial_pos: u64, // always len(magic) + 20
}

impl LazyBuffer {
    fn new(buffer: BufWriter<File>, initial_pos: u64) -> std::io::Result<Self> {
        Ok(Self {
            buffer,
            current_pos: 0,
            initial_pos,
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.flush()
    }

    fn seek(&mut self, in_pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let pos = self.buffer.seek(in_pos)?;
        self.current_pos = pos;
        Ok(pos)
    }
}

impl CBORWrite for LazyBuffer {
    type Error = std::io::Error;

    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.current_pos += data.len() as u64;
        self.buffer.write_all(data)?;
        Ok(())
    }
}

struct LazyWriter<'py> {
    py: Python<'py>,
    encoder: Encoder<LazyBuffer>,
    ndarray_type: Option<Py<PyAny>>,
    sorted_fn: Py<PyAny>,
    trivial_size: u64,
    small_obj_threshold: u64,
    numpy_encoder: bool,
}

impl<'py> LazyWriter<'py> {
    fn new(py: Python<'py>, path: &str, magic_len: usize) -> PyResult<Self> {
        let config = py.import("msglc.config")?.getattr("config")?;

        Ok(Self {
            py,
            encoder: Encoder::new(
                LazyBuffer::new(
                    BufWriter::with_capacity(
                        config.getattr("write_buffer_size")?.extract()?,
                        File::create(path).map_err(to_py)?,
                    ),
                    (magic_len + HEADER_TOTAL_LEN) as u64,
                )
                .map_err(to_py)?,
            ),
            ndarray_type: py
                .import("numpy")
                .ok()
                .and_then(|m| m.getattr("ndarray").ok())
                .map(Bound::unbind),
            sorted_fn: py.import("builtins")?.getattr("sorted")?.unbind(),
            trivial_size: config.getattr("trivial_size")?.extract()?,
            small_obj_threshold: config
                .getattr("small_obj_optimization_threshold")?
                .extract()?,
            numpy_encoder: config.getattr("numpy_encoder")?.extract()?,
        })
    }

    fn writer(&self) -> &LazyBuffer {
        self.encoder.writer()
    }

    fn writer_mut(&mut self) -> &mut LazyBuffer {
        self.encoder.writer_mut()
    }

    // this method shall only be called when packing the data
    fn offset(&self) -> PyResult<u64> {
        if self.writer().current_pos < self.writer().initial_pos {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Buffer position is before the initial position.",
            ));
        }

        Ok(self.writer().current_pos - self.writer().initial_pos)
    }

    fn try_pack_numpy(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Option<LazyTOC>> {
        let Some(ndarray_type) = &self.ndarray_type else {
            return Ok(None);
        };

        if !obj.is_instance(ndarray_type.bind(self.py))? {
            return Ok(None);
        }

        if !self.numpy_encoder {
            let value = obj.call_method0("tolist")?;
            return self.pack(value.as_any()).map(Some);
        }

        let start_pos = self.offset()?;
        let bytes = obj.call_method0("dumps")?.cast_into::<PyBytes>()?;
        self.encoder.bytes(bytes.as_bytes()).map_err(to_py)?;

        Ok(Some(LazyTOC::Leaf {
            pos: [start_pos, self.offset()?],
        }))
    }

    fn pack_map(&mut self, value: &Bound<'py, PyDict>) -> PyResult<LazyTOC> {
        let start_pos = self.offset()?;
        let mut all_trivial = true;
        let mut items = Vec::with_capacity(value.len());

        self.encoder.map(value.len() as u64).map_err(to_py)?;

        for (k, v) in value.iter() {
            write_primitive(&k, &mut self.encoder)?;
            let node = self.pack(&v)?;
            if all_trivial && !node.is_trivial(self.trivial_size) {
                all_trivial = false;
            }
            items.push((k.unbind(), node));
        }

        Ok(build_tree(
            start_pos,
            self.offset()?,
            all_trivial,
            LazyContainer::Map(items),
            self.small_obj_threshold,
        ))
    }

    fn pack_array(
        &mut self,
        iter: impl Iterator<Item = Bound<'py, PyAny>>,
        len: usize,
    ) -> PyResult<LazyTOC> {
        let start_pos = self.offset()?;
        let mut all_trivial = true;
        let mut items = Vec::with_capacity(len);

        self.encoder.array(len as u64).map_err(to_py)?;

        for item in iter {
            let node = self.pack(&item)?;
            if all_trivial && !node.is_trivial(self.trivial_size) {
                all_trivial = false;
            }
            items.push(node);
        }

        Ok(build_tree(
            start_pos,
            self.offset()?,
            all_trivial,
            LazyContainer::Array(items),
            self.small_obj_threshold,
        ))
    }

    fn pack(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<LazyTOC> {
        if let Ok(value) = obj.cast::<PyDict>() {
            return self.pack_map(value);
        }
        if let Ok(value) = obj.cast::<PyList>() {
            return self.pack_array(value.iter(), value.len());
        }
        if let Ok(value) = obj.cast::<PyTuple>() {
            return self.pack_array(value.iter(), value.len());
        }
        if obj.cast::<PySet>().is_ok() {
            let value = self
                .sorted_fn
                .bind(self.py)
                .call1((obj,))?
                .cast_into::<PyList>()?;
            return self.pack_array(value.iter(), value.len());
        }
        if let Some(node) = self.try_pack_numpy(obj)? {
            return Ok(node);
        }

        let start_pos = self.offset()?;

        write_primitive(obj, &mut self.encoder)?;

        Ok(LazyTOC::Leaf {
            pos: [start_pos, self.offset()?],
        })
    }
}

fn encode_header(value: u64) -> PyResult<[u8; HEADER_FIELD_LEN]> {
    let mut encoded = [0u8; HEADER_FIELD_LEN];
    minicbor::encode(value, encoded.as_mut()).map_err(to_py)?;
    Ok(encoded)
}

#[pyfunction]
pub fn dump_rust_impl_cbor(py: Python<'_>, path: String, obj: Bound<'_, PyAny>) -> PyResult<()> {
    let magic: Vec<u8> = py
        .import("msglc.writer")?
        .getattr("LazyWriter")?
        .getattr("magic")?
        .extract()?;
    let header_pos = std::io::SeekFrom::Start(magic.len() as u64);

    let mut writer = LazyWriter::new(py, &path, magic.len())?;
    writer.writer_mut().write_all(&magic).map_err(to_py)?;
    writer
        .writer_mut()
        .write_all(&[0u8; HEADER_TOTAL_LEN])
        .map_err(to_py)?;

    let toc = writer.pack(&obj)?;
    let toc_start_pos = writer.offset()?;
    toc.encode(py, &mut writer.encoder)?;
    let toc_end_pos = writer.offset()?;

    writer.writer_mut().seek(header_pos).map_err(to_py)?;
    let toc_start = encode_header(toc_start_pos)?;
    let toc_size = encode_header(toc_end_pos - toc_start_pos)?;
    writer.writer_mut().write_all(&toc_start).map_err(to_py)?;
    writer.writer_mut().write_all(&toc_size).map_err(to_py)?;
    writer.writer_mut().flush().map_err(to_py)?;

    Ok(())
}

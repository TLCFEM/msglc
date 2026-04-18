use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PySet, PyTuple};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};

const HEADER_FIELD_LEN: usize = 10;
const HEADER_TOTAL_LEN: usize = 2 * HEADER_FIELD_LEN;

fn to_py(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

enum TOC {
    Leaf {
        pos: [u64; 2],
    },
    Blocked {
        blocks: Vec<(u64, u64, u64)>,
    },
    Normal {
        pos: [u64; 2],
        container: TOCContainer,
    },
}

enum TOCContainer {
    Map(Vec<(Py<PyAny>, TOC)>),
    Array(Vec<TOC>),
}

fn write_primitive<W: Write>(obj: &Bound<'_, PyAny>, out: &mut W) -> PyResult<()> {
    if obj.is_none() {
        rmp::encode::write_nil(out).map_err(to_py)?;
        return Ok(());
    }

    if obj.is_instance_of::<pyo3::types::PyBool>() {
        let value: bool = obj.extract()?;
        rmp::encode::write_bool(out, value).map_err(to_py)?;
        return Ok(());
    }

    if obj.is_instance_of::<pyo3::types::PyFloat>() {
        let value: f64 = obj.extract()?;
        rmp::encode::write_f64(out, value).map_err(to_py)?;
        return Ok(());
    }

    if obj.is_instance_of::<pyo3::types::PyInt>() {
        if let Ok(value) = obj.extract::<i64>() {
            rmp::encode::write_sint(out, value).map_err(to_py)?;
            return Ok(());
        }
        if let Ok(value) = obj.extract::<u64>() {
            rmp::encode::write_uint(out, value).map_err(to_py)?;
            return Ok(());
        }
    }

    if obj.is_instance_of::<pyo3::types::PyByteArray>()
        || obj.is_instance_of::<pyo3::types::PyMemoryView>()
    {
        let value = obj.call_method0("tobytes")?.cast_into::<PyBytes>()?;
        rmp::encode::write_bin(out, value.as_bytes()).map_err(to_py)?;
        return Ok(());
    }

    if let Ok(value) = obj.cast::<PyBytes>() {
        rmp::encode::write_bin(out, value.as_bytes()).map_err(to_py)?;
        return Ok(());
    }

    if let Ok(value) = obj.cast::<pyo3::types::PyString>() {
        rmp::encode::write_str(out, value.to_str()?).map_err(to_py)?;
        return Ok(());
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Unsupported type.",
    ))
}

impl TOC {
    fn is_trivial(&self, threshold: u64) -> bool {
        matches!(self, TOC::Leaf { pos } if (pos[1] - pos[0]) <= threshold)
    }

    fn encode(&self, py: Python<'_>, out: &mut Vec<u8>) -> PyResult<()> {
        match self {
            TOC::Leaf { pos } => {
                rmp::encode::write_map_len(out, 1).map_err(to_py)?;
                rmp::encode::write_str(out, "p").map_err(to_py)?;
                rmp::encode::write_array_len(out, 2).map_err(to_py)?;
                rmp::encode::write_uint(out, pos[0]).map_err(to_py)?;
                rmp::encode::write_uint(out, pos[1]).map_err(to_py)?;
            }
            TOC::Blocked { blocks } => {
                rmp::encode::write_map_len(out, 1).map_err(to_py)?;
                rmp::encode::write_str(out, "p").map_err(to_py)?;
                rmp::encode::write_array_len(out, blocks.len() as u32).map_err(to_py)?;
                for &(count, start, end) in blocks {
                    rmp::encode::write_array_len(out, 3).map_err(to_py)?;
                    rmp::encode::write_uint(out, count).map_err(to_py)?;
                    rmp::encode::write_uint(out, start).map_err(to_py)?;
                    rmp::encode::write_uint(out, end).map_err(to_py)?;
                }
            }
            TOC::Normal { pos, container } => {
                rmp::encode::write_map_len(out, 2).map_err(to_py)?;
                rmp::encode::write_str(out, "p").map_err(to_py)?;
                rmp::encode::write_array_len(out, 2).map_err(to_py)?;
                rmp::encode::write_uint(out, pos[0]).map_err(to_py)?;
                rmp::encode::write_uint(out, pos[1]).map_err(to_py)?;
                rmp::encode::write_str(out, "t").map_err(to_py)?;
                match container {
                    TOCContainer::Map(items) => {
                        rmp::encode::write_map_len(out, items.len() as u32).map_err(to_py)?;
                        for (key, child) in items {
                            write_primitive(key.bind(py), out)?;
                            child.encode(py, out)?;
                        }
                    }
                    TOCContainer::Array(items) => {
                        rmp::encode::write_array_len(out, items.len() as u32).map_err(to_py)?;
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
    children: TOCContainer,
    small_obj_threshold: u64,
) -> TOC {
    let size = end_pos - start_pos;

    if size <= small_obj_threshold {
        return TOC::Leaf {
            pos: [start_pos, end_pos],
        };
    }

    if !all_trivial {
        return TOC::Normal {
            pos: [start_pos, end_pos],
            container: children,
        };
    }

    if let TOCContainer::Array(ref items) = children {
        let mut blocks = Vec::new();
        let mut count = 0u64;
        let mut size = 0u64;
        let mut block_start = 0u64;

        for (index, item) in items.iter().enumerate() {
            if let TOC::Leaf { pos } = item {
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
            return TOC::Blocked { blocks };
        }
    }

    TOC::Leaf {
        pos: [start_pos, end_pos],
    }
}

struct LazyBuffer<W: Write + Seek> {
    buffer: W,
    current_pos: u64,
    initial_pos: u64, // always len(magic) + 20
}

impl<W: Write + Seek> LazyBuffer<W> {
    fn new(buffer: W, initial_pos: u64) -> std::io::Result<Self> {
        Ok(Self {
            buffer,
            current_pos: 0,
            initial_pos,
        })
    }
}

impl<W: Write + Seek> Write for LazyBuffer<W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let size = self.buffer.write(data)?;
        self.current_pos += size as u64;
        Ok(size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.flush()
    }
}

impl<W: Write + Seek> Seek for LazyBuffer<W> {
    fn seek(&mut self, in_pos: SeekFrom) -> std::io::Result<u64> {
        let pos = self.buffer.seek(in_pos)?;
        self.current_pos = pos;
        Ok(pos)
    }
}

struct LazyWriter<'py> {
    py: Python<'py>,
    buffer: LazyBuffer<BufWriter<File>>,
    ndarray_type: Option<Py<PyAny>>,
    trivial_size: u64,
    small_obj_threshold: u64,
    numpy_encoder: bool,
}

impl<'py> LazyWriter<'py> {
    fn new(py: Python<'py>, path: &str, magic_len: usize) -> PyResult<Self> {
        let config = py.import("msglc.config")?.getattr("config")?;

        Ok(Self {
            py,
            buffer: LazyBuffer::new(
                BufWriter::with_capacity(
                    config.getattr("write_buffer_size")?.extract()?,
                    File::create(path).map_err(to_py)?,
                ),
                (magic_len + HEADER_TOTAL_LEN) as u64,
            )
            .map_err(to_py)?,
            ndarray_type: py
                .import("numpy")
                .ok()
                .and_then(|m| m.getattr("ndarray").ok())
                .map(Bound::unbind),
            trivial_size: config.getattr("trivial_size")?.extract()?,
            small_obj_threshold: config
                .getattr("small_obj_optimization_threshold")?
                .extract()?,
            numpy_encoder: config.getattr("numpy_encoder")?.extract()?,
        })
    }

    fn offset(&mut self) -> u64 {
        self.buffer.current_pos - self.buffer.initial_pos
    }

    fn try_pack_numpy(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Option<TOC>> {
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

        let start_pos = self.offset();
        let bytes = obj.call_method0("dumps")?.cast_into::<PyBytes>()?;
        let value = bytes.as_bytes();
        rmp::encode::write_bin_len(&mut self.buffer, value.len() as u32).map_err(to_py)?;
        self.buffer.write_all(value).map_err(to_py)?;

        Ok(Some(TOC::Leaf {
            pos: [start_pos, self.offset()],
        }))
    }

    fn pack_map(&mut self, value: &Bound<'py, PyDict>) -> PyResult<TOC> {
        let start_pos = self.offset();
        let mut all_trivial = true;
        let mut items = Vec::with_capacity(value.len());

        rmp::encode::write_map_len(&mut self.buffer, value.len() as u32).map_err(to_py)?;

        for (k, v) in value.iter() {
            write_primitive(&k, &mut self.buffer)?;
            let node = self.pack(&v)?;
            if all_trivial && !node.is_trivial(self.trivial_size) {
                all_trivial = false;
            }
            items.push((k.unbind(), node));
        }

        Ok(build_tree(
            start_pos,
            self.offset(),
            all_trivial,
            TOCContainer::Map(items),
            self.small_obj_threshold,
        ))
    }

    fn pack_array(
        &mut self,
        iter: impl Iterator<Item = Bound<'py, PyAny>>,
        len: usize,
    ) -> PyResult<TOC> {
        let start_pos = self.offset();
        let mut all_trivial = true;
        let mut items = Vec::with_capacity(len);

        rmp::encode::write_array_len(&mut self.buffer, len as u32).map_err(to_py)?;

        for item in iter {
            let node = self.pack(&item)?;
            if all_trivial && !node.is_trivial(self.trivial_size) {
                all_trivial = false;
            }
            items.push(node);
        }

        Ok(build_tree(
            start_pos,
            self.offset(),
            all_trivial,
            TOCContainer::Array(items),
            self.small_obj_threshold,
        ))
    }

    fn pack(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<TOC> {
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
                .py
                .import("builtins")?
                .getattr("sorted")?
                .call1((obj,))?
                .cast_into::<PyList>()?;
            return self.pack_array(value.iter(), value.len());
        }
        if let Some(node) = self.try_pack_numpy(obj)? {
            return Ok(node);
        }

        let start_pos = self.offset();

        write_primitive(obj, &mut self.buffer)?;

        Ok(TOC::Leaf {
            pos: [start_pos, self.offset()],
        })
    }
}

fn encode_header(value: u64) -> PyResult<[u8; HEADER_FIELD_LEN]> {
    let mut encoded = Vec::new();
    rmp::encode::write_uint(&mut encoded, value).map_err(to_py)?;
    let mut out = [0u8; HEADER_FIELD_LEN];
    let start = HEADER_FIELD_LEN - encoded.len();
    out[start..].copy_from_slice(&encoded);
    Ok(out)
}

#[pyfunction]
fn dump_rust_impl(py: Python<'_>, path: String, obj: Bound<'_, PyAny>) -> PyResult<()> {
    let magic: Vec<u8> = py
        .import("msglc.writer")?
        .getattr("LazyWriter")?
        .getattr("magic")?
        .extract()?;

    let mut writer = LazyWriter::new(py, &path, magic.len())?;
    writer.buffer.write_all(&magic).map_err(to_py)?;
    writer
        .buffer
        .write_all(&[0u8; HEADER_TOTAL_LEN])
        .map_err(to_py)?;

    let toc = writer.pack(&obj)?;
    let toc_start = encode_header(writer.offset())?;
    let mut toc_bytes = Vec::new();
    toc.encode(py, &mut toc_bytes)?;
    writer.buffer.write_all(&toc_bytes).map_err(to_py)?;
    let toc_size = encode_header(toc_bytes.len() as u64)?;

    writer
        .buffer
        .seek(SeekFrom::Start(magic.len() as u64))
        .map_err(to_py)?;
    writer.buffer.write_all(&toc_start).map_err(to_py)?;
    writer.buffer.write_all(&toc_size).map_err(to_py)?;
    writer.buffer.flush().map_err(to_py)?;

    Ok(())
}

#[pymodule]
fn msglc_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(dump_rust_impl, m)?)?;
    Ok(())
}

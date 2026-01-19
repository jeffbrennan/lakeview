pub mod delta;

use std::path::Path;

use pyo3::prelude::*;

use crate::delta::{get_table_history, TableHistory};
#[pyfunction]
fn hello_from_bin() -> String {
    "Hello from lakeview!".to_string()
}

#[pyfunction]
#[pyo3(signature = (path, recursive, limit=None))]
fn get_table_history_py(path: &str, recursive: bool, limit: Option<usize>) -> Vec<TableHistory> {
    let path_resolved = Path::new(path);
    let result = get_table_history(path_resolved, recursive, limit);
    result.unwrap()
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_from_bin, m)?)?;
    m.add_function(wrap_pyfunction!(get_table_history_py, m)?)?;
    Ok(())
}

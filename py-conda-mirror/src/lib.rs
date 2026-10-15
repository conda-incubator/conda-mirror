use pyo3::{
    Bound, PyResult, Python, pyfunction, pymodule,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction,
};

#[pyfunction]
pub fn mirror_py() -> String {
    "Hello, world!".to_string()
}

#[pymodule]
fn conda_mirror<'py>(_py: Python<'py>, m: Bound<'py, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(mirror_py, &m).unwrap())?;

    Ok(())
}

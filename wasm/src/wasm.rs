use std::sync::Mutex;
use std::sync::PoisonError;

use once_cell::sync::Lazy;
use sboxdb::access::kv::Kv;
use sboxdb::error::Error;
use sboxdb::error::Result;
use sboxdb::session::Session;
use sboxdb::sql::execution::ResultSet;
use sboxdb::storage::memory::Memory;
use wasm_bindgen::prelude::wasm_bindgen;

/// A wasm session with memory kv
struct WasmSession {
    session: Session<Kv<Memory>>,
}

impl WasmSession {
    fn new() -> Self {
        let kv = Kv::new(Memory::new());
        Self { session: Session::new(kv) }
    }
    fn handle(&mut self, query: impl Into<String>) -> Result<ResultSet> {
        self.session.process_query(query.into())
    }
}

#[wasm_bindgen]
pub struct WasmError {
    err_msg: String,
}

#[wasm_bindgen]
impl WasmError {
    pub fn get_err_msg(&self) -> String {
        self.err_msg.clone()
    }
}

impl From<Error> for WasmError {
    fn from(err: Error) -> Self {
        WasmError { err_msg: err.to_string() }
    }
}
impl<T> From<PoisonError<T>> for WasmError {
    fn from(err: PoisonError<T>) -> Self {
        WasmError { err_msg: err.to_string() }
    }
}

#[wasm_bindgen]
pub struct WasmResultSet {
    result_str: String,
}

#[wasm_bindgen]
impl WasmResultSet {
    pub fn get_result_str(&self) -> String {
        self.result_str.clone()
    }
}

impl From<ResultSet> for WasmResultSet {
    fn from(rs: ResultSet) -> Self {
        WasmResultSet { result_str: format!("{}", rs) }
    }
}

static WASM: Lazy<Mutex<WasmSession>> = Lazy::new(|| Mutex::new(WasmSession::new()));

#[wasm_bindgen]
pub fn execute_sql(sql: &str) -> std::result::Result<WasmResultSet, WasmError> {
    let mut session = WASM.lock()?;
    let rs = session.handle(sql)?;
    Ok(WasmResultSet::from(rs))
}

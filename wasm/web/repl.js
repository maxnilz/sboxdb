import init, { execute_sql } from './dist/wasm.js';

const output = document.getElementById('output');
const runBtn = document.getElementById('run');
const sqlInput = document.getElementById('sql');

let wasmReady = false;

init().then(() => {
    wasmReady = true;
    output.textContent = 'WASM loaded. Enter SQL and click Run.';
});

function runSql() {
    if (!wasmReady) {
        output.textContent = 'WASM not loaded yet.';
        return;
    }
    const sql = sqlInput.value.trim();
    if (!sql) {
        output.textContent = 'Please enter a SQL statement.';
        return;
    }
    try {
        const resultSet = execute_sql(sql);
        output.textContent = resultSet.get_result_str();
    } catch (err) {
        output.textContent = `Error: ${err.get_err_msg() || err}`;
    }
}

runBtn.onclick = runSql;

sqlInput.addEventListener('keydown', (e) => {
    if (e.ctrlKey && e.key === 'Enter') {
        e.preventDefault();
        runSql();
    }
});
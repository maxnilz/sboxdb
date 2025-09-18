# wasm lib. List recipes with `just -l wasm`
mod wasm 'wasm/justfile'

sboxdb:
    cargo run --bin sboxdb

cluster:
    ./cluster/run.sh

sboxsql:
    cargo run --bin sboxsql -- -a localhost:8911
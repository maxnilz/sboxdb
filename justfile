# wasm lib. List recipes with `just -l wasm`
mod wasm 'wasm/justfile'

# sltgen tool. List recipes with `just -l sltgen`
mod sltgen 'tools/sltgen'

# build lib and binaries
build:
    cargo build

# run the sboxdb server in standalong mode
sboxdb:
    cargo run --bin sboxdb

# run the sboxdb server in cluster mode
cluster:
    ./cluster/run.sh

# connect to server with sboxsql shell
sboxsql:
    cargo run --bin sboxsql -- -a localhost:8911
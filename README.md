# sboxdb

sboxdb(Sandbox Database) is a distributed SQL database written in Rust, built as a learning project to explore how
modern databases work under the hood — from storage engines and transaction systems (MVCC) to Raft-based replication and
SQL query execution.

**NB**: Just for learning and experimenting db internals, not suitable for real-word use, and not optimized for
performance.

## Outline

- [ ] **KV Storage:**
    - [x] in memory based key-value storage
    - [ ] add LSM based kv storage for OLTP
    - [ ] add parquet based storage for OLAP
    - ~~[x] buffer pool manager with lru-k replacer~~
- [x] **Replication:** Raft-based replicated state machine
    - no cluster membership config change support.
- [ ] **Transactional Storage:** transactional mvcc storage
    - [x] concurrency control with MVCC+OCC
    - [ ] add Write-Ahead-Log support
    - [ ] add ARIES recovery support
- [x] **Transactional access method:**
    - [x] catalog related access methods
    - [x] tuple related CRUD access methods
    - [x] index based access methods
    - [x] raft-backed access methods
- [x] **SQL parser:** A handcraft SQL parser without yacc/bison.
    - **Data Types:** null, boolean, i64, double, utf-8 string
    - **SQL syntax:**
        * `BEGIN`, `COMMIT`, and `ROLLBACK`
        * `[CREATE|DROP] TABLE ...` and `[CREATE|DROP] INDEX ...`
        * `UPDATE [TABLE] SET ... WHERE ...`
        * `DELETE FROM [TABLE] WHERE ...`
        * `SELECT ... FROM ... WHERE ... ORDER BY ...`
        * `EXPLAIN SELECT ...`
        * `SHOW TABLES`
        * `CREATE DATASET ...`
    - **Full reference** at [here](docs/sql.md)
- [ ] **SQL Execution Engine:** Simple heuristic-based planner and optimizer supporting expressions, functions and
  joins.
    - [x] Logical Planner
    - [ ] Logical Optimizer
    - [x] Physical Planner
    - [x] Executors
    - [x] Function support
    - [x] [sql logical test util](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki)
- [x] Wasm for browser for fun, deployed at [here](https://maxnilz.com/app/sboxdb)

## Documentation

- [Architecture](docs/arch.md): A high level architecture about sboxdb
- [SQL reference](docs/sql.md): Detailed SQL syntax sboxdb supported
- [Examples](src/slt/script/README.md): The SQL examples sboxdb supported
- [References](docs/references.md): Materials used while building sboxdb

## Dev Tools

1. [Just](https://github.com/casey/just) - Task runner for project commands
2. [uv](https://docs.astral.sh/uv/getting-started) - Python package manager for
   the [SQL Logical Test script generator](tools/sltgen)

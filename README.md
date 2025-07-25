# sboxdb

A distributed SQL database, written as a learning project about my journey on the database and rust.

`sboxdb` represents `Sandbox DB`.

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
    - **Full reference** at [here](src/sql/sql.md)
- [ ] **SQL Execution Engine:** Simple heuristic-based planner and optimizer supporting expressions, functions and
  joins.
    - [x] Planner
    - [ ] Optimizer
    - [ ] Executors
    - [ ] Function support

## Reference

- https://github.com/cmu-db/bustub
- https://github.com/erikgrinaker/toydb
- https://github.com/postgres/postgres
- https://github.com/apache/datafusion-sqlparser-rs
- https://github.com/apache/datafusion
 
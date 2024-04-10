# sboxdb

A distributed SQL database, written as a learning project about my journey on the database and rust.

`sboxdb` represents `Sandbox DB`.

## Outline

- [ ] **Storage:** pluggable storage engine. start with simplified key-value storage.
    - started with simplified key-value storage
    - no WAL or log compaction
    - consider sharded kv implementation later
    - consider buffer pool manager later
    - consider heap files, clustered B+ tree or LSM for OLTP later
    - consider parquet files for OLAP later
- [ ] **Replication:** Raft-based replicated state machine
    - no cluster membership config change support.
- [ ] **SQL parser:** A handcraft SQL parser without yacc/bison.
    - **Data Types:** null, boolean, i64, double, utf-8 string
    - **SQL syntax:**
        * `BEGIN`, `COMMIT`, and `ROLLBACK`
        * `[CREATE|DROP] TABLE ...` and `[CREATE|DROP] INDEX ...`
        * `UPDATE [TABLE] SET ... WHERE ...`
        * `DELETE FROM [TABLE] WHERE ...`
        * `SELECT ... FROM ... WHERE ... ORDER BY ...`
        * `EXPLAIN SELECT ...`
- [ ] **Query Engine:** Simple heuristic-based planner and optimizer supporting expressions, functions and joins.
- [ ] **Transaction:** MVCC-based serializable isolation. 

## Reference

- https://github.com/erikgrinaker/toydb

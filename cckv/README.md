# cckv: An Experimental Key-Value Store

A simplified LSM-tree implementation in C++20, designed for learning and experimentation. `cckv` serves as the storage engine for `sboxdb`, drawing inspiration from RocksDB to explore core database concepts with a clean, modern codebase.

## Outline

### Infrastructure
  - [x] FileSystem abstraction (POSIX)
  - [x] Codec (InternalKey, Varint)
  - [x] Status/Slice

### Write path
  - [x] Naive std::map based mem table
  - [ ] Lock-free SkipList MemTable
  - [x] Raw allocator with new/delete
  - [x] Strawman Write buffer manager
  - [ ] Arena allocator
  - [x] Delete range
  - [ ] Flush memtable
  - [ ] Level compaction
  - [ ] Indexing (SST Builder)
  - [ ] WAL
  - [ ] Metadata/manifest

### Read path
  - [x] Point lookup
  - [ ] Range deletion tombstones
  - [ ] Range Scan
  - [ ] Merging iterator
  - [ ] Recovery
  - [ ] Block Cache & Bloom Filters

### Multi-threading & Optimization
  - [ ] Writer group (Group Commit)
  - [ ] Parallel Flush/Compaction

## Considerations

- **SkipList MemTable:** A common choice for LSM engines. SkipLists handle many writes at once better than other data structures.
- **Writer Group (Group Commit):** Improves write speed by combining many small writes into one bigger write to the Write-Ahead Log (WAL). This saves time by reducing how often the system talks to the disk.
- **Pipelined Writes (RocksDB Opt):** An improved way to write data. It lets one part of the system write to the WAL while other parts add data to the MemTable at the same time. This makes writing much faster.
- **Bloom Filters:** Important for making reads faster. These filters can quickly tell if a key is *not* in a data file without needing to read the file. This reduces how much data needs to be read from disk.
- **Dynamic Level Sizing (RocksDB Opt):** This feature automatically adjusts the size limits of data levels. It bases these sizes on the largest data level, which helps manage how much disk space is used.
- **Compaction Strategies (Leveled vs. Tiered):** Leveled compaction makes reads faster but can use more disk writes. Tiered compaction writes less to disk but can make reads slower.
- **Write Stalls (Backpressure):** Stops the system from getting overloaded. If data cannot be written to disk fast enough, new writes are temporarily paused to avoid errors.
- **Data Integrity (Checksums):** Prevents data corruption. Data blocks and WAL entries should include checksums (like CRC32 or xxHash) to check if the data is correct when read.
- **Block Compression:** Saves disk space and speeds up disk reads by compressing data blocks (using methods like Zstd, LZ4, Snappy). This uses a little more CPU but greatly reduces disk usage.
- **Tombstone Management:** Deals with old delete markers (tombstones). If too many accumulate, they can slow down reading data ranges. Getting rid of them efficiently is important for performance.
- **Iterator Pinning:** In C++, when using iterators to read data, they must "pin" (hold onto) data blocks in memory. This stops these blocks from being removed from the cache too early, which helps keep the system safe and fast.

## **WHY** C++?

C++20 was primarily chosen to refresh my understanding and catch up with modern C++ practices. Both C++ and Rust offer excellent performance for systems programming, and this LSM-bases storage lib serves as a hands-on opportunity to explore modern C++ capabilities.

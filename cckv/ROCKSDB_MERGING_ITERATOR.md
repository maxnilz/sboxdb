# RocksDB Merging Iterator & Read Path Reminder

This document summarizes the **Merging Iterator** and its role in the RocksDB read path, specifically clarifying how it interacts with Range Scans and duplicates.

## 1. What is the Merging Iterator?
The `MergingIterator` (`table/merging_iterator.cc`) is the fundamental engine that drives the **LSM Read Path**. It is **NOT** related to the user-facing `Merge` API (read-modify-write).

Its purpose is to provide a **Single Sorted View** of the entire database by merging multiple sorted sources.

### How it Works (Merge Sort)
It uses a **Min-Heap** to manage a set of "Child Iterators."
*   **Child Iterators:** These represent the sorted runs at different levels (e.g., one iterator for the Memtable, one for an L0 file, one for the entire L1 level, etc.).
*   **The Heap:** Keeps the "head" of every Child Iterator sorted.
*   **Execution:**
    1.  Look at the heads of all children.
    2.  Pick the **smallest key** (based on Key ASC, then SeqNum DESC).
    3.  Emit that key.
    4.  Advance that specific child iterator.
    5.  Repeat.

### Example Flow

If you have three sources:
*   **Memtable:** `[Key1 @ 10]`
*   **L0 File:** `[Key1 @ 9, Key2 @ 15]`
*   **L1 File:** `[Key2 @ 16, Key3 @ 7]`

The Merging Iterator sees: `Key1@10`, `Key1@9`, `Key2@16`, `Key2@15`, `Key3@7`.
It emits them in exactly that order. It creates the illusion of one massive, sorted list.

---

## 2. The Range Scan "Drill Down"
A common question is whether a Range Scan visits the lowest levels (e.g., Level 6) of the SST files.

*   **Yes, it drills down.**
    *   **Why?** Because valid data (keys) might exist *only* at the bottom level. If `Key M` is only in Level 6, the scan must go there to find it.
    *   **When?** The iterator visits a lower-level file **IF** the scan range overlaps with that file's key range `[Smallest, Largest]` **AND** the data hasn't been skipped by optimizations (like Bloom Filters or Metadata checks).

### The "Parallel" Nature
The Merging Iterator doesn't check Level 0, *then* Level 1. It runs them **in parallel** via the Heap.
*   If the smallest key in the entire DB is currently in Level 6, the Heap will pop the Level 6 iterator's value immediately, "drilling down" instantly for that specific key.

---

## 3. Range Tombstone Handling (Modern Implementation)
**Important:** Range tombstones are handled **directly inside MergingIterator**, not by a separate RangeDelAggregator during reads.

### Historical Evolution (2018-2022)

This was a **4-year journey** with multiple attempts before the current implementation:

1. **2018 - Initial Optimization** ([PR #4677](https://github.com/facebook/rocksdb/pull/4677))
   *   Author: Abhishek Madan
   *   Added position tracking to `RangeDelAggregator` with active/inactive heaps
   *   Problem: Still required `DBIter` to call `RangeDelAggregator::ShouldDelete()` for every key

2. **2019 - First Integration Attempt** ([PR #5506](https://github.com/facebook/rocksdb/pull/5506))
   *   Author: Andrew Kryczka (@ajkr)
   *   Attempted to move logic into `BlockBasedTableIterator`
   *   Status: **Closed without merging** due to complexity

3. **2020 - Second Attempt** ([Issue #7317](https://github.com/facebook/rocksdb/issues/7317))
   *   Author: Andrew Kryczka (@ajkr)
   *   Recognized `RangeDelAggregator` should have same scope as `MergingIterator`
   *   Status: **Not completed**, but laid critical groundwork

4. **2022 - Final Implementation** ([PR #10449](https://github.com/facebook/rocksdb/pull/10449))
   *   Author: Changyu Bi (@cbi42, Meta)
   *   Date: **September 2, 2022**
   *   Commit: **30bc495c0** - `git show 30bc495c0` for full details
   *   Changes: +2,401 lines, -357 lines across 31 files
   *   Inspiration: [Pebble's merging iterator](https://github.com/cockroachdb/pebble/blob/master/merging_iter.go) (CockroachDB's storage engine)
   *   **Key Innovation:** Range tombstone boundaries inserted directly into min/max heap as first-class items
   *   **Critical Invariant Discovered:** "A key in level L has a larger sequence number than all keys in any level >L" - this enables cascading seeks

### Traditional Approach (Pre-2022) - How It Worked

Before the 2022 redesign, range tombstones were handled by a **separate component** outside MergingIterator.

#### Architecture Overview

**Component Separation:**
```
DBIter (user-facing iterator)
  ├─► MergingIterator (point keys only)
  │   └── minHeap containing only point keys
  │
  └─► RangeDelAggregator (tombstones only)
      └── ForwardRangeDelIterator
          ├── active_seqnums_: multiset<iter*, ordered by seqno desc>
          ├── active_iters_: heap<iter-to-seqnums, ordered by end_key asc>
          └── inactive_iters_: heap<iter*, ordered by start_key asc>
```

**Total: 5 separate heaps!** (1 point key + 2 forward + 2 reverse)

#### The Scan Flow

```
User calls: iterator->Next()
  ↓
DBIter::Next()
  ↓
1. key = merging_iter_->Next()        // Pop point key from heap
  ↓
2. while (range_del_agg_->ShouldDelete(key)):  // External call!
     key = merging_iter_->Next()      // If deleted, try next key
  ↓
3. Return key to user
```

#### ForwardRangeDelIterator::ShouldDelete() - The Three Phases

**Phase 1: EXPIRE tombstones**
```cpp
while (!active_iters_.empty() &&
       active_iters_.top()->end_key() <= current_key) {
  // Tombstone ended before current position
  iter = PopActiveIter();           // Remove from both active heaps
  do {
    iter->Next();                   // Advance to next range
  } while (iter->Valid() && iter->end_key() <= current_key);
  PushIter(iter, current_key);      // Reclassify (active/inactive/invalid)
}
```

**Phase 2: ACTIVATE tombstones**
```cpp
while (!inactive_iters_.empty() &&
       inactive_iters_.top()->start_key() <= current_key) {
  // Tombstone starting at/before current position
  iter = PopInactiveIter();
  while (iter->Valid() && iter->end_key() <= current_key) {
    iter->Next();                   // Skip already-expired ranges
  }
  PushIter(iter, current_key);      // Reclassify (active/inactive)
}
```

**Phase 3: CHECK coverage**
```cpp
if (active_seqnums_.empty()) {
  return false;                     // No active tombstones
}
// Get highest-seqno active tombstone
return (*active_seqnums_.begin())->seq() > key.sequence;
```

#### Why Two Heaps for Active Tombstones?

Need to answer two questions with different orderings:

| Question | Data Structure | Ordering | Purpose |
|----------|---------------|----------|---------|
| "Which tombstone expires first?" | `active_iters_` | END KEY (asc) | Find tombstones to expire in Phase 1 |
| "Which has highest precedence?" | `active_seqnums_` | SEQUENCE (desc) | Check coverage in Phase 3 |

**Solution:** `active_iters_` stores iterators INTO `active_seqnums_`, enabling dual ordering!

#### Detailed Example: Scanning Keys

**Database state:**
```
Tombstones: [D, H)@100, [K, O)@80
Point keys: A@50, B@60, D@40, E@45, K@70
```

**Scan trace:**

| Key | Phase 1 (Expire) | Phase 2 (Activate) | Phase 3 (Check) | Active State | Result |
|-----|------------------|-------------------|-----------------|--------------|---------|
| A@50 | Skip (no active) | Both still inactive (D>A, K>A) | No active | {} | FALSE ✓ |
| D@40 | Skip (no active) | [D,H) activates (D≤D) | 100>40? YES | {[D,H)@100} | TRUE ✗ |
| E@45 | [D,H) still active (H>E) | [K,O) still inactive (K>E) | 100>45? YES | {[D,H)@100} | TRUE ✗ |
| K@70 | [D,H) expires (H≤K), advances, becomes invalid | [K,O) activates (K≤K) | 80>70? YES | {[K,O)@80} | TRUE ✗ |

**Key observations:**
- Each key requires `ShouldDelete()` call
- D, E skipped individually (no range optimization)
- State transitions happen lazily as we scan

#### Performance Characteristics

**Per-key cost:**
```
Typical case: O(1) - no state changes
  Just check active_seqnums_.begin()->seq() > key.seq

Worst case: O((E+N) × log T)
  E = # tombstones expiring
  N = # tombstones activating  
  T = total tombstones
```

**Problem: Wide tombstone covering 1000 keys**
```
Old behavior:
  - MergingIterator.Next() → key1
  - ShouldDelete(key1) → TRUE (activate tombstone)
  - MergingIterator.Next() → key2
  - ShouldDelete(key2) → TRUE (check again)
  - MergingIterator.Next() → key3
  - ShouldDelete(key3) → TRUE (check again)
  ... repeat 1000 times ...
  
Total: 1000 MergingIterator.Next() calls
       1000 ShouldDelete() calls
       Each key checked individually!
```

#### The Fundamental Limitation

```
┌──────────────────────────────────────────────┐
│ Why Cascading Seeks Were Impossible          │
├──────────────────────────────────────────────┤
│                                              │
│ MergingIterator doesn't know about tombstones│
│         ↓                                    │
│ Pops point key and returns to DBIter        │
│         ↓                                    │
│ DBIter calls ShouldDelete(key)              │
│         ↓                                    │
│ Discovers key is deleted                     │
│         ↓                                    │
│ But it's TOO LATE!                          │
│         ↓                                    │
│ Key already popped from MergingIterator      │
│         ↓                                    │
│ Can't seek MergingIterator's child iters    │
│         ↓                                    │
│ Must pop next key and check again           │
│         ↓                                    │
│ No way to skip ranges!                       │
│                                              │
└──────────────────────────────────────────────┘
```

The architectural separation between point keys (in MergingIterator) and tombstones (in RangeDelAggregator) prevented optimization because deletion was discovered **after** the key was already removed from the heap.

### Comparison: Traditional vs Modern

| Aspect | Traditional (Pre-2022) | Modern (2022+) |
|--------|----------------------|----------------|
| **Architecture** | Separated: MergingIterator + RangeDelAggregator | Unified: Everything in MergingIterator |
| **Heap Count** | 5 heaps (1 point + 4 tombstone) | 1 heap (all items) |
| **Tombstone Check** | External `ShouldDelete()` call per key | Integrated: tombstones in same heap |
| **Range Skipping** | ❌ Impossible (key already popped) | ✅ Cascading seeks enabled |
| **Performance** | O(N) checks for N covered keys | O(1) seek skips all N keys |
| **Data Structures** | active_seqnums_, active_iters_, inactive_iters_ | active_ set only |
| **Complexity** | Active/inactive state machine | Heap pop/push driven |

**Example: Tombstone [D,M) covers 10 keys**

*Traditional:*
```
Pop D → ShouldDelete? YES → discard
Pop E → ShouldDelete? YES → discard
Pop F → ShouldDelete? YES → discard
... repeat 10 times ...
Total: 10 pops, 10 ShouldDelete() calls
```

*Modern:*
```
Pop DELETE_RANGE_START → active_={0}
Pop D → covered → Seek all levels to M
Pop DELETE_RANGE_END → active_={}
Total: 1 cascading seek, 10 keys skipped
```

### Key Innovations in Detail

#### Background: The LSM Level Invariant

Before diving into the optimizations, it's critical to understand the **LSM level invariant** that makes cascading seeks possible:

**The Invariant (from commit 30bc495c0 - see `git show 30bc495c0` for source):**
> "A key in level L has a larger sequence number than all keys in any level >L"

This is stated explicitly in the commit message of PR #10449 (September 2, 2022) by Changyu Bi.

**How RocksDB Maintains This:**

1. **Write Path:**
   - All writes go to memtable with monotonically increasing sequence numbers
   - Global sequence number counter is atomic and never decreases

2. **Flush Path:**
   - Memtable flushes create L0 SST files
   - Each L0 file has a seqno range [min_seqno, max_seqno]
   - Newer flushes create L0 files with higher seqno ranges

3. **Compaction Path:**
   - Compaction picks files from Level L and Level L+1
   - Merges and writes output to Level L+1
   - The output keys preserve their original sequence numbers
   - Once compaction finishes, Level L's input files are deleted
   - Result: Level L+1 now has keys that were in Level L (moving "older" data down)

4. **The Invariant Emerges:**
   - New data enters at top (memtable/L0) with highest seqnos
   - Old data gets pushed down through compaction
   - By the time a key reaches L4, all keys written after it are in L0-L3
   - Therefore: L0 seqnos > L1 seqnos > L2 seqnos > ... > L6 seqnos

**Special Cases:**
- **L0 files can overlap** in key range, but NOT in seqno range (newer flushes have higher seqnos)
- **Compaction preserves seqnos** - a key keeps its original seqno when moved to lower levels
- **Snapshots preserve old versions** - multiple versions of same key with different seqnos can exist across levels

This invariant is **essential** for the cascading seek optimization to work correctly.

#### 1. Three Heap Item Types (Unified Data Structure)

**Before (2018-2022):**
*   MergingIterator heap contained only point keys
*   Range tombstones stored separately in `RangeDelAggregator`
*   Required calling `ShouldDelete()` for each key (extra overhead)

**After (2022):**
*   The heap contains **three types of items**:
    1.  **ITERATOR** - Regular point keys: `"B" @ seq=50, value="data"`
    2.  **DELETE_RANGE_START** - Tombstone start: `["D", ...) @ seq=100`
    3.  **DELETE_RANGE_END** - Tombstone end: `[..., "H") @ seq=100`

**Why Better:**
*   Single data structure (simpler logic)
*   No separate `ShouldDelete()` calls
*   Tombstone boundaries naturally interleave with point keys
*   State managed by heap operations

#### 2. Cascading Seeks (Major Performance Optimization)

**The Problem:**
```
Database state:
  Level 0: Range tombstone [B, Z) @ seq=200
  Level 3: Keys: B, C, D, E, F, ... X, Y (1000s of keys)

OLD BEHAVIOR: Check each key individually
  1. Check B - deleted → skip
  2. Next() -> C
  3. Check C - deleted → skip
  4. Next() -> D
  ... (repeat 1000s of times!)
  Result: 1000+ individual checks!
```

**The Solution:**
```
NEW BEHAVIOR: Cascading seek
  1. Check B@50 - covered by [B,Z)@200 from Level 0
  2. LSM invariant: Level 0 keys have higher seqnos than Level 3 keys
  3. Therefore ALL keys in [B,Z) from Level 3 must have seqno < 200
  4. SEEK Level 3 directly to Z (skip checking each key individually)
  Result: Single seek instead of 1000+ checks!
  
Note: No per-key sequence number check needed for cross-level case!
The LSM level invariant guarantees coverage.
```

**Key Insight (The LSM Level Invariant):**

The cascading seek optimization relies on a **critical LSM invariant** stated in commit 30bc495c0:

> **"A key in level L has a larger sequence number than all keys in any level >L"**

**Source of truth:** Run `git show 30bc495c0` to see the full commit message, implementation, and performance benchmarks.

Where level numbering: memtable=0, L0=1, L1=2, L2=3, etc. (lower number = higher precedence)

**What this means:**
*   **Precedence order:** Memtable > L0 > L1 > L2 > ... > L6
*   **Sequence number ordering:** Keys in L0 have HIGHER seqnos than keys in L1, which have HIGHER seqnos than keys in L2, etc.
*   **Why it holds:** New writes go to memtable → flush to L0 → compact downward. By the time a key reaches L4, all keys written after it are in upper levels.

**Snapshot Handling - The Three Sequence Numbers:**

When scanning with a snapshot, there are actually **three sequence numbers** involved:

1. **Snapshot sequence number** (e.g., @300) - the read snapshot
2. **Tombstone sequence number** (e.g., @200) - when the DELETE_RANGE was written
3. **Key sequence number** (e.g., @250) - when the point key was written

**Critical: Snapshot filtering happens BEFORE MergingIterator sees tombstones!**

```
Two-Layer Filtering Architecture:

┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: TruncatedRangeDelIterator (Snapshot Filtering)        │
│ ─────────────────────────────────────────────────────────────── │
│                                                                  │
│ SetRangeDelReadSeqno(snapshot_seqno = 300)                     │
│   ↓                                                             │
│ SetMaxVisibleSeqAndTimestamp() filters tombstones:             │
│   - Tombstone [C,M)@400 → 400 > 300 → INVISIBLE (filtered out)│
│   - Tombstone [C,M)@200 → 200 ≤ 300 → VISIBLE (exposed)       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
         Only visible tombstones passed down
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2: MergingIterator (LSM Invariant + Seqno Check)        │
│ ─────────────────────────────────────────────────────────────── │
│                                                                  │
│ Sees: Tombstone [C,M)@200 from Level 0                        │
│       Key D@250 from Level 4                                    │
│                                                                  │
│ Cross-level check (i < current->level):                        │
│   LSM invariant says Level 0 seqnos > Level 4 seqnos          │
│   But wait! Key D@250 > Tombstone @200!                        │
│   This VIOLATES the invariant → but snapshot filtering         │
│   prevents this scenario from occurring in practice             │
│                                                                  │
│ Same-level check (i == current->level):                        │
│   Must check: key.seqno < tombstone.seqno                      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Code Locations:**

*   Snapshot filtering: `db/range_tombstone_fragmenter.h` line 338-357 (`SetMaxVisibleSeqAndTimestamp()`)
*   MergingIterator logic: `table/merging_iterator.cc` line 1028-1075 (`SkipNextDeleted()`)

**Coverage Rules Based on Invariant:**

1. **Cross-level case** (`i < current->level`): **NO sequence number check needed**
   - If tombstone `[C,M)@200` is in Level 0 (i=0)
   - And key `D@20` is in Level 4 (current->level=4)
   - The LSM invariant **guarantees** all keys in Level 4 have seqno < any tombstone in Level 0
   - Snapshot filtering ensures only tombstones visible to the snapshot are considered
   - Therefore: **"definitely covers"** - can cascading seek without checking seqno!

2. **Same-level case** (`i == current->level`): **Explicit sequence number check required**
   - Tombstone and key in same level (e.g., both in Level 2)
   - Must check: `if (key.seqno < tombstone.seqno)` before deciding coverage
   - No ordering guarantee within same level
   - Both tombstone and key already passed snapshot filtering

**Why Cascading Seek is Safe with Snapshots:**

```
Example: Scan with snapshot @300

Database state:
  Level 0: [C,M)@200 (visible to snapshot)
  Level 4: D@250, E@220

Question: Doesn't D@250 violate the invariant since 250 > 200?

Answer: The LSM invariant + snapshot filtering work together:
  - When D@250 was written, it went to L0
  - When [C,M)@200 was written (earlier), it also went to L0
  - D@250 was compacted down to L4
  - If [C,M)@200 still exists, it must be in L0 or higher
  - The invariant: all keys currently in L4 have seqno less than
    all CURRENT keys in L0
  - D@250 in L4 was compacted down when the L0 seqno range
    had moved beyond 250

Correct understanding:
  - The invariant applies to keys CURRENTLY AT each level
  - Not about when keys were originally written
  - Compaction maintains: Level L current max seqno > Level L+1 current max seqno
```

**Why Cascading Seek Works:**
*   When Level 0 has tombstone `[C,M)@200` and Level 4 has key `C@20`
*   Both passed snapshot filtering (both ≤ snapshot seqno)
*   The invariant guarantees **ALL** keys in Level 4 have seqno < 200
*   Instead of checking each key individually (C@20, D@15, E@18, ...), **seek directly to M**
*   This skips potentially thousands of keys in ONE operation

**Important: This is NOT about "precedence alone":**
> ⚠️ **Critical Understanding:** The optimization works because of the **LSM level invariant** PLUS **snapshot filtering at the iterator layer**. The code comment "range tombstone is from a newer level, definitely covers" (line 1035 in merging_iterator.cc) is shorthand for "the LSM invariant guarantees all keys in lower levels have smaller seqnos than this tombstone, and snapshot filtering ensures we only see tombstones visible to the snapshot."

**Performance Impact:**
*   Narrow tombstones: 10-20% improvement
*   Medium tombstones: 2-5x improvement
*   Wide tombstones: **10-50x improvement!**

**What If The Invariant Is Violated?**

If somehow a key with seqno 250 existed in Level 4 while a tombstone with seqno 200 existed in Level 0:
```
Level 0: [C, M)@200
Level 4: D@250, E@220
```

The cascading seek would **incorrectly skip D@250 and E@220** because the code has NO sequence number check for cross-level cases. This would be a **correctness bug** where visible keys are hidden from reads.

**However, this scenario is impossible under normal RocksDB operation because:**
1. Keys are written to memtable with monotonically increasing seqnos
2. Memtable flushes to L0 preserving seqno ordering
3. Compaction moves data from L0→L1→L2→... downward
4. For a key with seqno 250 to be in L4, it must have been written and compacted down
5. At that point, any tombstone with seqno 200 (written earlier) would be in an even lower level or already dropped
6. The invariant is maintained by the write path, flush, and compaction logic

**Edge cases that could potentially break this:**
- File ingestion with `IngestExternalFile()` if seqnos are not assigned correctly
- Bugs in compaction that violate level ordering
- Manual file manipulation

The cascading seek optimization **assumes this invariant holds** and has no safety checks. If violated, it would silently return incorrect results.

**Clarification: The Invariant + Snapshot Filtering:**

The confusion about "what if keys have higher seqnos in lower levels" is resolved by understanding both mechanisms:

1. **The LSM invariant** ensures the CURRENT max seqno in Level L > CURRENT max seqno in Level L+1
2. **Snapshot filtering** ensures we only see tombstones ≤ snapshot seqno
3. Together: When a tombstone @200 from L0 covers keys in L4, all those L4 keys must have seqno < 200

```
Timeline clarification:

T=100: Write Key D@100 → memtable → L0
T=200: Write DeleteRange([C,M))@200 → memtable → L0  
T=250: Write Key E@250 → memtable → L0
T=300: Snapshot created @300
T=400: Compaction moves D@100 from L0 → L4
T=500: Read with snapshot @300

Result:
- Tombstone @200 visible to snapshot (200 ≤ 300) ✓
- Key D@100 in L4, covered by tombstone @200 (100 < 200) ✓
- Key E@250 in L0, NOT covered by tombstone @200 (250 > 200) ✓
- LSM invariant: max(L4 seqnos) < max(L0 seqnos) ✓
- When D@100 was in L0, tombstone @200 was also in L0 (same level check)
- After D@100 moved to L4, it satisfies cross-level invariant
```

The key insight: **seqnos are timestamps of when keys were WRITTEN, not where they currently LIVE**.

#### 3. Sentinel Keys (Correctness Fix)

**The Problem:**
```
SST File "file123.sst" at Level 2:
  - Point keys: A, B, C
  - Range tombstone: [C, K) @ seq=100
  - File boundary: smallest="A", largest="K"

OLD BUG:
  1. Iterator returns A, B, C
  2. No more point keys → close file, open next file
  3. Tombstone [C, K) is LOST!
  4. Keys D-J from Level 3 incorrectly returned (BUG!)
```

**The Solution:**
*   File boundaries treated as special **sentinel keys**
*   LevelIterator returns sentinel at boundary: `"K" [DELETE_RANGE_SENTINEL]`
*   MergingIterator keeps tombstone active until sentinel is processed
*   Only then does iterator advance to next file

**Why Essential:**
*   Common pattern: `DeleteRange("user:1000", "user:9999")` spans multiple files
*   Time-series data with range deletes across date boundaries
*   Prevents returning deleted data (critical for correctness)

#### 4. Single Unified Heap (Architectural Simplification)

**Before (2018-2022):**
*   MergingIterator with minHeap (point keys only)
*   RangeDelAggregator with:
    *   ForwardRangeDelIterator (2 heaps: active + inactive)
    *   ReverseRangeDelIterator (2 heaps: active + inactive)
*   **Total: 5 separate heaps/data structures!**

**After (2022):**
*   MergingIterator with:
    *   minHeap (all three types)
    *   `active_` set (simple `std::set<size_t>`)
*   **Total: 1 heap + 1 set!**

**Benefits:**
*   Single pass through data
*   No external function calls
*   Better cache locality
*   Simpler state management
*   Clear invariants, all logic in one place

### Complete Example: All Innovations Working Together

**Scenario: Scan with Snapshot @300**

**Database State:**
```
Level 0: Range tombstones [C, M)@200, [X, Z)@350
Level 2: Keys B@50, D@80, E@70, F@60, G@90, H@75, K@85, N@65
Level 4: Keys A@10, C@20, E@15, J@25, L@30, M@35, O@40, X@120, Y@130
```

**Step 0: Snapshot Filtering (Before MergingIterator sees anything)**

```
TruncatedRangeDelIterator filtering with snapshot @300:

Tombstone [C, M)@200:
  - Check: 200 ≤ 300? YES
  - Status: VISIBLE to snapshot → exposed to MergingIterator
  
Tombstone [X, Z)@350:
  - Check: 350 ≤ 300? NO
  - Status: INVISIBLE to snapshot → filtered out, NOT exposed to MergingIterator

Result: MergingIterator only sees [C, M)@200
```

**Now the MergingIterator operates on filtered tombstones:**

**Heap Evolution:**

**Step 1:** After `Seek("A")`
```
minHeap:
  1. A@10 [ITERATOR] Level 4          ← Returned to user
  2. B@50 [ITERATOR] Level 2
  3. C@200 [DELETE_RANGE_START] L0    ← Tombstone start
active_ = {}
```

**Step 2:** After `Next()` from A
```
minHeap:
  1. B@50 [ITERATOR] Level 2          ← Returned to user
  2. C@200 [DELETE_RANGE_START] L0
active_ = {}
```

**Step 3:** After `Next()` from B - **Cascading Seek Triggered!**
```
Pop C@200 [DELETE_RANGE_START] → Level 0 enters active_

minHeap:
  1. C@20 [ITERATOR] Level 4          ← Check: covered?
  2. D@80 [ITERATOR] Level 2
  3. M@200 [DELETE_RANGE_END] L0      ← End now in heap
active_ = {0}

Check C@20:
  - Level 0 in active_? YES
  - C@20 is from Level 4, tombstone [C,M)@200 is from Level 0
  - LSM invariant: i < current->level (0 < 4)
  - Therefore: ALL Level 4 keys have seqno < 200 → DEFINITELY COVERED!
  - NO need to check C's sequence number individually
  
CASCADING SEEK:
  → Level 4: Seek(M)  [skips C@20, E@15, J@25, L@30]
  → Level 2: Seek(M)  [skips D@80, E@70, F@60, G@90, H@75, K@85]
  
  Why safe to seek? Because:
  - LSM invariant guarantees: Level 0 keys have higher seqnos than Level 2/4 keys
  - Within [C,M), the tombstone @200 will cover ALL keys from Level 2 and Level 4
  - Instead of checking each key's seqno, jump directly to M
  
10 keys skipped in ONE operation!
```

**Step 4:** After cascading seek
```
minHeap:
  1. M@200 [DELETE_RANGE_END] L0      ← Pop this
  2. M@35 [ITERATOR] Level 4
  3. N@65 [ITERATOR] Level 2

Pop M@200 [DELETE_RANGE_END] → Level 0 leaves active_
active_ = {}
```

**Step 5:** Tombstone ended
```
minHeap:
  1. M@35 [ITERATOR] Level 4          ← Returned to user
  2. N@65 [ITERATOR] Level 2
active_ = {}
```

**Step 6:** Continue scanning - X and Y
```
minHeap:
  1. N@65 [ITERATOR] Level 2          ← Returned to user
  2. O@40 [ITERATOR] Level 4
  3. X@120 [ITERATOR] Level 4

After N@65, return O@40
After O@40, check X@120:
  - Is there active tombstone for X? NO
  - Tombstone [X,Z)@350 was filtered out by snapshot filtering
  - X@120 < snapshot @300, so X@120 is VISIBLE
  - Return X@120 to user ✓
  
Similarly, Y@130 is returned
```

**Result:**
*   **Keys returned:** A@10, B@50, M@35, N@65, O@40, X@120, Y@130
*   **Keys skipped by cascading seek:** C@20, D@80, E@70, E@15, F@60, G@90, H@75, J@25, K@85, L@30 (10 keys)
*   **Keys NOT deleted (snapshot filtering):** X@120, Y@130 (tombstone @350 invisible to snapshot @300)
*   **Old behavior:** Would check each of 10 keys individually
*   **New behavior:** One cascading seek skipped all 10!

### Testing Strategy for PR #10449

This was a massive architectural change. Here's how correctness was validated:

#### 1. Iterator Verification Framework ([PR #10538](https://github.com/facebook/rocksdb/pull/10538))

**New test:** `TestIterateAgainstExpected()`
*   Locks a range of keys (prevents concurrent modifications)
*   Builds "expected state" from database (all visible keys + values + order)
*   Creates iterator and performs random `Next()`/`Prev()` operations
*   Compares **every key/value** against expected state
*   Performs full forward scan + full backward scan
*   Any mismatch = test fails immediately

**Why this catches range tombstone bugs:**
*   Sentinel key failures → wrong keys returned
*   Cascading seek errors → wrong order
*   `active_` tracking bugs → deleted keys appear
*   **Snapshot filtering bugs** → keys deleted that should be visible

**Snapshot-specific test cases:**
*   Tests with multiple snapshots at different seqnos
*   Verifies tombstones only affect keys with seqno < tombstone seqno
*   Verifies tombstones invisible to snapshot don't affect visible keys
*   Ensures cascading seek + snapshot filtering work correctly together

**How to run:**
```bash
db_stress --verify_iterator_with_expected_state_one_in=1 \
          --iterpercent=44 \
          --delrangepercent=2
```

#### 2. Extensive Stress Testing

**Configuration:**
```bash
python3 ./tools/db_crashtest.py blackbox --simple \
  --write_buffer_size=524288 \
  --target_file_size_base=524288 \
  --max_bytes_for_level_base=2097152 \
  --compression_type=none \
  --max_background_compactions=8 \
  --value_size_mult=33 \
  --max_key=5000000 \
  --duration=7200 \                           # 2 hours!
  --delrangepercent=3 \                       # 3% DeleteRange ops
  --iterpercent=25 \                          # 25% iterator ops
  --range_deletion_width=100 \                # Wide tombstones
  --verify_iterator_with_expected_state_one_in=1
```

**Testing profile:**
*   Heavy mix of operations over 2+ hours
*   Many range tombstones (where bugs hide)
*   Wide tombstones (stress cascading seeks)
*   Continuous validation
*   Thousands of hours of cumulative testing

#### 3. Performance Benchmarks

**Setup:** 5M keys + 10K range tombstones (written after 4.5M keys)

**Benchmarks run:**
```bash
# Full DB scan
db_bench_main --benchmarks=readseq[-X5] --num=5000000

# Short range scans (10 Next calls)
db_bench_main --benchmarks=seekrandom[-X5] --seek_nexts=10

# Long range scans (1000 Next calls)  
db_bench_main --benchmarks=seekrandom[-X5] --seek_nexts=1000
```

**Results:**
*   Narrow tombstones: 10-20% improvement
*   Medium tombstones: 2-5x improvement
*   Wide tombstones: 10-50x improvement

#### 4. Rigorous Code Review

**PR #10449 review process:**
*   38 discussion comments
*   52 code review comments
*   19 commits (iterations on feedback)
*   Multiple reviewers from Meta's RocksDB team
*   Internal Phabricator review

**Review focus:**
*   Correctness of invariants
*   Edge cases in tombstone handling
*   Performance regression risks
*   Code maintainability

### Summary: Why These Innovations Matter

**The Four Innovations Work Together:**

1. **Three Heap Item Types**
   *   ✓ Simpler: One data structure instead of multiple
   *   ✓ Efficient: Tombstone boundaries naturally sorted
   *   ✓ Elegant: State driven by heap operations

2. **Cascading Seeks** ⭐ **(Biggest Performance Win!)**
   *   ✓ 10-50x faster for wide tombstones
   *   ✓ **Relies on LSM level invariant**: keys in Level L have higher seqnos than keys in Level >L
   *   ✓ **Relies on snapshot filtering**: only tombstones ≤ snapshot seqno are visible
   *   ✓ Skip thousands of keys in single operation without individual seqno checks (cross-level case)
   *   ✓ **Critical assumptions**: The LSM invariant must hold AND snapshot filtering must work correctly

3. **Sentinel Keys**
   *   ✓ Correctness: Prevents losing tombstones at file boundaries
   *   ✓ Essential for multi-file range tombstones
   *   ✓ Fixed subtle bug that could return deleted data

4. **Single Unified Heap**
   *   ✓ Simpler architecture: 1 heap vs 5 data structures
   *   ✓ Better performance: No external function calls
   *   ✓ Maintainable: Clear invariants, all logic in one place

**The Complete Redesign:**
*   FASTER: 10-50x for wide tombstones
*   SIMPLER: Unified architecture
*   CORRECT: No lost tombstones
*   MAINTAINABLE: Clear code structure
*   **ASSUMES: LSM level invariant holds**

**Why It Took 4 Years (2018-2022):**
*   2019: First attempt failed (too complex)
*   2020: Second attempt incomplete (design issues)
*   2022: Success! (right architecture + right place + discovered LSM invariant)

The breakthrough: Realizing that:
1. Range tombstone boundaries ARE keys and should be in the SAME heap with point keys
2. The LSM level invariant allows skipping sequence number checks for cross-level coverage

**Real-World Impact:**
*   Time-series data (delete old data)
*   Multi-tenant systems (delete tenant data)
*   Session management (delete expired sessions)
*   Any workload with heavy `DeleteRange()` usage sees dramatic scan improvements

**Critical Dependencies:**

The cascading seek optimization **absolutely depends** on TWO mechanisms working together:

1. **LSM level invariant** - ensures keys in Level L have higher seqnos than keys in Level >L
2. **Snapshot filtering at iterator layer** - ensures only tombstones ≤ snapshot seqno are visible

Together they ensure:
- Cross-level cascading seeks are safe without per-key seqno checks
- Tombstones don't incorrectly delete keys written after them (within snapshot view)
- The three sequence numbers (snapshot, tombstone, key) are properly considered

Operations that could break either mechanism (incorrect file ingestion, compaction bugs, manual file manipulation, broken snapshot filtering) would cause the merging iterator to silently return incorrect results by skipping visible keys or deleting keys that should be visible to the snapshot.

### Example: Range Tombstone `[B, F) @ Seq 100` at Level 0

**Note:** This example assumes the tombstone passed snapshot filtering (i.e., 100 ≤ snapshot seqno).

```
Heap operations:
1. Pop: DELETE_RANGE_START for [B, F) → Level 0 enters active_
2. Pop: Key C @ Seq 50 from Level 2
   → Check: Is Level 0 in active_? Yes
   → Check: 50 < 100? Yes → SKIP Key C
3. Pop: Key F @ Seq 30 from Level 2
   → Still active at this point, check coverage
4. Pop: DELETE_RANGE_END for [B, F) → Level 0 leaves active_
5. Pop: Key G @ Seq 20 from Level 2 → Not covered → EMIT
```

---

## 4. The Full Range Scan Process
The `MergingIterator` produces a filtered stream of visible keys. The `DBIter` (a wrapper) consumes this stream and applies additional logic.

### Step-by-Step Loop (`DBIter::Next`)

1.  **Pick Candidate (MergingIterator):**
    *   The Heap picks the globally smallest key `K` from all connected levels
    *   **Range tombstones are already processed** - deleted keys are skipped internally
    *   *Example:* `Key A @ Seq 50` (already verified not covered by range tombstones)

2.  **Check Duplicates (Shadowing):**
    *   If the previous key we emitted to the user was also `Key A` (but with a higher seqnum, e.g., 100), this current `Key A @ 50` is a **Shadow**
    *   **Action:** Discard `A @ 50`. It is an old version

3.  **Check Point Deletion:**
    *   Is `A @ 50` actually a Tombstone (`Type=Delete`)?
    *   **Action:** Discard it. (We don't return deleted keys to the user)

4.  **Emit:**
    *   If it passes all checks, return `Key A` to the user

### Note on RangeDelAggregator
`RangeDelAggregator` classes still exist but are primarily used for:
*   **Compaction:** `CompactionRangeDelAggregator` manages tombstones across snapshot boundaries
*   **Legacy code paths:** Some older APIs may still use `ReadRangeDelAggregator`
*   **Special operations:** Like `IsRangeOverlapped()` queries

### Summary: Why These Innovations Matter

**The Four Innovations Work Together:**

1. **Three Heap Item Types**
   *   ✓ Simpler: One data structure instead of multiple
   *   ✓ Efficient: Tombstone boundaries naturally sorted
   *   ✓ Elegant: State driven by heap operations

2. **Cascading Seeks** ⭐ **(Biggest Performance Win!)**
   *   ✓ 10-50x faster for wide tombstones
   *   ✓ Uses source precedence + sequence number rules to skip checking covered keys
   *   ✓ Skip thousands of keys in single operation

3. **Sentinel Keys**
   *   ✓ Correctness: Prevents losing tombstones at file boundaries
   *   ✓ Essential for multi-file range tombstones
   *   ✓ Fixed subtle bug that could return deleted data

4. **Single Unified Heap**
   *   ✓ Simpler architecture: 1 heap vs 5 data structures
   *   ✓ Better performance: No external function calls
   *   ✓ Maintainable: Clear invariants, all logic in one place

**The Complete Redesign:**
*   FASTER: 10-50x for wide tombstones
*   SIMPLER: Unified architecture
*   CORRECT: No lost tombstones
*   MAINTAINABLE: Clear code structure

**Why It Took 4 Years (2018-2022):**
*   2019: First attempt failed (too complex)
*   2020: Second attempt incomplete (design issues)
*   2022: Success! (right architecture + right place)

The breakthrough: Realizing range tombstone boundaries ARE keys and should be in the SAME heap with point keys.

**Real-World Impact:**
*   Time-series data (delete old data)
*   Multi-tenant systems (delete tenant data)
*   Session management (delete expired sessions)
*   Any workload with heavy `DeleteRange()` usage sees dramatic scan improvements

### Further Reading
*   **Commit 30bc495c0** (Sept 2, 2022): Run `git show 30bc495c0` for the **authoritative source** - includes full commit message explaining the LSM level invariant, complete implementation, and performance benchmarks
*   [Original DeleteRange blog post (2018)](http://rocksdb.org/blog/2018/11/21/delete-range.html) - Introduction of range tombstones
*   [PR #10449 Discussion](https://github.com/facebook/rocksdb/pull/10449) - The 2022 implementation with benchmarks
*   [PR #10538](https://github.com/facebook/rocksdb/pull/10538) - Iterator verification testing framework added after #10449
*   [Pebble's merging_iter.go](https://github.com/cockroachdb/pebble/blob/master/merging_iter.go) - Inspiration for the approach

### Key Takeaways: The Three Sequence Numbers

**Critical Understanding for Snapshot-Based Scans:**

When a read uses a snapshot, there are **three sequence numbers** that determine whether a key is deleted:

1. **Snapshot Sequence Number** (e.g., @300)
   - Set when snapshot is created
   - Defines "point in time" view of database
   - Passed to iterators via `SetRangeDelReadSeqno()`

2. **Tombstone Sequence Number** (e.g., @200)
   - When the DeleteRange was written
   - Must be ≤ snapshot seqno to be visible
   - Filtered by `FragmentedRangeTombstoneIterator::SetMaxVisibleSeqAndTimestamp()`

3. **Key Sequence Number** (e.g., @250)
   - When the point key was written
   - Must be < tombstone seqno to be deleted
   - Checked in `MergingIterator::SkipNextDeleted()` for same-level case

**The Two-Layer Architecture:**

```
┌──────────────────────────────────────────────────────┐
│ Layer 1: Iterator Layer (Snapshot Filtering)        │
│ ──────────────────────────────────────────────────── │
│ TruncatedRangeDelIterator + FragmentedRangeTombstone│
│ - Filters tombstones: only expose seqno ≤ snapshot  │
│ - Code: range_tombstone_fragmenter.h line 338-357   │
└──────────────────────────────────────────────────────┘
                        ↓
              Only visible tombstones
                        ↓
┌──────────────────────────────────────────────────────┐
│ Layer 2: MergingIterator Layer (LSM Invariant)      │
│ ──────────────────────────────────────────────────── │
│ MergingIterator                                      │
│ - Cross-level: no seqno check (LSM invariant)       │
│ - Same-level: checks key.seqno < tombstone.seqno    │
│ - Code: merging_iterator.cc line 1028-1075          │
└──────────────────────────────────────────────────────┘
```

**Why This Matters:**

The cascading seek optimization in MergingIterator **assumes** that only valid tombstones (filtered by snapshot) are visible. If the iterator layer's snapshot filtering is broken, MergingIterator would incorrectly delete keys that should be visible to the snapshot, leading to **silent data loss**.

Both layers must work correctly together for correctness.

---

## 5. Other Key Iterators
The [wiki](https://github.com/facebook/rocksdb/wiki/Iterator-Implementation) mentions two other critical iterators used as "Children" by the Merging Iterator:

### A. MemtableIterator (`db/memtable.cc`)
*   Wraps the specific data structure of the Memtable (SkipList, HashLinkList, etc.).
*   Exposes the in-memory keys as a sorted stream.

### B. TwoLevelIterator (`table/two_level_iterator.cc`)
*   Used to read **SST Files**.
*   **Why Two Levels?** SST files are huge. We can't load all keys into memory.
*   **Level 1 (Index Block):** A small index that tells us which Data Block contains our key range.
*   **Level 2 (Data Block):** The actual block containing the Key/Values.
*   **Process:** To find `Key 8`:
    1.  L1 Iterator checks Index: *"Key 8 is in Block 2 (Offset 0x0250)"*.
    2.  L2 Iterator loads Block 2 and scans for `Key 8`.


## 6. ALGO details

### Invariant 3 and 4

```c++
// (3) For all level i and j <= i, range_tombstone_iters_[j].prev.end_key() <
// children_[i].iter.key(). That is, range_tombstone_iters_[j] is at or before
// the first range tombstone from level j with end_key() >
// children_[i].iter.key().
// (4) For all level i and j <= i, if j in active_, then
// range_tombstone_iters_[j]->start_key() < children_[i].iter.key().
```

Invariant 3, invariant 4 have two level involved i, and j, where i can be any level of all level, and j <= i.

These two invariants are the mathematical "proof" that allows RocksDB to efficiently check if a key is deleted without scanning the entire history of range tombstones every time.
To understand them, we first need to define the roles of i, j, and the condition j <= i.

#### 1. The Roles of i and j

In the MergingIterator, we are merging multiple "children" (sorted runs, like Memtables or SST files). They are indexed from 0 to n-1.

- **i (The Victim Candidate)**: This is the index of the child that currently holds the minimum key in the heap.
   Example: children_[i].iter.key() is the specific point key (e.g., "user1") we are trying to return to the user.
- **j (The Potential Killer)**: This is the index of a child that contains range tombstones.
   Example: range_tombstone_iters_[j] might hold a deletion range [user0, user5).

#### 2. The Meaning of j <= i (The Shadowing Rule)

In RocksDB's MergingIterator, index 0 typically represents the newest data (e.g., Memtable), and higher indices represent older data (e.g., L0, L1, etc.).

- **LSM Tree Rule**: A deletion (tombstone) can only delete data that is older than or equal to itself.
- j <= i: This condition means level j is newer (or the same age) as level i.
   - If j > i, the tombstone at j is older than the key at i, so it cannot delete it.
   - Therefore, when checking if the key at level i is deleted, we only care about range tombstones at levels j where j <= i.

---

#### 3. Invariant (3): "History is History"

> Text: range_tombstone_iters_[j].prev.end_key() < children_[i].iter.key()

**Translation**: "Any range tombstone at level j that we have already finished processing must have ended strictly before the current point key at level i."

**Why this is critical**: It guarantees that we never have to look backwards.

- Imagine we are at key "Apple" in level i.
- Invariant (3) promises that if level j had a tombstone [Aardvark, Ant), which we already scrolled past, it definitely ended before "Apple".
- This means we only need to check the current state of range_tombstone_iters_[j] (or the active_ set). We don't need to keep a history of past tombstones to see if one of them might still cover "Apple".

---

#### 4. Invariant (4): "Active means Started"

> Text: if j in active_, then range_tombstone_iters_[j]->start_key() < children_[i].iter.key()

**Translation**: "If we think level j is currently 'active' (inside a deletion range), that range must have started strictly before the current point key at level i."

**Why this is critical**: It validates the heap ordering.

- The MergingIterator uses a min-heap. We process keys in strict ascending order.
- If level j is active_, it means we popped its start_key from the heap earlier.
- Since children_[i].iter.key() is currently at the top of the heap, it must be larger than that previously popped start_key.
- Implication: To check if the key at i is deleted by an active level j, we only need to check if key < end_key. We don't need to check start_key <= key because Invariant (4) guarantees it is already true.

---

#### Summary: How they work together

When the iterator holds a key at level i, it needs to answer: **"Is this key deleted?"**

Thanks to these invariants, the logic is incredibly simple:

1. We only look at levels j <= i (Newer levels).
2. We check the active_ set.
3. If j is in active_:
   - Invariant (4) says the deletion started before our key.
   - We just check: Does it end after our key? If yes -> Deleted.
4. If j is NOT in active_:
   - The current tombstone at j hasn't started yet (because if it started before our key, it would be active or finished).
   - Invariant (3) says any previous tombstone ended before our key.
   - Therefore -> Not Deleted (by level j).

This allows RocksDB to determine visibility by simply checking the active_ set for levels 0 to i, without complex interval searches.

### Let `k` be the smallest key among `children_[i].iter.key()`, `k <= children_[i].iter.key() <= LevelNextVisible(i, k)`

This inequality describes the state of the iterators at a specific point during the Next() operation, specifically regarding how point iterators relate to the "current" position and range tombstones.

- `k`: This represents the current "frontier" of the merge operation. It is defined as the smallest key among all the child iterators (children_).
- `LevelNextVisible(i, k)`: This is a theoretical value defined as the first key in level i that is greater than or equal to `k` and is **not** covered (deleted) by any range tombstone.

The inequality `k <= children_[i].iter.key() <= LevelNextVisible(i, k)` asserts two things for every level i:

1. `k <= children_[i].iter.key()`:
Since k is the minimum of all child keys, every specific child's key must be greater than or equal to k. This is a standard property of the min-heap used in merging.

2. `children_[i].iter.key() <= LevelNextVisible(i, k)`:
- This is the critical part. It means the iterator at level `i` is positioned at or before the next valid (non-deleted) key for that level.
- If **equal (=)**: The iterator is currently pointing to valid data that should be returned to the user eventually.
- If strictly **less (<)**: The iterator is pointing to a key that is physically present in the SST file but is covered by a range tombstone (deleted).

Why this invariant matters: It guarantees that no iterator has "overshot" the next visible data. The function `FindNextVisibleKey()` relies on this state to safely advance iterators that are currently pointing at deleted keys (the `<` case) until they reach the `LevelNextVisible` (the `=` case), without missing any valid data.

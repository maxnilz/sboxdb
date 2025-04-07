use super::*;
use crate::error::Error;
use crate::error::Result;

#[test]
/// Begin should create txns with new versions and current active sets.
fn begin() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("begin")?;

    let t1 = mvcc.begin()?;
    assert_eq!(
        t1.state(),
        TransactionState { version: 1, read_only: false, active: HashSet::new() }
    );

    let t2 = mvcc.begin()?;
    assert_eq!(
        t2.state(),
        TransactionState { version: 2, read_only: false, active: HashSet::from([1]) }
    );

    let t3 = mvcc.begin()?;
    assert_eq!(
        t3.state(),
        TransactionState { version: 3, read_only: false, active: HashSet::from([1, 2]) }
    );

    t2.commit()?; // commit to remove from active set

    let t4 = mvcc.begin()?;
    assert_eq!(
        t4.state(),
        TransactionState { version: 4, read_only: false, active: HashSet::from([1, 3]) }
    );

    Ok(())
}

#[test]
/// Begin read-only should not create a new version, instead using the
/// next one, but it should use the current active set.
fn begin_read_only() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("begin_read_only")?;

    // Start an initial read-only transaction, and make sure it's actually
    // read-only.
    let t1 = mvcc.begin_read_only()?;
    assert_eq!(
        t1.state(),
        TransactionState { version: 1, read_only: true, active: HashSet::new() }
    );
    assert_eq!(t1.set(b"foo", vec![1]), Err(Error::ReadOnly));
    assert_eq!(t1.delete(b"foo"), Err(Error::ReadOnly));

    // Start a new read-write transaction, then another read-only
    // transaction which should have it in its active set. t1 should not be
    // in the active set, because it's read-only.
    let t2 = mvcc.begin()?;
    assert_eq!(
        t2.state(),
        TransactionState { version: 1, read_only: false, active: HashSet::new() }
    );

    let t3 = mvcc.begin_read_only()?;
    assert_eq!(
        t3.state(),
        TransactionState { version: 2, read_only: true, active: HashSet::from([1]) }
    );

    Ok(())
}

/// Asserts that a scan yields the expected result.
macro_rules! assert_scan {
    // macro pattern captures
    ($scan: expr => {$( $key: expr => $value: expr ),* $(,)?}) => {
        // macro expansion
        let result = $scan.to_vec()?;
        let expect = vec![
            $( ($key.to_vec(), $value.to_vec()), )*
        ];
        assert_eq!(expect, result);
    }
}

#[test]
/// Begin as of should provide a read-only view of a historical version.
fn begin_as_of() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("begin_as_of")?;

    // Start a concurrent transaction that should be invisible.
    let t1 = mvcc.begin()?;
    t1.set(b"other", vec![1])?;

    // Write a couple of versions for a key.
    let t2 = mvcc.begin()?;
    t2.set(b"key", vec![2])?;
    t2.commit()?;

    let t3 = mvcc.begin()?;
    t3.set(b"key", vec![3])?;

    // Reading as of version 3 should only see key=2, because t1 and
    // t3 haven't committed yet.
    let t4 = mvcc.begin_as_of(3)?;
    assert_eq!(
        t4.state(),
        TransactionState { version: 3, read_only: true, active: HashSet::from([1]) }
    );
    assert_scan!(t4.scan(..)? => {b"key" => vec![2]});

    // Writes should error.
    assert_eq!(t4.set(b"foo", vec![1]), Err(Error::ReadOnly));
    assert_eq!(t4.delete(b"foo"), Err(Error::ReadOnly));

    // Once we commit t1 and t3, neither the existing as of transaction nor
    // a new one should see their writes, since versions must be stable.
    t1.commit()?;
    t3.commit()?;

    assert_scan!(t4.scan(..)? => {b"key" => [2]});

    let t5 = mvcc.begin_as_of(3)?;
    assert_scan!(t5.scan(..)? => {b"key" => [2]});

    // Rolling back and committing read-only transactions is noops.
    t4.rollback()?;
    t5.commit()?;

    // Commit a new value.
    let t6 = mvcc.begin()?;
    t6.set(b"key", vec![4])?;
    t6.commit()?;

    // A snapshot as of version 4 should see key=3 and other=1.
    let t7 = mvcc.begin_as_of(4)?;
    assert_eq!(
        t7.state(),
        TransactionState { version: 4, read_only: true, active: HashSet::new() }
    );
    assert_scan!(t7.scan(..)? => {b"key" => [3], b"other" => [1]});

    // Check that future versions are invalid, including the next.
    assert_eq!(mvcc.begin_as_of(5).err(), Some(Error::Value("Version 5 does not exists".into())));
    assert_eq!(mvcc.begin_as_of(9).err(), Some(Error::Value("Version 9 does not exists".into())));

    Ok(())
}

#[test]
/// Deletes should work on both existing, missing, and deleted keys, be
/// idempotent.
fn delete() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("delete")?;
    mvcc.setup(vec![(b"key", 1, Some(&[1])), (b"tombstone", 1, None)])?;

    let t1 = mvcc.begin()?;
    t1.set(b"key", vec![2])?;
    t1.delete(b"key")?; // delete uncommitted version
    t1.delete(b"key")?; // idempotent
    t1.delete(b"tombstone")?;
    t1.delete(b"missing")?;

    Ok(())
}

#[test]
/// Delete should return serialization errors both for uncommitted versions
/// (past and future), and future committed versions.
fn delete_conflict() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("delete_conflict")?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;
    let t4 = mvcc.begin()?;

    t1.set(b"a", vec![1])?;
    t3.set(b"c", vec![3])?;
    t4.set(b"d", vec![4])?;
    t4.commit()?;

    assert_eq!(t2.delete(b"a"), Err(Error::Serialization));
    assert_eq!(t2.delete(b"c"), Err(Error::Serialization));
    assert_eq!(t2.delete(b"d"), Err(Error::Serialization));

    Ok(())
}

#[test]
/// Get should return the correct latest value.
fn get() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("get")?;
    mvcc.setup(vec![
        (b"foo", 1, Some(&[1])),
        (b"key", 1, Some(&[1])),
        (b"updated", 1, Some(&[1])),
        (b"updated", 2, Some(&[2])),
        (b"deleted", 1, Some(&[1])),
        (b"deleted", 2, None),
        (b"tombstone", 1, None),
    ])?;

    let t1 = mvcc.begin()?;
    t1.set(b"foo", vec![2])?;

    mvcc.print_temp_goldenfile(Some("gold.tmp"))?;

    assert_eq!(t1.get(b"foo")?, Some(vec![2])); // read my own write
    assert_eq!(t1.get(b"key")?, Some(vec![1]));
    assert_eq!(t1.get(b"updated")?, Some(vec![2]));
    assert_eq!(t1.get(b"deleted")?, None);
    assert_eq!(t1.get(b"tombstone")?, None);

    Ok(())
}

#[test]
/// Get should be isolated from future and uncommitted transactions.
fn get_isolation() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("get_isolation")?;

    let t1 = mvcc.begin()?;
    t1.set(b"a", vec![1])?;
    t1.set(b"b", vec![1])?;
    t1.set(b"d", vec![1])?;
    t1.set(b"e", vec![1])?;
    t1.commit()?;

    let t2 = mvcc.begin()?;
    t2.set(b"a", vec![2])?;
    t2.delete(b"b")?;
    t2.set(b"c", vec![2])?;

    let t3 = mvcc.begin_read_only()?;

    let t4 = mvcc.begin()?;
    t4.set(b"d", vec![3])?;
    t4.delete(b"e")?;
    t4.set(b"f", vec![3])?;
    t4.commit()?;

    assert_eq!(t3.get(b"a")?, Some(vec![1])); // uncommited update
    assert_eq!(t3.get(b"b")?, Some(vec![1])); // uncommited delete
    assert_eq!(t3.get(b"c")?, None); // uncommited write
    assert_eq!(t3.get(b"d")?, Some(vec![1])); // future write
    assert_eq!(t3.get(b"e")?, Some(vec![1])); // future delete
    assert_eq!(t3.get(b"f")?, None); // future write

    Ok(())
}

#[test]
/// Scans should use correct key and time bounds. Sets up an initial data
/// set as follows, and asserts results via the golden file.
///
/// T
/// 4             x   ba,4
/// 3   x   a,3  b,3        x
/// 2        x        ba,2 bb,2 bc,2
/// 1  0,1  a,1   x                  c,1
///     B    a    b    ba   bb   bc   c
fn scan() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("scan")?;
    mvcc.setup(vec![
        (b"B", 1, Some(&vec![0, 1])),
        (b"B", 3, None),
        (b"a", 1, Some(&vec![0x0a, 1])),
        (b"a", 2, None),
        (b"a", 3, Some(&vec![0x0a, 3])),
        (b"b", 1, None),
        (b"b", 3, Some(&vec![0x0b, 3])),
        (b"b", 4, None),
        (b"ba", 2, Some(&vec![0xba, 2])),
        (b"ba", 4, Some(&vec![0xba, 4])),
        (b"bb", 2, Some(&vec![0xbb, 2])),
        (b"bb", 3, None),
        (b"bc", 2, Some(&vec![0xbc, 2])),
        (b"c", 1, Some(&vec![0x0c, 1])),
    ])?;

    mvcc.print_temp_goldenfile(Some("gold.tmp"))?;

    // Full scans at all timestamps.
    for version in 1..5 {
        let txn = match version {
            5 => mvcc.begin_read_only()?,
            v => mvcc.begin_as_of(v)?,
        };
        let mut scan = txn.scan(..)?; // see golden master
        assert_scan_invariants(&mut scan)?;
    }

    // All bounded scans around ba-bc at version 3.
    let txn = mvcc.begin_as_of(3)?;
    let starts =
        [Bound::Unbounded, Bound::Included(b"ba".to_vec()), Bound::Excluded(b"ba".to_vec())];
    let ends = [Bound::Unbounded, Bound::Included(b"bc".to_vec()), Bound::Excluded(b"bc".to_vec())];
    for start in &starts {
        for end in &ends {
            let mut scan = txn.scan((start.to_owned(), end.to_owned()))?; // see golden master
            assert_scan_invariants(&mut scan)?;
        }
    }

    Ok(())
}

#[test]
/// Prefix scans should use correct key and time bounds. Sets up an initial
/// data set as follows, and asserts results via the golden file.
///
/// T
/// 4             x   ba,4
/// 3   x   a,3  b,3        x
/// 2        x        ba,2 bb,2 bc,2
/// 1  0,1  a,1   x                  c,1
///     B    a    b    ba   bb   bc   c
fn scan_prefix() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("scan_prefix")?;
    mvcc.setup(vec![
        (b"B", 1, Some(&[0, 1])),
        (b"B", 3, None),
        (b"a", 1, Some(&[0x0a, 1])),
        (b"a", 2, None),
        (b"a", 3, Some(&[0x0a, 3])),
        (b"b", 1, None),
        (b"b", 3, Some(&[0x0b, 3])),
        (b"b", 4, None),
        (b"ba", 2, Some(&[0xba, 2])),
        (b"ba", 4, Some(&[0xba, 4])),
        (b"bb", 2, Some(&[0xbb, 2])),
        (b"bb", 3, None),
        (b"bc", 2, Some(&[0xbc, 2])),
        (b"c", 1, Some(&[0x0c, 1])),
    ])?;

    // Full scans at all timestamps.
    for version in 1..5 {
        let txn = match version {
            5 => mvcc.begin_read_only()?,
            v => mvcc.begin_as_of(v)?,
        };
        let mut scan = txn.scan_prefix(&[])?; // see golden master
        assert_scan_invariants(&mut scan)?;
    }

    // All prefixes at version 3 and version 4.
    for version in 3..=4 {
        let txn = mvcc.begin_as_of(version)?;
        for prefix in [b"B" as &[u8], b"a", b"b", b"ba", b"bb", b"bbb", b"bc", b"c", b"d"] {
            let mut scan = txn.scan_prefix(prefix)?; // see golden master
            assert_scan_invariants(&mut scan)?;
        }
    }

    Ok(())
}

#[test]
/// Scan should be isolated from future and uncommitted transactions.
fn scan_isolation() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("scan_isolation")?;

    let t1 = mvcc.begin()?;
    t1.set(b"a", vec![1])?;
    t1.set(b"b", vec![1])?;
    t1.set(b"d", vec![1])?;
    t1.set(b"e", vec![1])?;
    t1.commit()?;

    let t2 = mvcc.begin()?;
    t2.set(b"a", vec![2])?;
    t2.delete(b"b")?;
    t2.set(b"c", vec![2])?;

    let t3 = mvcc.begin_read_only()?;

    let t4 = mvcc.begin()?;
    t4.set(b"d", vec![3])?;
    t4.delete(b"e")?;
    t4.set(b"f", vec![3])?;
    t4.commit()?;

    assert_scan!(t3.scan(..)? => {
        b"a" => [1], // uncommited update
        b"b" => [1], // uncommited delete
        // b"c" is uncommited write
        b"d" => [1], // future update
        b"e" => [1], // future delete
        // b"f" is future write
    });

    Ok(())
}

#[test]
/// Tests that the key encoding is resistant to key/version overlap.
/// For example, a naïve concatenation of keys and versions would
/// produce incorrect ordering in this case:
///
// 00|00 00 00 00 00 00 00 01
// 00 00 00 00 00 00 00 00 02|00 00 00 00 00 00 00 02
// 00|00 00 00 00 00 00 00 03
fn scan_key_version_encoding() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("scan_key_version_encoding")?;

    let t1 = mvcc.begin()?;
    t1.set(&[0], vec![1])?;
    t1.commit()?;

    let t2 = mvcc.begin()?;
    t2.set(&[0], vec![2])?;
    t2.set(&[0, 0, 0, 0, 0, 0, 0, 0, 2], vec![2])?;
    t2.commit()?;

    let t3 = mvcc.begin()?;
    t3.set(&[0], vec![3])?;
    t3.commit()?;

    let t4 = mvcc.begin_read_only()?;
    assert_scan!(t4.scan(..)? => {
        b"\x00" => [3],
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x02" => [2],
    });
    Ok(())
}

#[test]
/// Sets should work on both existing, missing, and deleted keys, and be
/// idempotent.
fn set() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("set")?;
    mvcc.setup(vec![(b"key", 1, Some(&[1])), (b"tombstone", 1, None)])?;

    let t1 = mvcc.begin()?;
    t1.set(b"key", vec![2])?; // update
    t1.set(b"tombstone", vec![2])?; // update tombstone
    t1.set(b"new", vec![1])?; // new write
    t1.set(b"new", vec![1])?; // idempotent
    t1.set(b"new", vec![2])?; // update own
    t1.commit()?;

    Ok(())
}

#[test]
/// Set should return serialization errors both for uncommitted versions
/// (past and future), and future committed versions.
fn set_conflict() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("set_conflict")?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;
    let t4 = mvcc.begin()?;

    t1.set(b"a", vec![1])?;
    t3.set(b"c", vec![3])?;
    t4.set(b"d", vec![4])?;
    t4.commit()?;

    assert_eq!(t2.set(b"a", vec![2]), Err(Error::Serialization)); // past uncommitted
    assert_eq!(t2.set(b"c", vec![2]), Err(Error::Serialization)); // future uncommitted
    assert_eq!(t2.set(b"d", vec![2]), Err(Error::Serialization)); // future committed

    Ok(())
}

#[test]
/// Tests that transaction rollback properly rolls back uncommitted writes,
/// allowing other concurrent transactions to write the keys.
fn rollback() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("rollback")?;
    mvcc.setup(vec![
        (b"a", 1, Some(&[0])),
        (b"b", 1, Some(&[0])),
        (b"c", 1, Some(&[0])),
        (b"d", 1, Some(&[0])),
    ])?;

    // t2 will be rolled back. t1 and t3 are concurrent transactions.
    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;

    t1.set(b"a", vec![1])?;
    t2.set(b"b", vec![2])?;
    t2.delete(b"c")?;
    t3.set(b"d", vec![3])?;

    // Both t1 and t3 will get serialization errors with t2.
    assert_eq!(t1.set(b"b", vec![1]), Err(Error::Serialization));
    assert_eq!(t3.set(b"c", vec![3]), Err(Error::Serialization));

    // When t2 is rolled back, none of its writes will be visible, and t1
    // and t3 can perform their writes and successfully commit.
    t2.rollback()?;

    let t4 = mvcc.begin_read_only()?;
    assert_scan!(t4.scan(..)? => {
        b"a" => [0],
        b"b" => [0],
        b"c" => [0],
        b"d" => [0],
    });

    t1.set(b"b", vec![1])?;
    t3.set(b"c", vec![3])?;
    t1.commit()?;
    t3.commit()?;

    let t5 = mvcc.begin_read_only()?;
    assert_scan!(t5.scan(..)? => {
        b"a" => [1],
        b"b" => [1],
        b"c" => [3],
        b"d" => [3],
    });

    Ok(())
}

#[test]
/// A dirty write is when t2 overwrites an uncommitted value written by t1.
/// Snapshot isolation prevents this.
fn anomaly_dirty_write() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("anomaly_dirty_write")?;

    let t1 = mvcc.begin()?;
    t1.set(b"key", vec![1])?;

    let t2 = mvcc.begin()?;
    assert_eq!(t2.set(b"key", vec![2]), Err(Error::Serialization));

    Ok(())
}

#[test]
/// A dirty read is when t2 can read an uncommitted value set by t1.
/// Snapshot isolation prevents this.
fn anomaly_dirty_read() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("anomaly_dirty_read")?;

    let t1 = mvcc.begin()?;
    t1.set(b"key", vec![1])?;

    let t2 = mvcc.begin()?;
    assert_eq!(t2.get(b"key")?, None);

    Ok(())
}

#[test]
/// A lost update is when t1 and t2 both read a value and update it, where
/// t2's update replaces t1. Snapshot isolation prevents this.
fn anomaly_lost_update() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("anomaly_lost_update")?;
    mvcc.setup(vec![(b"key", 1, Some(&[0]))])?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    t1.get(b"key")?;
    t2.get(b"key")?;

    t1.set(b"key", vec![1])?;
    assert_eq!(t2.set(b"key", vec![2]), Err(Error::Serialization));
    t1.commit()?;

    Ok(())
}

#[test]
/// A fuzzy (or unrepeatable) read is when t2 sees a value change after t1
/// updates it. Snapshot isolation prevents this.
fn anomaly_fuzzy_read() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("anomaly_fuzzy_read")?;
    mvcc.setup(vec![(b"key", 1, Some(&[0]))])?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    assert_eq!(t2.get(b"key")?, Some(vec![0]));
    t1.set(b"key", b"t1".to_vec())?;
    t1.commit()?;
    assert_eq!(t2.get(b"key")?, Some(vec![0]));

    Ok(())
}

#[test]
/// Read skew is when t1 reads a and b, but t2 modifies b in between the
/// reads. Snapshot isolation prevents this.
fn anomaly_read_skew() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("anomaly_read_skew")?;
    mvcc.setup(vec![(b"a", 1, Some(&[0])), (b"b", 1, Some(&[0]))])?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    assert_eq!(t1.get(b"a")?, Some(vec![0]));
    t2.set(b"a", vec![2])?;
    t2.set(b"b", vec![2])?;
    t2.commit()?;
    assert_eq!(t1.get(b"b")?, Some(vec![0]));

    Ok(())
}

#[test]
/// A phantom read is when t1 reads entries matching some predicate, but a
/// modification by t2 changes which entries that match the predicate such
/// that a later read by t1 returns them. Snapshot isolation prevents this.
///
/// We use a prefix scan as our predicate.
fn anomaly_phantom_read() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("anomaly_phantom_read")?;
    mvcc.setup(vec![(b"a", 1, Some(&[0])), (b"ba", 1, Some(&[0])), (b"bb", 1, Some(&[0]))])?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    assert_scan!(t1.scan_prefix(b"b")? => {
        b"ba" => [0],
        b"bb" => [0],
    });

    t2.delete(b"ba")?;
    t2.set(b"bc", vec![2])?;
    t2.commit()?;

    assert_scan!(t1.scan_prefix(b"b")? => {
        b"ba" => [0],
        b"bb" => [0],
    });

    Ok(())
}

#[test]
/// Write skew is when t1 reads a and writes it to b while t2 reads b and
/// writes it to a. Snapshot isolation DOES NOT prevent this, which is
/// expected, so we assert the current behavior. Fixing this requires
/// implementing serializable snapshot isolation.
fn anomaly_write_skew() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("anomaly_write_skew")?;
    mvcc.setup(vec![(b"a", 1, Some(&[1])), (b"b", 1, Some(&[2]))])?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    assert_eq!(t1.get(b"a")?, Some(vec![1]));
    assert_eq!(t2.get(b"b")?, Some(vec![2]));

    t1.set(b"b", vec![1])?;
    t2.set(b"a", vec![2])?;

    t1.commit()?;
    t2.commit()?;

    Ok(())
}

#[test]
/// Tests unversioned key/value pairs, via set/get_unversioned().
fn unversioned() -> Result<()> {
    let mut mvcc = MVCCRecorder::new("unversioned")?;

    // Interleave versioned and unversioned writes.
    mvcc.set_unversioned(b"a", vec![0])?;

    let t1 = mvcc.begin()?;
    t1.set(b"a", vec![1])?;
    t1.set(b"b", vec![1])?;
    t1.set(b"c", vec![1])?;
    t1.commit()?;

    mvcc.set_unversioned(b"b", vec![0])?;
    mvcc.set_unversioned(b"d", vec![0])?;

    // Scans should not see the unversioned writes.
    let t2 = mvcc.begin_read_only()?;
    assert_scan!(t2.scan(..)? => {
        b"a" => [1],
        b"b" => [1],
        b"c" => [1],
    });

    // Unversioned gets should not see MVCC writes.
    assert_eq!(mvcc.get_unversioned(b"a")?, Some(vec![0]));
    assert_eq!(mvcc.get_unversioned(b"b")?, Some(vec![0]));
    assert_eq!(mvcc.get_unversioned(b"c")?, None);
    assert_eq!(mvcc.get_unversioned(b"d")?, Some(vec![0]));

    // Replacing an unversioned key should be fine.
    mvcc.set_unversioned(b"a", vec![1])?;
    assert_eq!(mvcc.get_unversioned(b"a")?, Some(vec![1]));

    Ok(())
}

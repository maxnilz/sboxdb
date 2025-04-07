use std::collections::{Bound, HashMap, HashSet, VecDeque};
use std::fs::{read_to_string, File};
use std::io::Write;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::Result;
use crate::storage::kv::codec::bincodec;
use crate::storage::kv::memory::{Memory, OwnedRangeIterator};
use crate::storage::kv::{ScanIterator, Storage};
use crate::storage::mvcc::{Key, Scan, Transaction, TransactionState, Version, MVCC};

mod tests;

struct MVCCRecorder {
    name: String,
    mvcc: MVCC<StorageRecorder<Memory>>,
    #[allow(unused)]
    mint: goldenfile::Mint,
    file: Arc<Mutex<File>>,
    next_id: u8,
}

const GOLDEN_DIR: &str = "src/storage/mvcc/tests/golden";

impl MVCCRecorder {
    fn new(name: &str) -> Result<Self> {
        let memory = Memory::new();
        let storage = StorageRecorder::new(memory);
        let mvcc = MVCC::new(storage);
        let mut mint = goldenfile::Mint::new(GOLDEN_DIR);
        let file = Arc::new(Mutex::new(mint.new_goldenfile(name)?));
        Ok(MVCCRecorder { name: name.to_string(), mvcc, mint, file, next_id: 1 })
    }

    /// Sets up an initial, versioned dataset from the given data as a
    /// vector of key,version,value tuples. These transactions are not
    /// assigned transaction IDs, nor are the writes logged, except for the
    /// initial engine state.
    #[allow(clippy::type_complexity)]
    fn setup(&mut self, data: Vec<(&[u8], Version, Option<&[u8]>)>) -> Result<()> {
        // Segment the writes by version.
        let mut writes = HashMap::new();
        for (key, version, value) in data {
            writes
                .entry(version)
                .or_insert(Vec::new())
                .push((key.to_vec(), value.map(|v| v.to_vec())));
        }
        // Insert the writes with individual transactions.
        for i in 1..=writes.keys().max().copied().unwrap_or(0) {
            let txn = self.mvcc.begin()?;
            for (key, value) in writes.get(&i).unwrap_or(&Vec::new()) {
                if let Some(value) = value {
                    txn.set(key, value.clone())?;
                } else {
                    txn.delete(key)?;
                }
            }
            txn.commit()?;
        }

        // Flush the write log, but dump the kv storage contents.
        self.mvcc.kv.lock()?.clear_write_log();
        self.record_kv_state()?;
        writeln!(&mut self.file.lock()?)?;
        Ok(())
    }

    fn begin(&mut self) -> Result<TransactionRecorder> {
        self.new_txn("begin", self.mvcc.begin())
    }

    fn begin_read_only(&mut self) -> Result<TransactionRecorder> {
        self.new_txn("begin read-only", self.mvcc.begin_read_only())
    }

    fn begin_as_of(&mut self, version: Version) -> Result<TransactionRecorder> {
        self.new_txn(&format!("begin read-only {}", version), self.mvcc.begin_as_of(version))
    }

    fn new_txn(
        &mut self,
        name: &str,
        result: Result<Transaction<StorageRecorder<Memory>>>,
    ) -> Result<TransactionRecorder> {
        let id = self.next_id;
        self.next_id += 1;
        self.record_txn_begin(id, name, &result)?;
        result.map(|txn| TransactionRecorder { id, txn, file: Arc::clone(&self.file) })
    }

    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let value = self.mvcc.get_unversioned(key)?;
        write!(
            self.file.lock()?,
            "T_: get unversioned {} → {}\n\n",
            format_raw(key),
            if let Some(ref value) = value { format_raw(value) } else { String::from("None") }
        )?;
        Ok(value)
    }

    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        write!(
            self.file.lock()?,
            "T_: set unversioned {} = {}",
            format_raw(key),
            format_raw(&value),
        )?;
        let result = self.mvcc.set_unversioned(key, value.clone());
        let mut f = self.file.lock()?;
        match &result {
            Ok(_) => writeln!(f)?,
            Err(err) => writeln!(f, " → Error::{:?}", err)?,
        }
        MVCCRecorder::record_kv_writes(&mut f, &mut self.mvcc.kv.lock()?)?;
        writeln!(f)?;
        result
    }

    fn record_txn_begin(
        &self,
        id: u8,
        name: &str,
        result: &Result<Transaction<StorageRecorder<Memory>>>,
    ) -> Result<()> {
        let mut f = self.file.lock()?;
        // For this to work, we have to have std::io::Write trait in the scope,
        // i.e., place `use std::io::Write` in the scope. because the expanded
        // write! macro would expand to something like this:
        // `f.write_fmt(format_args!("T{0}: {1} → ", id, name))?;` where the write_fmt
        // method is not defined on the f directly, but it can be found in std::io::Write
        // trait, and it is implemented by the File.
        write!(f, "T{}: {} → ", id, name)?;
        match result {
            Ok(txn) => writeln!(f, "{}", txn)?,
            Err(err) => writeln!(f, "Error::{:?}", err)?,
        };
        Self::record_kv_writes(&mut f, &mut self.mvcc.kv.lock()?)?;
        writeln!(f)?;
        Ok(())
    }

    fn record_kv_state(&mut self) -> Result<()> {
        let mut f = self.file.lock()?;
        let kv = self.mvcc.kv.lock()?;
        let mut scan = kv.scan((Bound::Unbounded, Bound::Unbounded));
        writeln!(f, "KV state:")?;
        while let Some((key, value)) = scan.next().transpose()? {
            if let (fkey, Some(fvalue)) = format_key_value(&key, &Some(value)) {
                writeln!(f, "{} = {}", fkey, fvalue)?;
            }
        }
        Ok(())
    }

    /// Log the kv write log since the last call to the golden file.
    fn record_kv_writes(
        f: &mut MutexGuard<'_, File>,
        kv: &mut MutexGuard<'_, StorageRecorder<Memory>>,
    ) -> Result<()> {
        let writes = kv.take_write_log();
        for (key, value) in &writes {
            let (fkey, fvalue) = format_key_value(key, value);
            match fvalue {
                Some(fvalue) => writeln!(f, "    set {} = {}", fkey, fvalue)?,
                None => writeln!(f, "    del {}", fkey)?,
            }
        }
        Ok(())
    }

    fn print_temp_goldenfile(&mut self, to: Option<&str>) -> Result<()> {
        let mut file = self.file.lock()?;
        file.flush()?;

        let path = self.mint.new_goldenpath(&self.name)?;
        match to {
            Some(p) => {
                std::fs::copy(path, p)?;
            }
            None => {
                println!("{}", read_to_string(path)?);
            }
        }
        Ok(())
    }
}

impl Drop for MVCCRecorder {
    fn drop(&mut self) {
        _ = self.record_kv_state();
        // goldenfile assertions run when mint is dropped
    }
}

struct TransactionRecorder {
    id: u8,
    txn: Transaction<StorageRecorder<Memory>>,
    file: Arc<Mutex<std::fs::File>>,
}

impl Clone for TransactionRecorder {
    /// Allow cloning a schedule transaction, to simplify handling when
    /// commit/rollback consumes it. We don't want to allow this in general,
    /// since a commit/rollback will invalidate the cloned transactions.
    fn clone(&self) -> Self {
        let txn = Transaction { kv: self.txn.kv.clone(), st: self.txn.st.clone() };
        TransactionRecorder { id: self.id, txn, file: Arc::clone(&self.file) }
    }
}

impl TransactionRecorder {
    pub fn state(&self) -> TransactionState {
        self.txn.st.clone()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let value = self.txn.get(key)?;

        // record get
        let mut f = self.file.lock()?;
        write!(
            f,
            "T{}: get {} → {}\n\n",
            self.id,
            format_raw(key),
            if let Some(ref value) = value { format_raw(value) } else { String::from("None") }
        )?;

        Ok(value)
    }

    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let result = self.txn.set(key, value.clone());
        self.record_mutation(
            &format!("set {} = {}", format_raw(key), format_raw(&value)),
            &result,
        )?;
        result
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let result = self.txn.delete(key);
        self.record_mutation(&format!("del {}", format_raw(key)), &result)?;
        result
    }

    pub fn commit(self) -> Result<()> {
        let result = self.clone().txn.commit(); // clone to retain self.txn for recording
        self.record_mutation("commit", &result)?;
        result
    }

    pub fn rollback(self) -> Result<()> {
        let result = self.clone().txn.rollback(); // clone to retain self.txn for recording
        self.record_mutation("rollback", &result)?;
        result
    }

    pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Scan<StorageRecorder<Memory>>> {
        let name = format!(
            "scan {}..{}",
            match range.start_bound() {
                Bound::Excluded(k) => format!("({}", format_raw(k)),
                Bound::Included(k) => format!("[{}", format_raw(k)),
                Bound::Unbounded => "".to_string(),
            },
            match range.end_bound() {
                Bound::Excluded(k) => format!("{})", format_raw(k)),
                Bound::Included(k) => format!("{}]", format_raw(k)),
                Bound::Unbounded => "".to_string(),
            },
        );
        let scan = self.txn.scan((range.start_bound().cloned(), range.end_bound().cloned()))?;
        self.record_scan(&name, scan.to_vec()?)?;
        Ok(scan)
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Scan<StorageRecorder<Memory>>> {
        let scan = self.txn.scan_prefix(prefix.to_vec())?;
        self.record_scan(&format!("scan prefix {}", format_raw(prefix)), scan.to_vec()?)?;
        Ok(scan)
    }

    fn record_scan(&self, name: &str, scan: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        let mut f = self.file.lock()?;
        writeln!(f, "T{}: {}", self.id, name)?;
        for (key, value) in scan {
            writeln!(f, "    {} = {}", format_raw(&key), format_raw(&value))?
        }
        writeln!(f)?;
        Ok(())
    }

    fn record_mutation(&self, name: &str, result: &Result<()>) -> Result<()> {
        let mut f = self.file.lock()?;
        write!(f, "T{}: {}", self.id, name)?;
        match result {
            Ok(_) => writeln!(f)?,
            Err(err) => writeln!(f, " → Error::{:?}", err)?,
        }
        MVCCRecorder::record_kv_writes(&mut f, &mut self.txn.kv.lock()?)?;
        writeln!(f)?;
        Ok(())
    }
}

/// A debug kv storage, which wraps the kv storage and logs mutations.
#[derive(Debug)]
struct StorageRecorder<KV: Storage> {
    /// The wrapped kv storage.
    inner: KV,
    /// Write log as key/value tuples. Value is None for deletes.
    write_log: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl<KV: Storage> StorageRecorder<KV> {
    fn new(inner: KV) -> Self {
        StorageRecorder { inner, write_log: Vec::new() }
    }

    /// Returns and resets the write log. The next call only returns new writes.
    fn take_write_log(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut write_log = Vec::new();
        std::mem::swap(&mut write_log, &mut self.write_log);
        write_log
    }

    fn clear_write_log(&mut self) {
        self.write_log.clear()
    }
}

impl<KV: Storage> Storage for StorageRecorder<KV> {
    fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.inner.set(key, value.clone())?;
        self.write_log.push((key.to_vec(), Some(value)));
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn scan(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator<'_> + '_> {
        self.inner.scan(range)
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let res = self.inner.remove(key)?;
        self.write_log.push((key.to_vec(), None));
        Ok(res)
    }

    fn remove_range(
        &mut self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Box<dyn ScanIterator<'_> + '_> {
        let mut scan = self.inner.remove_range(range);
        let mut out: VecDeque<(Vec<u8>, Vec<u8>)> = VecDeque::new();
        while let Some(it) = scan.next() {
            if let Ok((key, value)) = it {
                self.write_log.push((key.clone(), None));
                out.push_back((key, value));
            }
        }
        Box::new(OwnedRangeIterator { it: out })
    }

    fn remove_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let result = self.inner.remove_prefix(prefix)?;
        for (ref key, _) in result.iter() {
            self.write_log.push((key.clone(), None));
        }
        Ok(result)
    }
}

pub fn format_raw(v: &[u8]) -> String {
    if v.is_empty() {
        return String::from("[]");
    }
    if let Ok(s) = String::from_utf8(v.to_vec()) {
        if s.chars().all(|c| !c.is_control()) {
            return format!(r#""{}""#, s);
        }
    }
    format!("0x{}", hex::encode(v))
}

/// Formats a HashSet with sorted elements.
pub fn format_hashset<T: Copy + Ord + std::fmt::Display>(set: &HashSet<T>) -> String {
    let mut elements: Vec<T> = set.iter().copied().collect();
    elements.sort();
    let elements: Vec<String> = elements.into_iter().map(|v| v.to_string()).collect();
    format!("{{{}}}", elements.join(","))
}

/// Formats a raw engine key/value pair, or just the key if the value is None.
/// Attempts to decode known MVCC key formats and values.
fn format_key_value(key: &[u8], value: &Option<Vec<u8>>) -> (String, Option<String>) {
    // Default to string/hex formatting of the raw key and value.
    let mut fkey = format_raw(key);
    let mut fvalue = value.as_ref().map(|v| format_raw(v.as_slice()));

    // Try to decode MVCC keys and values.
    if let Ok(key) = Key::decode(key) {
        // Use the debug formatting of the key, unless we need more.
        fkey = format!("{:?}", key);

        match key {
            Key::NextVersion => {
                if let Some(ref v) = value {
                    if let Ok(v) = bincodec::deserialize::<u64>(v) {
                        fvalue = Some(format!("{}", v))
                    }
                }
            }
            Key::TxnActive(_) => {}
            Key::TxnActiveSnapshot(_) => {
                if let Some(ref v) = value {
                    if let Ok(active) = bincodec::deserialize::<HashSet<Version>>(v) {
                        fvalue = Some(format_hashset(&active));
                    }
                }
            }
            Key::TxnWrite(version, userkey) => {
                fkey = format!("TxnWrite({}, {})", version, format_raw(&userkey))
            }
            Key::Version(userkey, version) => {
                fkey = format!("Version({}, {})", format_raw(&userkey), version);
                if let Some(ref v) = value {
                    match bincodec::deserialize(v) {
                        Ok(Some(v)) => fvalue = Some(format_raw(v)),
                        Ok(None) => fvalue = Some(String::from("None")),
                        Err(_) => {}
                    }
                }
            }
            Key::Unversioned(userkey) => {
                fkey = format!("Unversioned({})", format_raw(&userkey));
            }
        }
    }

    (fkey, fvalue)
}

/// Asserts scan invariants.
#[track_caller]
fn assert_scan_invariants(scan: &mut Scan<StorageRecorder<Memory>>) -> Result<()> {
    // Iterator and vec should yield same results.
    let result = scan.to_vec()?;
    assert_eq!(scan.iter().collect::<Result<Vec<_>>>()?, result);

    // Forward and reverse scans should give the same results.
    let mut forward = result.clone();
    forward.reverse();
    let reverse = scan.iter().rev().collect::<Result<Vec<_>>>()?;
    assert_eq!(reverse, forward);

    // Alternating next/next_back calls should give the same results.
    let mut forward = Vec::new();
    let mut reverse = Vec::new();
    let mut iter = scan.iter();
    while let Some(b) = iter.next().transpose()? {
        forward.push(b);
        if let Some(b) = iter.next_back().transpose()? {
            reverse.push(b);
        }
    }
    reverse.reverse();
    forward.extend_from_slice(&reverse);
    assert_eq!(forward, result);

    Ok(())
}

//! This module implements MVCC (Multi-Version Concurrency Control), a widely
//! used method for ACID transactions and concurrency control. It allows
//! multiple concurrent transactions to access and modify the same dataset,
//! isolates them from each other, detects and handles conflicts, and commits
//! their writes atomically as a single unit. It uses an underlying key-value
//! storage to store raw keys and values.
//!
//! VERSIONS
//! ========
//!
//! MVCC handles concurrency control by managing multiple historical versions of
//! keys, identified by a timestamp. Every write adds a new version at a higher
//! timestamp, with deletes having a special tombstone value. For example, the
//! keys a,b,c,d may have the following values at various logical timestamps (x
//! is tombstone):
//!
//! Time
//! 5
//! 4  a4
//! 3      b3      x
//! 2
//! 1  a1      c1  d1
//!    a   b   c   d   Keys
//!
//! A transaction t2 that started at T=2 will see the values a=a1, c=c1, d=d1. A
//! different transaction t5 running at T=5 will see a=a4, b=b3, c=c1.
//!
//! sboxdb uses logical timestamps with a sequence number stored in
//! Key::NextVersion. Each new read-write transaction takes its timestamp from
//! the current value of Key::NextVersion and then increments the value for the
//! next transaction.
//!
//! ISOLATION
//! =========
//!
//! MVCC provides an isolation level called snapshot isolation. Briefly,
//! transactions see a consistent snapshot of the database state as of their
//! start time. Writes made by concurrent or subsequent transactions are never
//! visible to it. If two concurrent transactions write to the same key they
//! will conflict and one of them must retry. A transaction's writes become
//! atomically visible to subsequent transactions only when they commit, and are
//! rolled back on failure. Read-only transactions never conflict with other
//! transactions.
//!
//! Transactions write new versions at their timestamp, storing them as
//! Key::Version(key, version) => value. If a transaction writes to a key and
//! finds a newer version, it returns an error and the client must retry.
//!
//! Active (uncommitted) read-write transactions record their version in the
//! active set, stored as Key::Active(version) with empty value. When new
//! transactions begin, they take a snapshot of this active set, and any key
//! versions that belong to a transaction in the active set are considered
//! invisible (to anyone except that transaction itself). Writes to keys that
//! already have a past version in the active set will also return an error.
//!
//! To commit, a transaction simply deletes its record in the active set. This
//! will immediately (and, crucially, atomically) make all of its writes visible
//! to subsequent transactions, but not ongoing ones. If the transaction is
//! cancelled and rolled back, it maintains a record of all keys it wrote as
//! Key::TxnWrite(version, key), so that it can find the corresponding versions
//! and delete them before removing itself from the active set.
//!
//! Consider the following example, where we have two ongoing transactions at
//! time T=2 and T=5, with some writes that are not yet committed marked in
//! parentheses.
//!
//! Active set: [2, 5]
//!
//! Time
//! 5 (a5)
//! 4  a4
//! 3      b3      x
//! 2         (x)     (e2)
//! 1  a1      c1  d1
//!    a   b   c   d   e   Keys
//!
//! Here, t2 will see a=a1, d=d1, e=e2 (it sees its own writes). t5 will see
//! a=a5, b=b3, c=c1. t2 does not see any newer versions, and t5 does not see
//! the tombstone at c@2 nor the value e=e2, because version=2 is in its active
//! set.
//!
//! If t2 tries to write b=b2, it receives an error and must retry, because a
//! newer version exists. Similarly, if t5 tries to write e=e5, it receives an
//! error and must retry, because the version e=e2 is in its active set.
//!
//! To commit, t2 can remove itself from the active set. A new transaction t6
//! starting after the commit will then see c as deleted and e=e2. t5 will still
//! not see any of t2's writes, because it's still in its local snapshot of the
//! active set at the time it began.
//!
//! READ-ONLY AND TIME TRAVEL QUERIES
//! =================================
//!
//! Since MVCC stores historical versions, it can trivially support time travel
//! queries where a transaction reads at a past timestamp and has a consistent
//! view of the database at that time.
//!
//! This is done by a transaction simply using a past version, as if it had
//! started far in the past, ignoring newer versions like any other transaction.
//! This transaction cannot write, as it does not have a unique timestamp (the
//! original read-write transaction originally owned this timestamp).
//!
//! The only wrinkle is that the time-travel query must also know what the active
//! set was at that version. Otherwise, it may see past transactions that committed
//! after that time, which were not visible to the original transaction that wrote
//! at that version. Similarly, if a time-travel query reads at a version that is
//! still active, it should not see its in-progress writes, and after it commits
//! a different time-travel query should not see those writes either, to maintain
//! version consistency.
//!
//! To achieve this, every read-write transaction stores its active set snapshot
//! in the key-value storage as well, as Key::TxnActiveSnapshot, such that later
//! time-travel queries can restore its original snapshot. Furthermore, a
//! time-travel query can only see versions below the snapshot version, otherwise
//! it could see spurious in-progress or since-committed versions.
//!
//! In the following example, a time-travel query at version=3 would see a=a1,
//! c=c1, d=d1.
//!
//! Time
//! 5
//! 4  a4
//! 3      b3      x
//! 2
//! 1  a1      c1  d1
//!    a   b   c   d   Keys
//!
//! Read-only queries work similarly to time-travel queries, with one exception:
//! they read at the next (current) version, i.e. Key::NextVersion, and use the
//! current active set, storing the snapshot in memory only. Read-only queries
//! do not increment the version sequence number in Key::NextVersion.
//!
//! GARBAGE COLLECTION
//! ==================
//!
//! Normally, old versions would be garbage collected regularly, when they are
//! no longer needed by active transactions or time-travel queries. However,
//! sandbox does not implement garbage collection, instead keeping all history
//! forever, both out of laziness and also because it allows unlimited time
//! travel queries.

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::iter::Peekable;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use serde::Deserialize;
use serde::Serialize;

use crate::error::Error;
use crate::error::Result;
use crate::storage;
use crate::storage::codec::bincodec;
use crate::storage::codec::keycodec;
use crate::storage::Storage;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_helper;

/// An MVCC version represents a logical timestamp. The latest version
/// is incremented when beginning each read-write transaction.
type Version = u64;

/// MVCC keys, using the KeyCode encoding which preserves the ordering and
/// grouping of keys. Cow byte slices allow encoding borrowed values and
/// decoding into owned values.
/// Using an enum as a top-level wrapper type. This will allow us to seamlessly
/// add a new variant when we need to change the key format in a backwards-compatible
/// manner (the different key types will sort separately).
#[derive(Debug, Deserialize, Serialize)]
enum Key<'a> {
    /// The next available MVCC version.
    NextVersion,
    /// Active (uncommitted) transactions by version, the value of this
    /// kind of key is useless, leave it as empty.
    TxnActive(Version),
    /// A snapshot of the active set at each version. Only written for
    /// versions where the active set is non-empty (excluding itself).
    TxnActiveSnapshot(Version),
    /// Keeps track of all keys written to by an active transaction (identified
    /// by its version), in case it needs to roll back.
    TxnWrite(
        Version,
        /// With sere_bytes, it will call serializer.serialize_bytes on [u8] instead
        /// of the default serializer.collect_seq which will result better performance.
        #[serde(with = "serde_bytes")]
        /// Fields of type &str and &[u8] are implicitly borrowed from the input data by Serde.
        /// Any other type of field can opt in to borrowing by using the #[serde(borrow)] attribute.
        ///
        /// With borrow attr, it will make difference at the deserialization stage.
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    /// A versioned key/value pair.
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
        Version,
    ),
    /// Unversioned non-transactional key/value pairs. These exist separately
    /// from versioned keys, i.e. the unversioned key "foo" is entirely
    /// independent of the versioned key "foo@7". These are mostly used
    /// for metadata.
    Unversioned(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> Key<'a> {
    pub fn encode(&self) -> Result<Vec<u8>> {
        keycodec::serialize(self)
    }

    // Add a 'a lifetime to the bytes parameter to ensure that the
    // lifetime of the bytes parameter matches the lifetime of the Key enum.
    // This is necessary because the Key enum contains variants that borrow
    // data with the 'a lifetime. By adding the 'a lifetime to the bytes parameter,
    // we ensure that the borrowed data in the Key enum is valid for the same
    // lifetime as the bytes parameter.
    pub fn decode(bytes: &'a [u8]) -> Result<Self> {
        keycodec::deserialize(bytes)
    }
}

/// MVCC key prefixes, for prefix scans. These must match the keys above,
/// including the enum variant index.
#[derive(Debug, Deserialize, Serialize)]
enum KeyPrefix<'a> {
    NextVersion,
    TxnActive,
    TxnActiveSnapshot,
    TxnWrite(Version),
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    Unversioned,
}

impl<'a> KeyPrefix<'a> {
    fn encode(&self) -> Result<Vec<u8>> {
        keycodec::serialize(&self)
    }
}

/// A raw key/value oriented MVCC-based transactional storage engine.
#[derive(Debug)]
pub struct MVCC<T: Storage> {
    kv: Arc<Mutex<T>>,
}

impl<T: Storage> MVCC<T> {
    pub fn new(kv: T) -> MVCC<T> {
        MVCC { kv: Arc::new(Mutex::new(kv)) }
    }

    pub fn begin(&self) -> Result<Transaction<T>> {
        Transaction::begin(Arc::clone(&self.kv))
    }

    pub fn begin_read_only(&self) -> Result<Transaction<T>> {
        Transaction::begin_read_only(Arc::clone(&self.kv), None)
    }

    pub fn begin_as_of(&self, as_of: Version) -> Result<Transaction<T>> {
        Transaction::begin_read_only(Arc::clone(&self.kv), Some(as_of))
    }

    pub fn resume(&self, state: TransactionState) -> Result<Transaction<T>> {
        Transaction::resume(Arc::clone(&self.kv), state)
    }

    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let mut session = self.kv.lock()?;
        session.set(&Key::Unversioned(key.into()).encode()?, value)?;
        Ok(())
    }

    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.kv.lock()?;
        session.get(&Key::Unversioned(key.into()).encode()?)
    }
}

/// A raw key/value MVCC transaction.
pub struct Transaction<T: Storage> {
    /// The underlying kv storage, shared by all transactions.
    kv: Arc<Mutex<T>>,
    /// The transaction state.
    st: TransactionState,
}

impl<T: Storage> Transaction<T> {
    fn begin(kv: Arc<Mutex<T>>) -> Result<Self> {
        let mut session = kv.lock()?;

        // Allocate a new version for the transaction.
        let version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => bincodec::deserialize(v)?,
            None => 1,
        };
        // Increase the next version.
        session.set(&Key::NextVersion.encode()?, bincodec::serialize(&(version + 1))?)?;

        // Take a snapshot of the active set.
        let active = Self::scan_active(&mut session)?;
        if !active.is_empty() {
            let key = Key::TxnActiveSnapshot(version).encode()?;
            session.set(&key, bincodec::serialize(&active)?)?;
        }
        // Add this transaction to the active set.
        session.set(&Key::TxnActive(version).encode()?, vec![])?;

        drop(session);

        let st = TransactionState::new(version, false, active);
        Ok(Transaction { kv, st })
    }

    /// Begins a new read-only transaction. If version is given it will see the
    /// state as of the beginning of that version (ignoring writes at that
    /// version). In other words, it sees the same state as the read-write
    /// transaction at that version saw when it began.
    /// If the given version is empty, use the latest version and active set as the
    /// snapshot at the moment the request is being made.
    fn begin_read_only(kv: Arc<Mutex<T>>, as_of: Option<Version>) -> Result<Self> {
        let mut session = kv.lock()?;

        // Fetch the latest version
        let mut version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => bincodec::deserialize(v)?,
            None => 1,
        };

        let mut active = HashSet::new();
        match as_of {
            None => active = Self::scan_active(&mut session)?,
            Some(as_of) => {
                if as_of >= version {
                    return Err(Error::value(format!("Version {} does not exists", as_of)));
                }
                // Set the version as the given version.
                version = as_of;
                // Fetch the snapshot at the given version
                let snapshot = session.get(&Key::TxnActiveSnapshot(version).encode()?)?;
                if let Some(snapshot) = snapshot {
                    active = bincodec::deserialize(&snapshot)?;
                }
            }
        }
        drop(session);

        let st = TransactionState::new(version, true, active);
        Ok(Transaction { kv, st })
    }

    fn resume(kv: Arc<Mutex<T>>, st: TransactionState) -> Result<Self> {
        if !st.read_only {
            // For read-write transactions, verify that the transaction is still
            // active before making further writes.
            let session = kv.lock()?;
            let key = Key::TxnActive(st.version).encode()?;
            if session.get(&key)?.is_none() {
                #[rustfmt::skip]
                return Err(Error::internal(format!( "No active transaction at version {}", st.version )));
            }
        }
        Ok(Transaction { kv, st })
    }

    /// Write a value for the given key.
    ///
    /// Check write conflict by looking for any versions of the key that:
    ///
    /// Are newer than the transaction's version
    /// Belong to another active transaction (uncommitted)
    ///
    /// Write value with None means delete the key.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if self.st.read_only {
            return Err(Error::ReadOnly);
        }

        let mut session = self.kv.lock()?;

        // Determine the version range for potential write conflict
        let from_version = self.st.active.iter().min().copied().unwrap_or(self.st.version + 1);
        let from = Key::Version(key.into(), from_version).encode()?;
        let to = Key::Version(key.into(), u64::MAX).encode()?;

        let scan = session.scan((Bound::Included(from), Bound::Excluded(to)));
        // Check the conflict by checking whether the last version is visible, i.e. if the latest
        // key is invisible to us (either a newer version, or an uncommitted version in our past).
        // We can only conflict with the latest key, since all transactions enforce the same invariant.
        if let Some((key, _)) = scan.last().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.st.is_visible(version) {
                        return Err(Error::Serialization);
                    }
                }
                key => return Err(Error::internal(format!("Expected Key::Version got {:?}", key))),
            }
        }

        // Write the new version and the txn write set.
        session.set(
            &Key::Version(key.into(), self.st.version).encode()?,
            bincodec::serialize(&value)?,
        )?;
        session.set(&Key::TxnWrite(self.st.version, key.into()).encode()?, vec![])?;

        Ok(())
    }

    /// Fetches a key's value, or None if it does not exist. traverse the version chain
    /// reversely, and return the newest visible version.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.kv.lock()?;
        let from = Key::Version(key.into(), 0).encode()?;
        let to = Key::Version(key.into(), self.st.version).encode()?;
        let mut scan = session.scan((Bound::Excluded(from), Bound::Included(to))).rev();
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if self.st.is_visible(version) {
                        return bincodec::deserialize(&value);
                    }
                }
                key => return Err(Error::internal(format!("Expected Key::Version got {:?}", key))),
            }
        }
        Ok(None)
    }

    /// Sets a value for a key.
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// Deletes a key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// Returns an iterator over the latest visible key/value pairs at the
    /// transaction's version.
    pub fn scan(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Result<Scan<T>> {
        let r0 = match range.0 {
            Bound::Included(key) => Bound::Included(Key::Version(key.into(), 0).encode()?),
            Bound::Excluded(key) => Bound::Excluded(Key::Version(key.into(), u64::MAX).encode()?),
            Bound::Unbounded => Bound::Included(Key::Version(vec![].into(), 0).encode()?),
        };
        let r1 = match range.1 {
            Bound::Included(key) => Bound::Included(Key::Version(key.into(), u64::MAX).encode()?),
            Bound::Excluded(key) => Bound::Excluded(Key::Version(key.into(), 0).encode()?),
            Bound::Unbounded => Bound::Excluded(KeyPrefix::Unversioned.encode()?),
        };
        let kv = self.kv.lock()?;
        Ok(Scan::new(kv, &self.st, (r0, r1)))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Scan<T>> {
        // Normally, KeyPrefix::Version will only match all versions of the
        // exact given key. We want all keys matching the prefix, so we chop off
        // the KeyCode byte slice terminator 0x0000 at the end.
        let mut prefix = KeyPrefix::Version(prefix.into()).encode()?;
        prefix.truncate(prefix.len() - 2);
        let kv = self.kv.lock()?;
        Ok(Scan::new_prefix(kv, &self.st, prefix))
    }

    /// Commits the transaction, by removing it from the active set. This will
    /// immediately make its writes visible to subsequent transactions. Also
    /// removes its TxnWrite records, which are no longer needed.
    pub fn commit(&self) -> Result<()> {
        if self.st.read_only {
            return Ok(());
        }
        let mut session = self.kv.lock()?;
        session.remove_prefix(&KeyPrefix::TxnWrite(self.st.version).encode()?)?;
        session.remove(&Key::TxnActive(self.st.version).encode()?)?;
        Ok(())
    }

    /// Rollback the transaction by undoing all written versions and removing
    /// it from the active set. The active set snapshot is left behind, since
    /// this is needed for time travel queries at this version.
    pub fn rollback(&self) -> Result<()> {
        if self.st.read_only {
            return Ok(());
        }
        let mut session = self.kv.lock()?;
        let mut rollback = Vec::new();
        let mut scan = session.scan_prefix(&KeyPrefix::TxnWrite(self.st.version).encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnWrite(version, key) => {
                    rollback.push(Key::Version(key, version).encode()?);
                }
                key => {
                    return Err(Error::internal(format!("Expected Key::TxnWrite got {:?}", key)))
                }
            }
        }
        // drop scan manually to terminate the immutable borrow lifetime bound from scan to session
        // so that we can borrow session as mutable later for removal.
        drop(scan);

        for key in rollback {
            session.remove(&key)?;
        }
        session.remove(&Key::TxnActive(self.st.version).encode()?)?;
        Ok(())
    }

    pub fn state(&self) -> TransactionState {
        self.st.clone()
    }

    fn scan_active(kv: &mut MutexGuard<T>) -> Result<HashSet<Version>> {
        let mut active = HashSet::new();
        let mut it = kv.scan_prefix(&KeyPrefix::TxnActive.encode()?);
        while let Some((k, _)) = it.next().transpose()? {
            match Key::decode(&k)? {
                Key::TxnActive(version) => active.insert(version),
                _ => return Err(Error::internal(format!("Expected TxnActive key, got {:?}", k))),
            };
        }
        Ok(active)
    }
}

impl<T: Storage> Display for Transaction<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.st)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionState {
    /// The version this transaction is running at. Only one read-write
    /// transaction can run at a given version, since this identifies its
    /// writes.
    pub version: Version,
    /// If true, the transaction is read only.
    pub read_only: bool,
    /// Act as a concurrent active(uncommitted) transactions, as of the start
    /// of this transaction. Their writes should be invisible to this
    /// transaction even if they're writing at a lower version, since they're
    /// not committed yet.
    pub active: HashSet<Version>,
}

impl TransactionState {
    fn new(version: Version, read_only: bool, active: HashSet<Version>) -> TransactionState {
        TransactionState { version, read_only, active }
    }

    /// Checks whether the given version is visible to this transaction.
    ///
    /// Future versions, and versions belonging to active transactions as of
    /// the start of this transaction, are never visible.
    ///
    /// Read-write transactions see their own writes at their version.
    ///
    /// Read-only queries only see versions below the transaction's version,
    /// excluding the version itself. Because the version of a read-only transaction
    /// is actually an upper bound instead of a meaningful version, i.e., read-only
    /// transaction does not consume version, and the version number can be used by
    /// a consecutive concurrent read-write transaction. In short this is to ensure
    /// time-travel queries see a consistent version both before and after any active
    /// transaction at that version commits its writes. See the module documentation
    /// for details.
    fn is_visible(&self, version: Version) -> bool {
        if self.active.get(&version).is_some() {
            return false;
        }
        if self.read_only {
            return version < self.version;
        }
        version <= self.version
    }
}

impl TransactionState {
    fn format_active(&self) -> String {
        let mut elements: Vec<Version> = self.active.iter().copied().collect();
        elements.sort();
        let elements: Vec<String> = elements.into_iter().map(|v| v.to_string()).collect();
        format!("{{{}}}", elements.join(","))
    }
}

impl Display for TransactionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "v{} {} active={}",
            self.version,
            if self.read_only { "read-only" } else { "read-write" },
            self.format_active()
        )
    }
}

enum ScanType {
    Range((Bound<Vec<u8>>, Bound<Vec<u8>>)),
    Prefix(Vec<u8>),
}

pub struct Scan<'a, T: Storage + 'a> {
    /// Locked kv storage.
    kv: MutexGuard<'a, T>,
    /// The transaction state, used for visibility check.
    st: &'a TransactionState,
    /// Scan type
    typ: ScanType,
}

impl<'a, T: Storage + 'a> Scan<'a, T> {
    fn new(
        kv: MutexGuard<'a, T>,
        st: &'a TransactionState,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Self {
        Scan { kv, st, typ: ScanType::Range(range) }
    }

    fn new_prefix(kv: MutexGuard<'a, T>, st: &'a TransactionState, prefix: Vec<u8>) -> Self {
        Scan { kv, st, typ: ScanType::Prefix(prefix) }
    }

    pub fn iter(&self) -> ScanIterator<'_> {
        match &self.typ {
            ScanType::Range(r) => {
                let inner = self.kv.scan(r.clone());
                ScanIterator::new(self.st, inner)
            }
            ScanType::Prefix(prefix) => {
                let inner = self.kv.scan_prefix(prefix);
                ScanIterator::new(self.st, inner)
            }
        }
    }

    #[allow(dead_code)] // used by test case only
    fn to_vec(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.iter()
            .map(|it| it.and_then(|(k, v)| Ok((k, v))))
            .collect::<Result<Vec<(Vec<u8>, Vec<u8>)>>>()
    }
}

pub struct ScanIterator<'a> {
    inner: Peekable<VersionIterator<'a>>,
    last_back: Option<Vec<u8>>,
}

impl<'a> ScanIterator<'a> {
    fn new(st: &'a TransactionState, it: Box<dyn storage::ScanIterator<'a>>) -> Self {
        let inner = VersionIterator::new(st, it).peekable();
        ScanIterator { inner, last_back: None }
    }

    /// Fallible next(), emitting the next item with latest visible version, or None if exhausted.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, _version, value)) = self.inner.next().transpose()? {
            // check if the next key is same with the current key. if yes,
            // it means the current keys is not the latest version, and we
            // should skip it.
            let next = self.inner.peek();
            match next {
                Some(Ok((next_key, _, _))) if next_key == &key => continue,
                Some(Err(err)) => return Err(err.clone()),
                Some(Ok(_)) | None => {}
            }
            // If the key is live and the value is not a tombstone, emit it.
            if let Some(val) = bincodec::deserialize(&value)? {
                return Ok(Some((key, val)));
            }
        }
        Ok(None)
    }

    /// Fallible next_back(), emitting the next item with the latest visible version from the back,
    /// or None if exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, _version, value)) = self.inner.next_back().transpose()? {
            // If this key is the same as the last emitted key from the back,
            // this must be an older version, so skip it.
            if let Some(last_key) = &self.last_back {
                if &key == last_key {
                    continue;
                }
            }
            self.last_back = Some(key.clone());
            // If the key is live and the value is not a tombstone, emit it.
            if let Some(val) = bincodec::deserialize(&value)? {
                return Ok(Some((key, val)));
            }
        }
        Ok(None)
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

struct VersionIterator<'a> {
    st: &'a TransactionState,
    inner: Box<dyn storage::ScanIterator<'a>>,
}

impl<'a> VersionIterator<'a> {
    fn new(st: &'a TransactionState, inner: Box<dyn storage::ScanIterator<'a>>) -> Self {
        VersionIterator { st, inner }
    }

    // Fallible next(), emitting the next item, or None if exhausted.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            if let Some(a) = self.decode_visible(key, value)? {
                return Ok(Some(a));
            }
        }
        Ok(None)
    }

    // Fallible next_back(), emitting the previous item, or None if exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next_back().transpose()? {
            if let Some(a) = self.decode_visible(key, value)? {
                return Ok(Some(a));
            }
        }
        Ok(None)
    }

    fn decode_visible(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        let (key, version) = match Key::decode(&key)? {
            Key::Version(key, version) => (key, version),
            _ => return Err(Error::internal(format!("Expected Key::Version, got {:?}", key))),
        };
        if !self.st.is_visible(version) {
            return Ok(None);
        }
        Ok(Some((key.into_owned(), version, value)))
    }
}

impl<'a> Iterator for VersionIterator<'a> {
    type Item = Result<(Vec<u8>, Version, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a> DoubleEndedIterator for VersionIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

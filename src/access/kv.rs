use std::borrow::Cow;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::access::engine::Engine;
use crate::access::engine::IndexScan;
use crate::access::engine::Scan;
use crate::access::engine::Transaction;
use crate::access::expression::Expression;
use crate::access::value::IndexKey;
use crate::access::value::PrimaryKey;
use crate::access::value::Tuple;
use crate::access::value::Values;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Columns;
use crate::catalog::index::Index;
use crate::catalog::index::Indexes;
use crate::catalog::r#type::Value;
use crate::catalog::schema::Schema;
use crate::catalog::schema::Schemas;
use crate::concurrency::mvcc;
use crate::concurrency::mvcc::TransactionState;
use crate::error::Error;
use crate::error::Result;
use crate::storage::codec::bincodec;
use crate::storage::codec::keycodec;
use crate::storage::Storage;

/// A transactional access engine based on an underlying MVCC key/value store.
#[derive(Debug)]
pub struct Kv<T: Storage> {
    mvcc: mvcc::MVCC<T>,
}

impl<T: Storage> Kv<T> {
    #[allow(dead_code)]
    pub fn new(kv: T) -> Kv<T> {
        Kv { mvcc: mvcc::MVCC::new(kv) }
    }

    pub fn resume(&self, state: TransactionState) -> Result<<Self as Engine>::Transaction> {
        let txn = self.mvcc.resume(state)?;
        Ok(KvTxn::new(txn))
    }
}

impl<T: Storage> Engine for Kv<T> {
    type Transaction = KvTxn<T>;

    fn begin(&self) -> Result<Self::Transaction> {
        let txn = self.mvcc.begin()?;
        Ok(KvTxn::new(txn))
    }

    fn begin_read_only(&self) -> Result<Self::Transaction> {
        let txn = self.mvcc.begin_read_only()?;
        Ok(KvTxn::new(txn))
    }

    fn begin_as_of(&self, version: u64) -> Result<Self::Transaction> {
        let txn = self.mvcc.begin_as_of(version)?;
        Ok(KvTxn::new(txn))
    }
}

/// SQL keys, using the KeyCode order-preserving encoding. Uses table and column
/// names directly as identifiers, to avoid additional indirection. It is not
/// possible to change names, so this is ok. Cow strings allow encoding borrowed
/// values and decoding into owned values.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Key<'a> {
    /// The next object id.
    NextOid,
    /// A table schema by table name.
    Table(Cow<'a, str>),
    /// An index schema by table name and index name.
    Index(Cow<'a, str>, Cow<'a, str>),
    /// A table row, by table name and id.
    Row(Cow<'a, str>, PrimaryKey),
    /// A hash index entry by table name, index name
    /// and values of index keys as the key a primary key
    /// posting list as the entry content.
    ///
    /// In case of Key-valued based storage, it maybe implemented
    /// with three different strategy, lazy, eger and composite.
    /// Here we are using the eger strategy.
    HashIndexEntry(
        Cow<'a, str>,
        Cow<'a, str>,
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> Key<'a> {
    fn encode(self) -> Result<Vec<u8>> {
        keycodec::serialize(&self)
    }

    fn decode(bytes: &'a [u8]) -> Result<Self> {
        keycodec::deserialize(bytes)
    }
}

/// Key prefixes, allowing prefix scans of specific parts of the keyspace. These
/// must match the keys -- in particular, the enum variant indexes must match.
#[derive(Debug, Deserialize, Serialize)]
enum KeyPrefix<'a> {
    NextOid,
    /// All table schemas.
    Table,
    /// ALl table index schemas
    Index(Cow<'a, str>),
    /// An entire table's rows, by table name.
    Row(Cow<'a, str>),
    /// All hash index entries by table name, index name
    HashIndexEntry(Cow<'a, str>, Cow<'a, str>),
}

impl<'a> KeyPrefix<'a> {
    fn encode(self) -> Result<Vec<u8>> {
        keycodec::serialize(&self)
    }
}

pub struct KvTxn<T: Storage> {
    txn: mvcc::Transaction<T>,
}

impl<T: Storage> KvTxn<T> {
    pub fn new(txn: mvcc::Transaction<T>) -> Self {
        KvTxn { txn }
    }

    pub fn state(&self) -> TransactionState {
        self.txn.state()
    }

    fn get_tuples_by_pks(&self, table: &str, pks: Vec<PrimaryKey>) -> Result<Vec<Tuple>> {
        pks.iter()
            .map(|it| {
                self.read(table, it)
                    .and_then(|t| t.ok_or(Error::value(format!("Tuple with key {} not found", it))))
            })
            .collect::<Result<Vec<Tuple>>>()
    }
}

impl<T: Storage> Catalog for KvTxn<T> {
    fn read_table(&self, table: &str) -> Result<Option<Schema>> {
        let table = Cow::Borrowed(table);
        let key = Key::Table(table).encode()?;
        self.txn.get(&key)?.map(|it| bincodec::deserialize(&it)).transpose()
    }

    fn create_table(&self, schema: Schema) -> Result<()> {
        schema.validate()?;
        if self.read_table(&schema.name)?.is_some() {
            return Err(Error::value(format!("Table {} already exits", &schema.name)));
        }
        let table = Cow::Borrowed(schema.name.as_str());
        let key = Key::Table(table).encode()?;
        self.txn.set(&key, bincodec::serialize(&schema)?)?;

        // Create unique index for unique column
        for column in schema.columns.iter() {
            if column.primary_key || !column.unique {
                continue;
            }
            let index =
                Index::from(&column.name, &schema.name, Columns::from([Arc::clone(column)]), true);
            self.create_index(index)?;
        }
        Ok(())
    }

    fn delete_table(&self, table: &str) -> Result<()> {
        // delete indexes
        let indexes = self.scan_table_indexes(table)?;
        for index in indexes {
            self.delete_index(&index.name, &index.tblname)?
        }

        // drop table data
        Transaction::drop(self, table)?;

        // delete table definition
        let key = Key::Table(table.into()).encode()?;
        self.txn.delete(&key)?;

        Ok(())
    }

    fn scan_tables(&self) -> Result<Schemas> {
        let prefix = KeyPrefix::Table.encode()?;
        let scan = self.txn.scan_prefix(prefix)?;
        let iter = scan
            .iter()
            .map(|it| it.and_then(|(_, v)| bincodec::deserialize(&v)))
            .collect::<Result<Vec<Schema>>>()?
            .into_iter();
        Ok(Box::new(iter))
    }

    fn read_index(&self, index: &str, table: &str) -> Result<Option<Index>> {
        let key = Key::Index(table.into(), index.into()).encode()?;
        self.txn.get(&key)?.map(|it| bincodec::deserialize(&it)).transpose()
    }

    fn create_index(&self, index: Index) -> Result<()> {
        index.validate()?;
        if self.read_index(&index.name, &index.tblname)?.is_some() {
            return Err(Error::value(format!(
                "Index {} on table {} already exists",
                index.name, index.tblname
            )));
        }
        let tblame = Cow::Borrowed(index.tblname.as_str());
        let name = Cow::Borrowed(index.name.as_str());
        let key = Key::Index(tblame, name).encode()?;
        self.txn.set(&key, bincodec::serialize(&index)?)

        // TODO:
        // index existing data in case of the table exists
    }

    fn delete_index(&self, index: &str, table: &str) -> Result<()> {
        // delete indexed entries
        self.delete_index_entries(table, index)?;

        // delete index definition
        let table = Cow::Borrowed(table);
        let index = Cow::Borrowed(index);
        let key = Key::Index(table, index).encode()?;
        self.txn.delete(&key)?;

        Ok(())
    }

    fn scan_table_indexes(&self, table: &str) -> Result<Indexes> {
        let prefix = KeyPrefix::Index(table.into()).encode()?;
        let scan = self.txn.scan_prefix(prefix)?;
        scan.iter()
            .map(|it| it.and_then(|(_, v)| bincodec::deserialize(&v)))
            .collect::<Result<Vec<Index>>>()
    }
}

impl<T: Storage> KvTxn<T> {
    fn insert_index_entry(&mut self, index: Index, tuple: &Tuple) -> Result<()> {
        let pk = tuple.primary_key()?;
        let index_key = tuple.get_values(&index.columns)?;
        let tblame = Cow::Borrowed(index.tblname.as_str());
        let name = Cow::Borrowed(index.name.as_str());
        let keyv = bincodec::serialize(&index_key)?;
        let key = Key::HashIndexEntry(tblame, name, keyv.into()).encode()?;
        match self.txn.get(&key)? {
            None => self.txn.set(&key, bincodec::serialize(&vec![pk])?),
            Some(bytes) => {
                let mut values: Vec<Value> = bincodec::deserialize(&bytes)?;
                // Check uniqueness
                if index.uniqueness && values.iter().find(|&it| it == pk).is_some() {
                    return Err(Error::value(format!(
                        "index key {:?} already exists for index {}",
                        index_key, index.name
                    )));
                }
                values.push(pk.clone());
                self.txn.set(&key, bincode::serialize(&values)?)
            }
        }
    }

    fn delete_index_entry(
        &mut self,
        index: Index,
        index_key: IndexKey,
        pk: &PrimaryKey,
    ) -> Result<()> {
        let tblame = Cow::Borrowed(index.tblname.as_str());
        let name = Cow::Borrowed(index.name.as_str());
        let keyv = bincodec::serialize(&index_key)?;
        let key = Key::HashIndexEntry(tblame, name, keyv.into()).encode()?;
        if index.uniqueness {
            self.txn.delete(&key)?;
            return Ok(());
        }
        match self.txn.get(&key)? {
            None => Ok(()),
            Some(bytes) => {
                let mut values: Vec<Value> = bincodec::deserialize(&bytes)?;
                if let Some(pos) = values.iter().position(|it| it == pk) {
                    values.remove(pos);
                }
                self.txn.set(&key, bincodec::serialize(&values)?)
            }
        }
    }

    fn delete_index_entries(&self, table: &str, index: &str) -> Result<()> {
        let table = Cow::Borrowed(table);
        let index = Cow::Borrowed(index);
        let prefix = KeyPrefix::HashIndexEntry(table, index).encode()?;
        let scan = self.txn.scan_prefix(prefix)?;
        let mut iter = scan.iter();
        while let Some((k, _)) = iter.next().transpose()? {
            self.txn.delete(&k)?;
        }
        Ok(())
    }
}

impl<T: Storage> Transaction for KvTxn<T> {
    fn version(&self) -> u64 {
        self.txn.state().version
    }

    fn read_only(&self) -> bool {
        self.txn.state().read_only
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<()> {
        self.txn.rollback()
    }

    fn insert(&mut self, table: &str, tuple: Tuple) -> Result<PrimaryKey> {
        let schema = self.must_read_table(table)?;
        tuple.check_columns(&schema.columns)?;
        // Check if the pk exists
        let pk = tuple.primary_key()?;
        let key = Key::Row(table.into(), pk.clone()).encode()?;
        if self.txn.get(&key)?.is_some() {
            return Err(Error::value(format!(
                "Primary key {} already exits for table {}",
                pk, table
            )));
        }
        // Set the tuple value under primary key
        self.txn.set(&key, bincodec::serialize(&tuple.values)?)?;

        // Update indexes
        let indexes = self.scan_table_indexes(table)?;
        for index in indexes {
            self.insert_index_entry(index, &tuple)?;
        }
        Ok(pk.clone())
    }

    fn delete(&mut self, table: &str, pk: &PrimaryKey) -> Result<()> {
        let schema = self.must_read_table(table)?;

        let key = Key::Row(table.into(), pk.clone()).encode()?;
        let row = self.txn.get(&key)?;
        if row.is_none() {
            return Ok(());
        }
        let tuple = Tuple::new(bincodec::deserialize(&row.unwrap())?, &schema.columns)?;

        // Delete tuple from index
        let indexes = self.scan_table_indexes(table)?;
        for index in indexes {
            let cols = &index.columns;
            let keyv = tuple.get_values(&cols)?;
            self.delete_index_entry(index, keyv, pk)?;
        }

        // Delete the tuple itself
        self.txn.delete(&key)?;

        Ok(())
    }

    fn read(&self, table: &str, pk: &PrimaryKey) -> Result<Option<Tuple>> {
        let schema = self.must_read_table(table)?;
        let key = Key::Row(table.into(), pk.clone()).encode()?;
        let row = self.txn.get(&key)?;
        if let Some(bytes) = row {
            let tuple = Tuple::new(bincodec::deserialize(&bytes)?, &schema.columns)?;
            return Ok(Some(tuple));
        }
        Ok(None)
    }

    fn scan(&self, table: &str, predicate: Option<Expression>) -> Result<Scan> {
        let schema = self.must_read_table(table)?;
        let prefix = KeyPrefix::Row(table.into()).encode()?;
        let scan = self.txn.scan_prefix(prefix)?;
        let iter = scan
            .iter()
            .map(|r| {
                r.and_then(|(_, v)| {
                    let values = bincodec::deserialize(&v)?;
                    Tuple::new(values, &schema.columns)
                })
            })
            .filter_map(|r| match r {
                Ok(t) => match &predicate {
                    None => Some(Ok(t)),
                    Some(p) => match p.evaluate(Some(&t)) {
                        Ok(Value::Boolean(b)) if b => Some(Ok(t)),
                        Ok(Value::Boolean(_)) => None,
                        Ok(v) => Some(Err(Error::value(format!(
                            "Expected boolean predicate, got {}",
                            v
                        )))),
                        Err(err) => Some(Err(err)),
                    },
                },
                err => Some(err),
            })
            .collect::<Vec<_>>()
            .into_iter();
        Ok(Box::new(iter))
    }

    fn drop(&self, table: &str) -> Result<()> {
        let prefix = KeyPrefix::Row(table.into()).encode()?;
        let scan = self.txn.scan_prefix(prefix)?;
        let mut iter = scan.iter();
        while let Some((k, _)) = iter.next().transpose()? {
            self.txn.delete(&k)?
        }
        Ok(())
    }

    fn read_index_entry(
        &self,
        table: &str,
        index: &str,
        index_key: IndexKey,
    ) -> Result<Option<Vec<Tuple>>> {
        let keyv = bincodec::serialize(&index_key)?;
        let key = Key::HashIndexEntry(table.into(), index.into(), keyv.into()).encode()?;
        match self.txn.get(&key)? {
            None => Ok(None),
            Some(bytes) => {
                let values: Vec<Value> = bincodec::deserialize(&bytes)?;
                let tuples = self.get_tuples_by_pks(table, values)?;
                Ok(Some(tuples))
            }
        }
    }

    fn scan_index_entries(&self, table: &str, index: &str) -> Result<IndexScan> {
        let prefix = KeyPrefix::HashIndexEntry(table.into(), index.into()).encode()?;
        let scan = self.txn.scan_prefix(prefix)?;
        let primary_keys = scan
            .iter()
            .map(|r| {
                r.and_then(|(k, vbytes)| match Key::decode(&k)? {
                    Key::HashIndexEntry(_, _, keyv) => {
                        let index_key: Values = bincodec::deserialize(&keyv)?;
                        let values: Vec<Value> = bincodec::deserialize(&vbytes)?;
                        Ok((index_key, values))
                    }
                    key => Err(Error::value(format!("Expect Key::HashIndexEntry got {:?}", key))),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // drop the scan to release the storage lock
        drop(scan);

        let iter = primary_keys
            .into_iter()
            .map(|(index_key, pks)| {
                let tuples = self.get_tuples_by_pks(table, pks)?;
                Ok((index_key, tuples))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter();

        Ok(Box::new(iter))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

    use rand::distributions::Distribution;
    use rand::distributions::Uniform;
    use rand::thread_rng;

    use super::super::engine::Transaction as Txn;
    use super::*;
    use crate::catalog::column::ColumnBuilder;
    use crate::catalog::r#type::DataType;
    use crate::error::Result;
    use crate::storage::memory::Memory;

    #[test]
    fn test_catalog_ops() -> Result<()> {
        let memory = Memory::new();
        let kv = Kv::new(memory);

        let t1 = kv.begin()?;

        let table = String::from("foo");
        let columns = Columns::from(vec![
            ColumnBuilder::new("col1", DataType::Integer).primary_key().build()?,
            ColumnBuilder::new("col2", DataType::String).default_value(Value::Null).build()?,
            ColumnBuilder::new("col3", DataType::String).not_null().unique().build()?,
        ]);
        let schema = Schema::new(table.clone(), columns);

        // create table
        t1.create_table(schema.clone())?;

        // check table creation
        let got = t1.read_table(&table)?;
        assert_eq!(got, Some(schema.clone()));

        // An unique index should be created for unique column col3
        let indexes = t1.scan_table_indexes(&table)?;
        assert_eq!(indexes.len(), 1);

        // creat index
        let index = String::from("alice");
        let index = Index::from(&index, &table, Columns::from(&schema.columns[1..2]), false);
        t1.create_index(index.clone())?;

        // check the index creation
        let got = t1.read_index(&index.name, &table)?;
        assert_eq!(got, Some(index.clone()));
        let indexes = t1.scan_table_indexes(&table)?;
        assert_eq!(indexes.len(), 2);

        // delete index
        t1.delete_index(&index.name, &table)?;

        // check the index deletion
        let got = t1.read_index(&index.name, &table)?;
        assert_eq!(got, None);
        let indexes = t1.scan_table_indexes(&table)?;
        assert_eq!(indexes.len(), 1);

        // delete table
        t1.delete_table(&table)?;

        // check table deletion
        let got = t1.read_table(&table)?;
        assert_eq!(got, None);
        let indexes = t1.scan_table_indexes(&table)?;
        assert_eq!(indexes.len(), 0);

        t1.commit()?;

        Ok(())
    }

    enum Dst {
        Uniform,
        Serial,
    }

    struct ColumnGenerator {
        name: String,
        datatype: DataType,
        primary_key: bool,
        nullable: bool,
        unique: bool,
        default: Option<Value>,
        dst: Dst,
        min: u64,
        max: u64,
        serial_counter: u64,
    }

    impl ColumnGenerator {
        fn from(name: &str, datatype: DataType, nullable: bool, unique: bool) -> ColumnGenerator {
            ColumnGenerator {
                name: name.to_string(),
                datatype,
                nullable,
                unique,
                ..Self::default()
            }
        }

        fn make_values(&mut self, count: usize) -> Result<Vec<Value>> {
            match self.datatype {
                DataType::Null => Ok(vec![Value::Null; count]),
                DataType::Boolean => self.gen_boolean_values(count),
                DataType::Integer => self.gen_integer_values(count),
                DataType::Float => self.gen_float_values(count),
                DataType::String => self.gen_string_values(count),
            }
        }

        fn gen_boolean_values(&mut self, count: usize) -> Result<Vec<Value>> {
            let mut values = Vec::with_capacity(count);
            match self.dst {
                Dst::Serial => {
                    for i in 0..count {
                        values.push(Value::Boolean(i % 2 == 0));
                        self.serial_counter += 1;
                    }
                }
                Dst::Uniform => {
                    let mut rng = thread_rng();
                    let uniform = Uniform::new(self.min, self.max);
                    for _ in 0..count {
                        let n = uniform.sample(&mut rng);
                        values.push(Value::Boolean(n % 2 == 0))
                    }
                }
            }
            Ok(values)
        }

        fn gen_integer_values(&mut self, count: usize) -> Result<Vec<Value>> {
            let mut values = Vec::with_capacity(count);
            match self.dst {
                Dst::Serial => {
                    for i in 0..count {
                        values.push(Value::Integer(i as i64));
                        self.serial_counter += 1;
                    }
                }
                Dst::Uniform => {
                    let mut rng = thread_rng();
                    let uniform = Uniform::new(self.min, self.max);
                    for _ in 0..count {
                        let n = uniform.sample(&mut rng);
                        values.push(Value::Integer(n as i64))
                    }
                }
            }
            Ok(values)
        }

        fn gen_float_values(&mut self, count: usize) -> Result<Vec<Value>> {
            let mut values = Vec::with_capacity(count);
            match self.dst {
                Dst::Serial => {
                    for i in 0..count {
                        values.push(Value::Float(i as f64));
                        self.serial_counter += 1;
                    }
                }
                Dst::Uniform => {
                    let mut rng = thread_rng();
                    let uniform = Uniform::new(self.min, self.max);
                    for _ in 0..count {
                        let n = uniform.sample(&mut rng);
                        values.push(Value::Float(n as f64))
                    }
                }
            }
            Ok(values)
        }

        fn gen_string_values(&mut self, count: usize) -> Result<Vec<Value>> {
            let mut values = Vec::with_capacity(count);
            match self.dst {
                Dst::Serial => {
                    let mut rng = thread_rng();
                    let letters = Uniform::new_inclusive(b'a', b'z');
                    for _ in 0..count {
                        let s = (0..self.serial_counter)
                            .map(|_| letters.sample(&mut rng) as char)
                            .collect();
                        values.push(Value::String(s));
                        self.serial_counter += 1;
                    }
                }
                Dst::Uniform => {
                    let mut rng = thread_rng();
                    let uniform = Uniform::new(self.min, self.max);
                    for _ in 0..count {
                        let len = uniform.sample(&mut rng);
                        let letters = Uniform::new_inclusive(b'a', b'z');
                        let s = (0..len).map(|_| letters.sample(&mut rng) as char).collect();
                        values.push(Value::String(s))
                    }
                }
            }
            Ok(values)
        }
    }

    impl Default for ColumnGenerator {
        fn default() -> Self {
            Self {
                name: "".to_string(),
                datatype: DataType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                dst: Dst::Uniform,
                min: 0,
                max: 100,
                serial_counter: 0,
            }
        }
    }

    struct TableGenerator {
        name: String,
        num_rows: u64,
        column_generators: Vec<ColumnGenerator>,
    }

    impl TableGenerator {
        fn new(
            name: String,
            num_rows: u64,
            column_generators: Vec<ColumnGenerator>,
        ) -> TableGenerator {
            TableGenerator { name, num_rows, column_generators }
        }

        fn generate(&mut self, txn: &mut dyn Txn) -> Result<()> {
            // generate column definition
            let mut columns = vec![];
            for (i, it) in &mut self.column_generators.iter_mut().enumerate() {
                let mut column_builder = ColumnBuilder::new(it.name.clone(), it.datatype.clone());
                if i == 0 {
                    column_builder = column_builder.primary_key();
                }

                column_builder = column_builder.uniqueness(it.unique);
                if it.unique {
                    it.dst = Dst::Serial;
                }

                column_builder = column_builder.nullable(it.nullable);
                if it.nullable {
                    let default = match it.datatype {
                        DataType::Null => Value::Null,
                        DataType::Boolean => Value::Boolean(false),
                        DataType::Integer => Value::Integer(0),
                        DataType::Float => Value::Float(0f64),
                        DataType::String => Value::String(String::from("")),
                    };
                    column_builder = column_builder.default_value(default);
                }

                let column = column_builder.build()?;
                columns.push(column)
            }
            // create table
            let table = Schema::new(self.name.clone(), columns.into());
            txn.create_table(table.clone())?;

            // generate table data
            self.generate_data(table, txn)?;
            Ok(())
        }

        fn generate_data(&mut self, table: Schema, txn: &mut dyn Txn) -> Result<()> {
            let batch_size = 128;
            let mut num_generated = 0;
            while num_generated < self.num_rows {
                let num_values = min(batch_size, self.num_rows - num_generated);
                let mut values = vec![];
                for it in &mut self.column_generators {
                    values.push(it.make_values(num_values as usize)?);
                }
                for i in 0..num_values {
                    let mut entry = vec![];
                    for it in &values {
                        entry.push(it[i as usize].clone());
                    }
                    let values = Values::from(entry);
                    let tuple = Tuple::new(values, &table.columns)?;
                    txn.insert(&table.name, tuple)?;
                    num_generated += 1;
                }
            }
            Ok(())
        }
    }

    #[test]
    fn test_table_ops() -> Result<()> {
        let memory = Memory::new();
        let kv = Kv::new(memory);

        let mut t1 = kv.begin()?;

        // generate table
        let column_generators = vec![
            ColumnGenerator::from("col1", DataType::Integer, false, true),
            ColumnGenerator::from("col2", DataType::Boolean, true, false),
            ColumnGenerator::from("col3", DataType::Float, true, false),
            ColumnGenerator::from("col4", DataType::String, false, true),
        ];
        let tblname = String::from("foo");
        let mut table_generator = TableGenerator::new(tblname.clone(), 100, column_generators);
        table_generator.generate(&mut t1)?;

        // scan tables
        let mut scan = t1.scan(&tblname, None)?;
        let mut tuples = vec![];
        while let Some(t) = scan.next().transpose()? {
            tuples.push(t)
        }
        assert_eq!(100, tuples.len());

        // get by pk
        let tuple = &tuples[0];
        let pk = tuple.primary_key()?;
        let got = t1.read(&tblname, pk)?;
        assert_eq!(Some(tuple.clone()), got);

        // indexes
        let indexes = t1.scan_table_indexes(&tblname)?;
        assert_eq!(1, indexes.len());

        let index = &indexes[0];

        // scan index entries
        let mut scan = t1.scan_index_entries(&tblname, &index.name)?;
        let mut index_entries = vec![];
        while let Some((_, v)) = scan.next() {
            index_entries.extend(v)
        }
        assert_eq!(100, index_entries.len());

        // get by index_key
        let index_key = tuple.get_values(&index.columns)?;
        let gots = t1.read_index_entry(&tblname, &index.name, index_key.clone())?;
        assert_ne!(None, gots);
        let gots = gots.unwrap();
        assert_eq!(1, gots.len());
        assert_eq!(tuple, &gots[0]);

        // delete by pk
        t1.delete(&tblname, pk)?;

        // check deletion
        let tuple = t1.read(&tblname, pk)?;
        assert_eq!(None, tuple);
        let tuple = t1.read_index_entry(&tblname, &index.name, index_key)?;
        assert_eq!(None, tuple);

        // scan tables again
        let mut scan = t1.scan(&tblname, None)?;
        let mut tuples = vec![];
        while let Some(t) = scan.next().transpose()? {
            tuples.push(t)
        }
        assert_eq!(99, tuples.len());

        // scan index entries again
        let mut scan = t1.scan_index_entries(&tblname, &index.name)?;
        let mut index_entries = vec![];
        while let Some((_, v)) = scan.next() {
            index_entries.extend(v)
        }
        assert_eq!(99, index_entries.len());

        t1.commit()?;

        Ok(())
    }
}

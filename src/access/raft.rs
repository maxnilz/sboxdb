use std::borrow::Cow;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::access::engine::Engine;
use crate::access::engine::IndexScan;
use crate::access::engine::Scan;
use crate::access::engine::Transaction;
use crate::access::kv::Kv;
use crate::access::predicate::Predicate;
use crate::access::value::IndexKey;
use crate::access::value::PrimaryKey;
use crate::access::value::Row;
use crate::catalog::catalog::Catalog;
use crate::catalog::index::Index;
use crate::catalog::index::Indexes;
use crate::catalog::schema::Schema;
use crate::catalog::schema::Schemas;
use crate::concurrency::mvcc::TransactionState;
use crate::error::Error;
use crate::error::Result;
use crate::raft;
use crate::raft::server::Server;
use crate::raft::ApplyMsg;
use crate::raft::Command;
use crate::raft::CommandResult;
use crate::storage::codec::bincodec;
use crate::storage::Storage;

/// Query related access methods
#[derive(Debug, Serialize, Deserialize)]
enum Query<'a> {
    /// Scans the tables
    ScanTables { txn: TransactionState },
    /// Reads a table
    ReadTable { txn: TransactionState, table: Cow<'a, str> },

    /// Scans the indexes
    ScanIndexes { txn: TransactionState, table: Cow<'a, str> },
    /// Reads an index
    ReadIndex { txn: TransactionState, table: Cow<'a, str>, index: Cow<'a, str> },

    /// Reads a row
    Read { txn: TransactionState, table: String, pk: PrimaryKey },
    /// Scans a table's rows
    Scan { txn: TransactionState, table: String, predicate: Option<Predicate> },

    /// Reads an index entry
    ReadIndexEntry { txn: TransactionState, table: String, index: String, value: IndexKey },
    /// Scans an index entries
    ScanIndexEntries { txn: TransactionState, table: String, index: String },
}

/// Mutation related access methods
#[derive(Debug, Serialize, Deserialize)]
enum Mutation<'a> {
    /// Begins a transaction
    Begin { read_only: bool, as_of: Option<u64> },
    /// Commits the given transaction
    Commit(TransactionState),
    /// Rolls back the given transaction
    Rollback(TransactionState),

    /// Creates a table
    CreateTable { txn: TransactionState, schema: Schema },
    /// Deletes a table
    DeleteTable { txn: TransactionState, table: Cow<'a, str> },

    /// Creates an index
    CreateIndex { txn: TransactionState, index: Index },
    /// Delete an index
    DeleteIndex { txn: TransactionState, table: Cow<'a, str>, index: Cow<'a, str> },

    /// Inserts a new table row
    Insert { txn: TransactionState, table: Cow<'a, str>, row: Row },
    /// Deletes a table row
    Delete { txn: TransactionState, table: Cow<'a, str>, pk: PrimaryKey },
    /// drop table data
    Drop { txn: TransactionState, table: Cow<'a, str> },
}

/// A transactional access engine based on Raft state machine.
pub struct Raft {
    client: Client,
}

impl Raft {
    /// Creates a Raft based transactional access engine
    #[allow(dead_code)]
    pub fn new(server: Arc<Server>) -> Raft {
        Raft { client: Client::new(server) }
    }

    /// Creates an underlying Raft state machine for the Raft machine
    #[allow(dead_code)]
    pub fn new_state<T: Storage + 'static>(kv: T) -> Box<dyn raft::State> {
        Box::new(State::new(kv))
    }
}

impl Engine for Raft {
    type Transaction = RaftTxn;

    fn begin(&self) -> Result<Self::Transaction> {
        RaftTxn::begin(self.client.clone(), false, None)
    }

    fn begin_read_only(&self) -> Result<Self::Transaction> {
        RaftTxn::begin(self.client.clone(), true, None)
    }

    fn begin_as_of(&self, version: u64) -> Result<Self::Transaction> {
        RaftTxn::begin(self.client.clone(), false, Some(version))
    }
}

pub struct RaftTxn {
    client: Client,
    state: TransactionState,
}

impl RaftTxn {
    fn begin(client: Client, read_only: bool, as_of: Option<u64>) -> Result<Self> {
        let state = client.mutate(Mutation::Begin { read_only, as_of })?;
        Ok(RaftTxn { client, state })
    }
}

impl Catalog for RaftTxn {
    fn read_table(&self, table: &str) -> Result<Option<Schema>> {
        self.client.query(Query::ReadTable { txn: self.state.clone(), table: table.into() })
    }

    fn create_table(&self, schema: Schema) -> Result<()> {
        self.client.mutate(Mutation::CreateTable { txn: self.state.clone(), schema })
    }

    fn delete_table(&self, table: &str) -> Result<()> {
        self.client.mutate(Mutation::DeleteTable { txn: self.state.clone(), table: table.into() })
    }

    fn scan_tables(&self) -> Result<Schemas> {
        let tables: Vec<Schema> =
            self.client.query(Query::ScanTables { txn: self.state.clone() })?;
        Ok(Box::new(tables.into_iter()))
    }

    fn read_index(&self, index: &str, table: &str) -> Result<Option<Index>> {
        self.client.query(Query::ReadIndex {
            txn: self.state.clone(),
            table: table.into(),
            index: index.into(),
        })
    }

    fn create_index(&self, index: Index) -> Result<()> {
        self.client.mutate(Mutation::CreateIndex { txn: self.state.clone(), index })
    }

    fn delete_index(&self, index: &str, table: &str) -> Result<()> {
        self.client.mutate(Mutation::DeleteIndex {
            txn: self.state.clone(),
            table: table.into(),
            index: index.into(),
        })
    }

    fn scan_table_indexes(&self, table: &str) -> Result<Indexes> {
        self.client.query(Query::ScanIndexes { txn: self.state.clone(), table: table.into() })
    }
}

impl Transaction for RaftTxn {
    fn version(&self) -> u64 {
        self.state.version
    }

    fn read_only(&self) -> bool {
        self.state.read_only
    }

    fn commit(&self) -> Result<()> {
        self.client.mutate(Mutation::Commit(self.state.clone()))
    }

    fn rollback(&self) -> Result<()> {
        self.client.mutate(Mutation::Rollback(self.state.clone()))
    }

    fn insert(&self, table: &str, row: Row) -> Result<PrimaryKey> {
        self.client.mutate(Mutation::Insert { txn: self.state.clone(), table: table.into(), row })
    }

    fn delete(&self, table: &str, pk: &PrimaryKey) -> Result<()> {
        self.client.mutate(Mutation::Delete {
            txn: self.state.clone(),
            table: table.into(),
            pk: pk.clone(),
        })
    }

    fn read(&self, table: &str, pk: &PrimaryKey) -> Result<Option<Row>> {
        self.client.query(Query::Read {
            txn: self.state.clone(),
            table: table.into(),
            pk: pk.clone(),
        })
    }

    fn scan(&self, table: &str, predicate: Option<Predicate>) -> Result<Scan> {
        let result: Vec<Result<Row>> = self.client.query(Query::Scan {
            txn: self.state.clone(),
            table: table.into(),
            predicate,
        })?;
        Ok(Box::new(result.into_iter()))
    }

    fn drop(&self, table: &str) -> Result<()> {
        self.client.mutate(Mutation::Drop { txn: self.state.clone(), table: table.into() })
    }

    fn read_index_entry(
        &self,
        table: &str,
        index: &str,
        index_key: IndexKey,
    ) -> Result<Option<Vec<Row>>> {
        self.client.query(Query::ReadIndexEntry {
            txn: self.state.clone(),
            table: table.into(),
            index: index.into(),
            value: index_key,
        })
    }

    fn scan_index_entries(&self, table: &str, index: &str) -> Result<IndexScan> {
        let result: Vec<(IndexKey, Vec<Row>)> = self.client.query(Query::ScanIndexEntries {
            txn: self.state.clone(),
            table: table.into(),
            index: index.into(),
        })?;
        Ok(Box::new(result.into_iter()))
    }
}

/// A client request to Raft state machine.
#[derive(Debug, Serialize, Deserialize)]
enum Request {
    Query(Vec<u8>),
    Mutate(Vec<u8>),
}

/// A client response from Raft state machine.
#[derive(Debug, Serialize, Deserialize)]
enum Response {
    Query(Vec<u8>),
    Mutate(Vec<u8>),
}

/// A client for the local Raft node.
#[derive(Clone)]
struct Client {
    server: Arc<Server>,
}

impl Client {
    /// Creates a new Raft client.
    fn new(server: Arc<Server>) -> Client {
        Client { server }
    }

    /// Executes a request against the Raft cluster.
    fn execute(&self, request: Request) -> Result<Response> {
        let cmd = Command(Some(bincodec::serialize(&request)?));
        let res = self.server.execute_command(cmd, None)?;
        match res {
            CommandResult::Applied { index: _index, result } => {
                let cmd: Vec<u8> =
                    result?.ok_or(Error::value("Unexpected Raft execute result None"))?;
                Ok(bincodec::deserialize(&cmd)?)
            }
            _ => Err(Error::value(format!("Unexpected Raft execute command result {:?}", res))),
        }
    }

    /// Queries the Raft state machine, deserializing the response into the return type.
    fn query<V: DeserializeOwned>(&self, query: Query) -> Result<V> {
        match self.execute(Request::Query(bincodec::serialize(&query)?))? {
            Response::Query(response) => Ok(bincodec::deserialize(&response)?),
            resp => Err(Error::internal(format!("Unexpected Raft query response {:?}", resp))),
        }
    }

    /// Mutates the Raft state machine, deserializing the response into the return type.
    fn mutate<V: DeserializeOwned>(&self, mutation: Mutation) -> Result<V> {
        match self.execute(Request::Mutate(bincodec::serialize(&mutation)?))? {
            Response::Mutate(response) => Ok(bincodec::deserialize(&response)?),
            resp => Err(Error::internal(format!("Unexpected Raft mutation response {:?}", resp))),
        }
    }
}

/// The Raft state machine for the Raft-based access engine, using a KV access engine
#[derive(Debug)]
pub struct State<T: Storage> {
    /// The underlying KV access engine
    engine: Kv<T>,
}

impl<T: Storage> State<T> {
    fn new(kv: T) -> State<T> {
        State { engine: Kv::new(kv) }
    }

    fn query(&self, query: Query) -> Result<Command> {
        let res = match query {
            Query::ScanTables { txn } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.scan_tables()?.collect::<Vec<_>>())
            }
            Query::ReadTable { txn, table } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.read_table(&table)?)
            }
            Query::ScanIndexes { txn, table } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.scan_table_indexes(&table)?)
            }
            Query::ReadIndex { txn, table, index } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.read_index(&index, &table)?)
            }
            Query::Read { txn, table, pk } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.read(&table, &pk)?)
            }
            Query::Scan { txn, table, predicate } => {
                let txn = self.engine.resume(txn)?;
                // FIXME These need to stream rows somehow
                bincodec::serialize(&txn.scan(&table, predicate)?.collect::<Vec<_>>())
            }
            Query::ReadIndexEntry { txn, table, index, value } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.read_index_entry(&table, &index, value)?)
            }
            Query::ScanIndexEntries { txn, table, index } => {
                let txn = self.engine.resume(txn)?;
                // FIXME These need to stream rows somehow
                bincodec::serialize(&txn.scan_index_entries(&table, &index)?.collect::<Vec<_>>())
            }
        };
        res.and_then(|it| Ok(Command::from(it)))
    }

    fn mutate(&self, mutation: Mutation) -> Result<Command> {
        let res = match mutation {
            Mutation::Begin { read_only, as_of } => {
                if read_only {
                    let txn = self.engine.begin_read_only()?;
                    bincodec::serialize(&txn.state())
                } else if let Some(as_of) = as_of {
                    let txn = self.engine.begin_as_of(as_of)?;
                    bincodec::serialize(&txn.state())
                } else {
                    let txn = self.engine.begin()?;
                    bincodec::serialize(&txn.state())
                }
            }
            Mutation::Commit(txn) => bincodec::serialize(&self.engine.resume(txn)?.commit()?),
            Mutation::Rollback(txn) => bincodec::serialize(&self.engine.resume(txn)?.rollback()?),
            Mutation::CreateTable { txn, schema } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.create_table(schema)?)
            }
            Mutation::DeleteTable { txn, table } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.delete_table(&table)?)
            }
            Mutation::CreateIndex { txn, index } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.create_index(index)?)
            }
            Mutation::DeleteIndex { txn, table, index } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.delete_index(&index, &table)?)
            }
            Mutation::Insert { txn, table, row } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.insert(&table, row)?)
            }
            Mutation::Delete { txn, table, pk } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.delete(&table, &pk)?)
            }
            Mutation::Drop { txn, table } => {
                let txn = self.engine.resume(txn)?;
                bincodec::serialize(&txn.drop(&table)?)
            }
        };
        res.and_then(|it| Ok(Command::from(it)))
    }
}

impl<T: Storage> raft::State for State<T> {
    fn apply(&self, msg: ApplyMsg) -> Result<Command> {
        let cmd = msg.command.ok_or(Error::internal("Unexpected Raft command"))?;
        let request: Request = bincodec::deserialize(&cmd)?;
        match request {
            // FIXME: query is considered as ready-only op, should not
            //  go though raft log replication.
            Request::Query(query) => self.query(bincodec::deserialize(&query)?),
            Request::Mutate(mutation) => self.mutate(bincodec::deserialize(&mutation)?),
        }
    }
}

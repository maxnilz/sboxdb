use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::SinkExt;
use log::debug;
use log::error;
use log::info;
use serde::Deserialize;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::try_join;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

use crate::access::engine::Engine;
use crate::access::engine::Transaction;
use crate::access::kv::Kv;
use crate::access::raft::Raft;
use crate::access::raft::State;
use crate::config::Config;
use crate::error::Error;
use crate::error::Result;
use crate::raft;
use crate::raft::log::Log;
use crate::raft::node::NodeId;
use crate::raft::transport::TcpTransport;
use crate::raft::transport::Transport;
use crate::sql::execution::compiler::Compiler;
use crate::sql::execution::context::Context;
use crate::sql::execution::context::ExecContext;
use crate::sql::execution::ExecutionEngine;
use crate::sql::execution::ResultSet;
use crate::sql::parser::ast::Statement;
use crate::sql::parser::Parser;
use crate::sql::plan::planner::BindContext;
use crate::sql::plan::planner::Planner;
use crate::storage::memory::Memory;
use crate::storage::new_storage;

#[async_trait]
pub trait Server {
    async fn serve(&mut self) -> Result<()>;
}

/// Server in raft-backed cluster mode.
pub struct ClusterServer {
    raft_server: Arc<raft::server::Server>,
    raft_engine: Raft,
    sql_listener: TcpListenerStream,
}

impl ClusterServer {
    pub async fn try_new(config: Config) -> Result<ClusterServer> {
        let storage = Memory::new();
        let state = Box::new(State::new(storage));
        let raft_server = Arc::new(Self::try_new_raft_server(&config, state)?);
        let raft_engine = Raft::new(Arc::clone(&raft_server));
        let sql_listener =
            TcpListenerStream::new(TcpListener::bind(&config.sql_listen_addr).await?);
        Ok(ClusterServer { raft_server, raft_engine, sql_listener })
    }

    async fn serve_sql(&mut self, _done: broadcast::Receiver<()>) -> Result<()> {
        while let Some(socket) = self.sql_listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = Session::new(self.raft_engine.clone());
            tokio::spawn(async move {
                info!("Client {} connected", peer);
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }
        Ok(())
    }

    fn try_new_raft_server(
        config: &Config,
        state: Box<dyn raft::State>,
    ) -> Result<raft::server::Server> {
        let raft_peers: HashMap<NodeId, SocketAddr> = config
            .peers
            .iter()
            .map(|(node_id, addr)| {
                let addr = SocketAddr::from_str(&addr)?;
                Ok((node_id.clone(), addr))
            })
            .collect::<Result<_>>()?;
        let my_addr = raft_peers
            .get(&config.id)
            .ok_or(Error::internal(format!("node {} not found in peers", config.id)))?;
        let me = (config.id, my_addr.clone());
        let raft_peers = raft_peers.into_iter().collect();
        let raft_transport: Box<dyn Transport> = Box::new(TcpTransport::new(me, raft_peers)?);
        let raft_log = Log::new(config.id, new_storage(config.raft_storage_type)?)?;
        raft::server::Server::try_new(raft_log, raft_transport, state)
    }
}

#[async_trait]
impl Server for ClusterServer {
    async fn serve(&mut self) -> Result<()> {
        info!(
            "Listening on {} (SQL) and {} (Raft)",
            self.sql_listener.as_ref().local_addr()?,
            self.raft_server.local_addr()?
        );
        let raft_server = Arc::clone(&self.raft_server);
        let (_, rx) = broadcast::channel(1);
        try_join!(raft_server.serve(rx.resubscribe()), self.serve_sql(rx))?;
        Ok(())
    }
}

/// Standalone server
pub struct StandaloneServer {
    engine: Kv<Memory>,
    sql_listener: TcpListenerStream,
}

impl StandaloneServer {
    pub async fn try_new(config: Config) -> Result<StandaloneServer> {
        let engine: Kv<Memory> = Kv::new(Memory::new());
        let sql_listener =
            TcpListenerStream::new(TcpListener::bind(&config.sql_listen_addr).await?);
        Ok(StandaloneServer { engine, sql_listener })
    }

    async fn serve_sql(&mut self, _done: broadcast::Receiver<()>) -> Result<()> {
        while let Some(socket) = self.sql_listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = Session::new(self.engine.clone());
            tokio::spawn(async move {
                info!("Client {} connected", peer);
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }
        Ok(())
    }
}

#[async_trait]
impl Server for StandaloneServer {
    async fn serve(&mut self) -> Result<()> {
        info!("Standalone server listening on {}", self.sql_listener.as_ref().local_addr()?,);
        let (_, rx) = broadcast::channel(1);
        try_join!(self.serve_sql(rx))?;
        Ok(())
    }
}

/// A client request.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Query(String),
}

/// A server response.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Query(ResultSet),
    Error(Error),
}

type FramedStream = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Request,
    Response,
    tokio_serde::formats::Bincode<Request, Response>,
>;

/// A session for incoming queries from client connection.
struct Session<E: Engine> {
    engine: E,

    planner: Planner,
    compiler: Compiler,
    vector_size: usize,

    txn: Option<Arc<dyn Transaction>>,
}

impl<E: Engine + 'static> Session<E> {
    fn new(engine: E) -> Self {
        Self {
            engine,
            planner: Planner::new(),
            compiler: Compiler::new(),
            vector_size: 10,
            txn: None,
        }
    }

    /// Handles a client connection.
    async fn handle(mut self, socket_stream: TcpStream) -> Result<()> {
        let length_delimited = Framed::new(socket_stream, LengthDelimitedCodec::new());
        let codec = tokio_serde::formats::Bincode::default();
        let mut framed_stream: FramedStream = tokio_serde::Framed::new(length_delimited, codec);
        while let Some(request) = framed_stream.try_next().await? {
            let res = tokio::task::block_in_place(|| self.process(request))?;
            framed_stream.send(res).await?;
        }
        Ok(())
    }

    /// Process request
    fn process(&mut self, request: Request) -> Result<Response> {
        debug!("Processing request {:?}", request);
        let resp = match request {
            Request::Query(query) => {
                let res = self.process_query(query);
                match res {
                    Ok(rs) => Response::Query(rs),
                    Err(err) => Response::Error(err),
                }
            }
        };
        Ok(resp)
    }

    /// Process query
    fn process_query(&mut self, query: String) -> Result<ResultSet> {
        let stmt = self.parse_query(query)?;
        match stmt {
            Statement::Begin { .. } if self.txn.is_some() => {
                Err(Error::value("Already in a transaction"))
            }
            Statement::Begin { read_only: true, as_of: None } => {
                let txn = self.engine.begin_read_only()?;
                self.txn = Some(Arc::new(txn));
                Ok(ResultSet::from(self.must_txn().as_ref()))
            }
            Statement::Begin { read_only: true, as_of: Some(version) } => {
                let txn = self.engine.begin_as_of(version)?;
                self.txn = Some(Arc::new(txn));
                Ok(ResultSet::from(self.must_txn().as_ref()))
            }
            Statement::Begin { read_only: false, as_of: Some(_) } => {
                Err(Error::value("Can't start read-write transaction in a given version"))
            }
            Statement::Begin { read_only: false, as_of: None } => {
                let txn = self.engine.begin()?;
                self.txn = Some(Arc::new(txn));
                Ok(ResultSet::from(self.must_txn().as_ref()))
            }
            Statement::Commit | Statement::Rollback if self.txn.is_none() => {
                Err(Error::value("Not in a transaction"))
            }
            Statement::Commit => {
                let txn = self.must_txn();
                txn.commit()?;
                Ok(ResultSet::from(txn.as_ref()))
            }
            Statement::Rollback => {
                let txn = self.must_txn();
                txn.rollback()?;
                Ok(ResultSet::from(txn.as_ref()))
            }
            stmt if self.txn.is_some() => self.execute_stmt(stmt),
            stmt => self.execute_auto_stmt(stmt),
        }
    }

    /// Execute the statement that have no explicit transaction wrapped
    /// around by start & attach an implicit txn then detach it afterward.
    fn execute_auto_stmt(&mut self, stmt: Statement) -> Result<ResultSet> {
        let txn = match stmt {
            Statement::Select { .. } => self.engine.begin_read_only(),
            _ => self.engine.begin(),
        }?;
        self.txn = Some(Arc::new(txn));
        let res = self.execute_stmt(stmt);
        let result = match res {
            Ok(rs) => {
                self.must_txn().commit()?;
                Ok(rs)
            }
            Err(err) => {
                self.must_txn().rollback()?;
                Err(err)
            }
        };
        result
    }

    /// Execute a statement inside a transaction.
    fn execute_stmt(&mut self, stmt: Statement) -> Result<ResultSet> {
        let txn = self.must_txn();
        let catalog = Arc::clone(txn);
        let mut ctx = BindContext::new(catalog);
        let plan = self.planner.sql_statement_to_plan(&mut ctx, stmt)?;
        let executor = self.compiler.build_execution_plan(plan)?;
        let ctx: &mut dyn Context = &mut ExecContext::new(Arc::clone(txn), self.vector_size);
        ExecutionEngine::execute(ctx, executor)
    }

    fn must_txn(&self) -> &Arc<dyn Transaction> {
        self.txn.as_ref().unwrap()
    }

    fn parse_query(&self, query: String) -> Result<Statement> {
        let mut parser = Parser::new(&query)?;
        parser.parse_statement()
    }
}

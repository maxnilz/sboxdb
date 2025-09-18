use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
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
use crate::session::Session;
use crate::sql::execution::ResultSet;
use crate::storage::memory::Memory;
use crate::storage::new_storage;

#[async_trait]
pub trait Server {
    async fn serve(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

/// Server in raft-backed cluster mode.
pub struct ClusterServer {
    raft_server: Arc<raft::server::Server>,
    raft_engine: Raft,
    sql_listener: TcpListenerStream,

    closec: broadcast::Sender<()>,
    donec: broadcast::Receiver<()>,
}

impl ClusterServer {
    pub async fn try_new(config: Config) -> Result<ClusterServer> {
        let storage = Memory::new();
        let state = Box::new(State::new(storage));
        let raft_server = Arc::new(Self::try_new_raft_server(&config, state)?);
        let raft_engine = Raft::new(Arc::clone(&raft_server));
        let sql_listener =
            TcpListenerStream::new(TcpListener::bind(&config.sql_listen_addr).await?);
        let (closec, donec) = broadcast::channel(1);
        Ok(ClusterServer { raft_server, raft_engine, sql_listener, closec, donec })
    }

    async fn serve_sql(&mut self, _done: broadcast::Receiver<()>) -> Result<()> {
        while let Some(socket) = self.sql_listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = SocketSession::new(self.raft_engine.clone());
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
                let addr = addr
                    .to_socket_addrs()?
                    .next()
                    .ok_or(Error::value(format!("resolve {} failed", addr)))?;
                Ok((node_id.clone(), addr))
            })
            .collect::<Result<_>>()?;
        let my_listen_addr = config
            .raft_listen_addr
            .to_socket_addrs()?
            .next()
            .ok_or(Error::value(format!("invalid raft listen addr {}", config.raft_listen_addr)))?;
        let raft_peers = raft_peers.into_iter().collect();
        let me = (config.id, my_listen_addr);
        let raft_transport: Box<dyn Transport> = Box::new(TcpTransport::try_new(me, raft_peers)?);
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
        try_join!(
            raft_server.serve(self.donec.resubscribe()),
            self.serve_sql(self.donec.resubscribe())
        )?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        match self.closec.send(()) {
            Ok(_) => {}
            Err(err) => return Err(err.into()),
        }
        // TODO: wait tasks done before consider close is done.
        Ok(())
    }
}

/// Standalone server
pub struct StandaloneServer {
    engine: Kv<Memory>,
    sql_listener: TcpListenerStream,

    closec: broadcast::Sender<()>,
    donec: broadcast::Receiver<()>,
}

impl StandaloneServer {
    pub async fn try_new(config: Config) -> Result<StandaloneServer> {
        let engine: Kv<Memory> = Kv::new(Memory::new());
        let sql_listener =
            TcpListenerStream::new(TcpListener::bind(&config.sql_listen_addr).await?);
        let (closec, donec) = broadcast::channel(1);
        Ok(StandaloneServer { engine, sql_listener, closec, donec })
    }

    async fn serve_sql(&mut self, _done: broadcast::Receiver<()>) -> Result<()> {
        while let Some(socket) = self.sql_listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = SocketSession::new(self.engine.clone());
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
        try_join!(self.serve_sql(self.donec.resubscribe()))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        match self.closec.send(()) {
            Ok(_) => {}
            Err(err) => return Err(err.into()),
        }
        // TODO: wait tasks done before consider close is done.
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

/// A session for incoming queries from tcp socket connection.
struct SocketSession<E: Engine> {
    session: Session<E>,
}

impl<E: Engine + 'static> SocketSession<E> {
    fn new(engine: E) -> Self {
        Self { session: Session::new(engine) }
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
                let res = self.session.process_query(query);
                match res {
                    Ok(rs) => Response::Query(rs),
                    Err(err) => Response::Error(err),
                }
            }
        };
        Ok(resp)
    }
}

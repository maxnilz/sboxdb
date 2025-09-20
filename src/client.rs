use futures::SinkExt;
use futures::TryStreamExt;
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

use crate::error::Result;
use crate::internal_err;
use crate::server::Request;
use crate::server::Response;
use crate::sql::execution::ResultSet;

type FramedStream = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Response,
    Request,
    tokio_serde::formats::Bincode<Response, Request>,
>;

pub struct Client {
    conn: FramedStream,
}

impl Client {
    pub async fn try_new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let socket_stream = TcpStream::connect(addr).await?;
        let length_delimited = Framed::new(socket_stream, LengthDelimitedCodec::new());
        let codec = tokio_serde::formats::Bincode::default();
        let conn: FramedStream = tokio_serde::Framed::new(length_delimited, codec);
        Ok(Self { conn })
    }

    /// Executes a query
    pub async fn execute_query(&mut self, query: String) -> Result<ResultSet> {
        let resp = self.send_request(Request::Query(query)).await?;
        match resp {
            Response::Query(rs) => Ok(rs),
            Response::Error(err) => Err(err),
        }
    }

    async fn send_request(&mut self, request: Request) -> Result<Response> {
        self.conn.send(request).await?;
        match self.conn.try_next().await? {
            Some(resp) => Ok(resp),
            None => Err(internal_err!("Server disconnected")),
        }
    }
}

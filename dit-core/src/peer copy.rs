//! Peer-to-peer communication protocol and distributed hash table implementation.
//!
//! The current implementation is based on chord.

use bytes::BytesMut;
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::io::{self, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

/// This address uniquely identifies peers and data stored on the distributed hash table.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct DhtAddr(pub [u8; Self::BYTE_LEN]);

impl DhtAddr {
    pub const BYTE_LEN: usize = 32;

    pub fn hash(data: &[u8]) -> Self {
        let hash = digest(&SHA256, data)
            .as_ref()
            .try_into()
            .expect("hash has wrong length");
        Self(hash)
    }
}

impl fmt::Display for DhtAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for DhtAddr {
    type Err = ParseDhtAddrError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() != 2 * Self::BYTE_LEN {
            return Err(ParseDhtAddrError(()));
        }

        let mut output = [0; Self::BYTE_LEN];
        for (index, tuple) in input.as_bytes().chunks_exact(2).enumerate() {
            let high = match tuple[0] {
                c @ b'0'..=b'9' => c - b'0',
                c @ b'A'..=b'F' => c - (b'A' - 10),
                c @ b'a'..=b'f' => c - (b'a' - 10),
                _ => return Err(ParseDhtAddrError(())),
            };
            let low = match tuple[1] {
                c @ b'0'..=b'9' => c - b'0',
                c @ b'A'..=b'F' => c - (b'A' - 10),
                c @ b'a'..=b'f' => c - (b'a' - 10),
                _ => return Err(ParseDhtAddrError(())),
            };
            output[index] = (high << 4) | low
        }

        Ok(Self(output))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseDhtAddrError(());

#[derive(Debug, Serialize, Deserialize)]
pub struct DhtAndSocketAddr {
    pub dht_addr: DhtAddr,
    pub socket_addr: SocketAddr,
}

#[derive(Debug)]
pub struct Config {
    pub addr: DhtAndSocketAddr,
    pub ttl: u32,
    pub query_queue_size: usize,
}

pub type ClientId = u64;

#[derive(Debug)]
pub struct LocalPeer {}

impl LocalPeer {
    pub fn new(config: Config) -> Self { }

    pub async fn listener(&self) -> io::Result<Listener> { }

    pub fn controller(&self) -> Controller { }

    #[tracing::instrument]
    pub async fn run(mut self) { }
}

#[derive(Debug, Clone)]
pub struct Controller { }

impl Controller {
    /// Completes when the connection to the [`LocalPeer`] has been closed.
    pub async fn closed(&self) {
        self.query_sender.closed().await
    }

    /// Signals the [`LocalPeer`] to shut down and completes after all [`Controller`]s have been dropped.
    pub async fn shutdown(self) -> QueryResult<()> { }

    pub async fn find_owners(&self, addr: DhtAddr) -> io::Result<Vec<SocketAddr>> {
        let subscription = query(Subscribe(addr)).await?;
        loop {
            let neighbours = query(Neighbours).await?;
            let stream = connect(neighbours.succ).await?;
            stream.send(FindOwners(addr)).await?;
            if let Ok(res) = subscription.timeout(0).await? {
                return Ok(res)
            }
        }
    }

    async fn send_packet(&self, packet: Payload) -> io::Result<()> {}

}

#[derive(Debug)]
pub struct Listener {
    controller: Controller,
    listener: TcpListener,
}

impl Listener {
    pub async fn accept(&self) -> io::Result<Option<RemotePeer>> {
        tokio::select! {
            biased;
            () = self.controller.closed() => Ok(None),
            result = self.listener.accept() => {
                let (stream, socket_addr) = result?;
                let Ok(id) = self.controller.send_remote_connect(socket_addr).await else {
                    return Ok(None);
                };
                let controller = self.controller.clone();
                Ok(Some(RemotePeer { controller, id, stream }))
            }
        }
    }
}

#[derive(Debug)]
pub struct RemotePeer {
    controller: Controller,
    id: ClientId,
    stream: TcpStream,
}

impl RemotePeer {
    #[tracing::instrument]
    pub async fn run(mut self) -> io::Result<()> {
        let mut bytes = BytesMut::new();
        loop {
            tokio::select! {
                biased;
                () = self.controller.closed() => return Ok(()),
                // TODO: decode packets here
                result = self.stream.read_buf(&mut bytes) => {
                    let count = result?;
                    if count == 0 {
                        let _ = self.controller.send_remote_disconnect(self.id).await;
                        return Ok(());
                    }
                    tracing::info!("got bytes: {bytes:?}")
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionClosed(());
pub type QueryResult<T> = Result<T, ConnectionClosed>;

#[derive(Debug)]
enum Query {
    Shutdown(oneshot::Sender<mpsc::Receiver<()>>),
    RemoteConnect(oneshot::Sender<ClientId>, SocketAddr),
}

#[derive(Debug)]
enum Event {
    RemoteDisconnect(ClientId),
}

#[derive(Debug, Serialize, Deserialize)]
struct Packet {
    src: DhtAndSocketAddr,
    dst: DhtAddr,
    ttl: u32,
    payload: Payload,
}

#[derive(Debug, Serialize, Deserialize)]
enum Payload {
    Ping(u64),
    Pong(u64),
    DhtPut(SocketAddr),
    DhtGet(Vec<SocketAddr>),
    NeighborsRequest,
    NeighborsResponse(Neighbors),
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Neighbors {
    pred: Option<DhtAndSocketAddr>,
    succ: Option<DhtAndSocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_value() {
        let addr = DhtAddr::hash(b"hello");
        assert_eq!(
            format!("{addr}"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            Ok(addr),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".parse()
        );
    }
}

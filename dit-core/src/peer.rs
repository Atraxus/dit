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
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};

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

    pub fn wrapping_sub(mut self, other: Self) -> Self {
        let mut carry = false;
        for (self_byte, other_byte) in self.0.iter_mut().zip(other.0).rev() {
            let (result_1, overflow_1) = self_byte.overflowing_sub(other_byte);
            let (result_2, overflow_2) = result_1.overflowing_sub(carry.into());
            *self_byte = result_2;
            carry = overflow_1 || overflow_2;
        }
        self
    }

    pub fn wrapping_distance(self, other: Self) -> Self {
        if self >= other {
            self.wrapping_sub(other)
        } else {
            other.wrapping_sub(self)
        }
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

/// Uniquely identifies a connection between a [`LocalPeer`] and [`RemotePeer`].
pub type ConnId = u64;

#[derive(Debug)]
pub struct LocalPeer {
    controller: Controller,
    query_receiver: mpsc::Receiver<Query>,
    event_receiver: mpsc::UnboundedReceiver<Event>,
    shutdown_sender: watch::Sender<bool>,
    next_conn_id: ConnId,
    inbound_conns: HashMap<ConnId, SocketAddr>,
    neighbors: Neighbors,
}

impl LocalPeer {
    pub fn new(config: Config) -> Self {
        let (query_sender, query_receiver) = mpsc::channel(config.query_queue_size);
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);

        let controller = Controller {
            config: Arc::new(config),
            query_sender,
            event_sender,
            shutdown_receiver,
        };

        Self {
            controller,
            query_receiver,
            event_receiver,
            shutdown_sender,
            next_conn_id: 0,
            inbound_conns: HashMap::default(),
            neighbors: Neighbors::default(),
        }
    }

    pub async fn listener(&self) -> io::Result<Listener> {
        let listener = TcpListener::bind(self.controller.config.addr.socket_addr).await?;
        let controller = self.controller();
        Ok(Listener {
            controller,
            listener,
        })
    }

    pub fn controller(&self) -> Controller {
        self.controller.clone()
    }

    #[tracing::instrument]
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                event = self.event_receiver.recv() => {
                    match event {
                        None => {
                            // Graceful shutdown completes when all controllers are dropped.
                            let _ = self.shutdown_sender.send(true);
                            return;
                        }
                        Some(Event::Shutdown) => {
                            // Initiate graceful shutdown by closing bidirectional messages.
                            // This completes `controller.closed()`.
                            self.query_receiver.close();
                        }
                        Some(Event::RemoteDisconnect(id)) => {
                            self.process_remote_disconnect(id);
                        }
                    }
                }
                Some(query) = self.query_receiver.recv() => {
                    match query {
                        Query::RemoteConnect(response, socket_addr) => {
                            let _ = response.send(self.process_remote_connect(socket_addr));
                        }
                        Query::GetNeighbors(response) => {
                            let _ = response.send(self.process_get_neighbors());
                        }
                        Query::UpdateNeighbors(response, addrs) => {
                            let _ = response.send(self.process_update_neighbors(addrs));
                        }
                    }
                }
            }
        }
    }

    fn process_remote_disconnect(&mut self, id: ConnId) {
        let socket_addr = self
            .inbound_conns
            .remove(&id)
            .expect("disconnected without connection");
        tracing::info!(id, ?socket_addr, "remote disconnected");
    }

    fn process_remote_connect(&mut self, socket_addr: SocketAddr) -> ConnId {
        let id = self.next_conn_id;
        self.next_conn_id = id.checked_add(1).expect("connection id overflow");

        tracing::info!(id, ?socket_addr, "incoming connection");

        self.inbound_conns.insert(id, socket_addr);
        id
    }

    fn process_get_neighbors(&mut self) -> Neighbors {
        self.neighbors
    }

    fn process_update_neighbors(&mut self, new: DhtAndSocketAddr) {
        let new_is_closer = |old: DhtAndSocketAddr| {
            self.controller.config.addr.dht_addr.wrapping_distance(new.dht_addr)
                < self.controller.config.addr.dht_addr.wrapping_distance(old.dht_addr)
        };

        if self.neighbors.pred.map_or(true, new_is_closer) {
            self.neighbors.pred = Some(new)
        }

        if self.neighbors.succ.map_or(true, new_is_closer) {
            self.neighbors.succ = Some(new)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Controller {
    config: Arc<Config>,
    query_sender: mpsc::Sender<Query>,
    event_sender: mpsc::UnboundedSender<Event>,
    shutdown_receiver: watch::Receiver<bool>,
}

impl Controller {
    /// Completes when the connection to the [`LocalPeer`] has been closed.
    pub async fn closed(&self) {
        self.query_sender.closed().await
    }

    /// Signals the [`LocalPeer`] to shut down and completes after all [`Controller`]s have been dropped.
    pub async fn shutdown(self) -> Response<()> {
        self.event(Event::Shutdown)?;

        // Shutdown completes when all senders are dropped.
        let Controller {
            mut shutdown_receiver,
            ..
        } = self;
        shutdown_receiver
            .wait_for(|&x| x)
            .await
            .map_err(|_| ConnectionClosed(()))?;
        Ok(())
    }

    /// Bootstraps the [`LocalPeer`] by sending a `GetNeighbors` packet to its own [`SocketAddr`].
    pub fn bootstrap(&self, bootstrap_addr: SocketAddr) -> io::Result<()> {
        todo!()
    }

    /// Stores the [`LocalPeer`]s [`SocketAddr`] in the DHT at [`DhtAddr`].
    pub async fn announce(&self, dht_addr: DhtAddr) -> io::Result<()> {
        todo!()
    }

    fn event_remote_disconnect(&self, id: ConnId) -> Response<()> {
        self.event(Event::RemoteDisconnect(id))
    }

    async fn query_remote_connect(&self, socket_addr: SocketAddr) -> Response<ConnId> {
        self.query(|response| Query::RemoteConnect(response, socket_addr))
            .await
    }

    async fn query_get_neighbors(&self) -> Response<Neighbors> {
        self.query(|response| Query::GetNeighbors(response)).await
    }

    async fn query_update_neighbors(&self, addrs: DhtAndSocketAddr) -> Response<()> {
        self.query(|response| Query::UpdateNeighbors(response, addrs))
            .await
    }

    fn event(&self, event: Event) -> Response<()> {
        self.event_sender
            .send(event)
            .map_err(|_| ConnectionClosed(()))
    }

    async fn query<T>(&self, mk_query: impl FnOnce(oneshot::Sender<T>) -> Query) -> Response<T> {
        let (response_sender, response_receiver) = oneshot::channel();
        let query = mk_query(response_sender);
        self.query_sender
            .send(query)
            .await
            .map_err(|_| ConnectionClosed(()))?;
        response_receiver.await.map_err(|_| ConnectionClosed(()))
    }

    async fn packet(&self, packet: Packet) -> io::Result<()> {
        // TODO: serialize and send packet
        todo!()
    }
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
                let Ok(id) = self.controller.query_remote_connect(socket_addr).await else {
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
    id: ConnId,
    stream: TcpStream,
}

impl Drop for RemotePeer {
    fn drop(&mut self) {
        let _ = self.controller.event_remote_disconnect(self.id);
    }
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
                        // Disconnect is sent by drop.
                        return Ok(());
                    }
                    tracing::info!("got bytes: {bytes:?}")
                }
            }
        }
    }
}

/// Error returned by methods of [`Controller`] when the connection to the [`LocalPeer`] is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionClosed(());
pub type Response<T> = Result<T, ConnectionClosed>;

#[derive(Debug)]
enum Query {
    RemoteConnect(oneshot::Sender<ConnId>, SocketAddr),
    GetNeighbors(oneshot::Sender<Neighbors>),
    UpdateNeighbors(oneshot::Sender<()>, DhtAndSocketAddr),
}

#[derive(Debug)]
enum Event {
    Shutdown,
    RemoteDisconnect(ConnId),
}

/// A datagram that can be sent to a peer.
#[derive(Debug, Serialize, Deserialize)]
enum Packet {
    /// This packet targets a specific peer directly.
    Socket(GenericPayload),

    /// This packet should be routes though the overlay network.
    Dht(DhtPacket),
}

#[derive(Debug, Serialize, Deserialize)]
struct DhtPacket {
    src: DhtAndSocketAddr,
    dst: DhtAddr,
    ttl: u32,
    payload: DhtPayload,
}

#[derive(Debug, Serialize, Deserialize)]
enum GenericPayload {
    Ping(u64),
    Pong(u64),
    NeighborsRequest,
    NeighborsResponse(Neighbors),
}

#[derive(Debug, Serialize, Deserialize)]
enum DhtPayload {
    Generic(GenericPayload),

    /// Append the `SocketAddr` to the DHT entry at `dst`.
    PutRequest(SocketAddr),

    /// Indicates a successful put of the `SocketAddr` at `src.dht_addr`.
    PutResponse(SocketAddr),

    /// Requests the DHT entry at `dst`.
    GetRequest,

    /// Contains the DHT entry at `src.dht_addr`.
    GetResponse(Vec<SocketAddr>),
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct Neighbors {
    pred: Option<DhtAndSocketAddr>,
    succ: Option<DhtAndSocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dht_addr_hash() {
        let addr = DhtAddr::hash(b"hello");
        assert_eq!(
            format!("{addr}"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        );
    }

    #[test]
    fn dht_addr_parse() {
        let addr = DhtAddr([
            0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9,
            0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62,
            0x93, 0x8b, 0x98, 0x24,
        ]);
        assert_eq!(
            Ok(addr),
            "2CF24DBA5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".parse()
        );
    }

    #[test]
    fn dht_addr_sub() {
        let a = DhtAddr([
            0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9,
            0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62,
            0x93, 0x8b, 0x98, 0x24,
        ]);
        let b = DhtAddr([
            0x48, 0x6e, 0xa4, 0x62, 0x24, 0xd1, 0xbb, 0x4f, 0xb6, 0x80, 0xf3, 0x4f, 0x7c, 0x9a,
            0xd9, 0x6a, 0x8f, 0x24, 0xec, 0x88, 0xbe, 0x73, 0xea, 0x8e, 0x5a, 0x6c, 0x65, 0x26,
            0x0e, 0x9c, 0xb8, 0xa7,
        ]);
        let a_minus_b = DhtAddr([
            0xe4, 0x83, 0xa9, 0x58, 0x3a, 0xde, 0xe7, 0xbe, 0x70, 0x67, 0x47, 0xdb, 0x49, 0x1f,
            0x09, 0x33, 0x8b, 0xf1, 0x31, 0xd3, 0x61, 0x33, 0x57, 0xd0, 0x18, 0x97, 0xce, 0x3c,
            0x84, 0xee, 0xdf, 0x7d,
        ]);
        let b_minus_a = DhtAddr([
            0x1b, 0x7c, 0x56, 0xa7, 0xc5, 0x21, 0x18, 0x41, 0x8f, 0x98, 0xb8, 0x24, 0xb6, 0xe0,
            0xf6, 0xcc, 0x74, 0x0e, 0xce, 0x2c, 0x9e, 0xcc, 0xa8, 0x2f, 0xe7, 0x68, 0x31, 0xc3,
            0x7b, 0x11, 0x20, 0x83,
        ]);
        assert_eq!(a.wrapping_sub(a), DhtAddr([0; DhtAddr::BYTE_LEN]));
        assert_eq!(a.wrapping_sub(b), a_minus_b);
        assert_eq!(b.wrapping_sub(a), b_minus_a);
    }
}

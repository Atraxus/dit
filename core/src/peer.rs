//! Peer-to-peer communication protocol and distributed hash table implementation.
//!
//! The current implementation is based on chord.

use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tracing::Instrument;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct DhtAddr(pub [u8; 32]);

impl DhtAddr {
    pub fn hash(data: &[u8]) -> Self {
        let hash = digest(&SHA256, data)
            .as_ref()
            .try_into()
            .expect("hash has wrong length");
        Self(hash)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DhtAndSocketAddr {
    pub dht_addr: DhtAddr,
    pub socket_addr: SocketAddr,
}

#[derive(Debug)]
pub struct Config {
    pub addr: DhtAndSocketAddr,
    pub ttl: u32,
    pub channel_buffer_size: usize,
}

#[derive(Debug, Clone)]
pub struct Controller {
    sender: mpsc::Sender<Message>,
}

pub fn run_peer(config: Config) -> (Controller, impl Future<Output = io::Result<()>>) {
    let (sender, receiver) = mpsc::channel(config.channel_buffer_size);
    let state = State { config, receiver };
    let state = Arc::new(Mutex::new(state));
    let controller = Controller {
        sender
    };
    let server = peer_server(state);
    (controller, server)
}

pub async fn peer_server(state: Arc<Mutex<State>>) -> io::Result<()> {
    let listener = TcpListener::bind(config.addr.socket_addr).await?;
    loop {
        let (client_stream, client_addr) = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = listener.accept() => result?,
        };
        let client_cancel = cancel.child_token();
        tokio::spawn(async move {
            let span = tracing::info_span!("peer connection", ?client_addr);
            let _result = client_io(client_stream, client_cancel)
                .instrument(span)
                .await;
        });
    }
}

async fn client_io(client: TcpStream, cancel: CancellationToken) -> io::Result<()> {
    Ok(())
}

#[derive(Debug)]
struct State {
    config: Config,
    receiver: mpsc::Receiver<Message>,
}

#[derive(Debug)]
enum Message {}

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
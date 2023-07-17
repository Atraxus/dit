//! The dit daemon is the main entry point for the dit application. It is
//! responsible for creating the runtime consisting of a local peer, a tcp listener
//! and a controller. The listener also has a controller, which is used to
//! manage the remote peers that connect to the listener.

use crate::codec::Codec;
use crate::peer::Controller;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub socket_addr: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Packet {
    Ping(u64),
    Pong(u64),
    Bootstrap(SocketAddr),
    Status(Result<(), String>),
}

/// This struct represents the connection from the daemon to a process.
#[derive(Debug)]
pub struct ConnectionToProcess {
    stream: Framed<TcpStream, Codec<Packet>>,
    controller: Controller,
}

impl ConnectionToProcess {
    /// Run the inbound connection decoding packets from the stream and
    /// responding to them.
    pub async fn run(mut self) -> io::Result<()> {
        tracing::info!("Running inbound connection");

        // Get packets from stream
        while let Some(packet) = self.stream.next().await {
            match packet {
                Ok(Packet::Ping(value)) => {
                    tracing::info!("Received Ping with value: {}", value);

                    // Respond with Pong
                    self.stream.send(Packet::Pong(value)).await?;
                }
                Ok(Packet::Bootstrap(address)) => {
                    tracing::info!("Received Bootstrap with address: {}", address);

                    // Bootstrap the local peer
                    let result = self.controller.bootstrap(address).await;
                    tracing::debug!(?result);
                    let status = result.map_err(|err| err.to_string());
                    self.stream.send(Packet::Status(status)).await?;
                }
                Ok(packet) => {
                    tracing::warn!(?packet, "Received unexpected packet");
                    // Disconnect the client if they send an invalid packet so that
                    // they won't wait for us to respond.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected packet",
                    ));
                }
                Err(e) => {
                    tracing::error!("Failed to process packet: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

/// This struct represents the connection to the daemon from a process.
#[derive(Debug)]
pub struct ConnectionToDaemon {
    stream: Framed<TcpStream, Codec<Packet>>,
}

impl ConnectionToDaemon {
    /// Create a new connection to the daemon.
    pub async fn connect(address: SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let stream = Framed::new(stream, Codec::new(1024));

        Ok(Self { stream })
    }

    /// Send a ping packet to the daemon.
    pub async fn ping(&mut self, value: u64) -> io::Result<()> {
        self.stream.send(Packet::Ping(value)).await
    }

    /// Receive a packet from the daemon.
    pub async fn receive(&mut self) -> io::Result<Packet> {
        match self.stream.next().await {
            Some(Ok(packet)) => Ok(packet),
            Some(Err(e)) => Err(e),
            None => Err(io::ErrorKind::UnexpectedEof.into()),
        }
    }

    pub async fn bootstrap(&mut self, address: SocketAddr) -> io::Result<()> {
        self.stream.send(Packet::Bootstrap(address)).await?;
        let response = self.receive().await?;
        let status = match response {
            Packet::Status(status) => status,
            packet => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    UnexpectedPacket(packet),
                ))
            }
        };
        status.map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }
}

/// An error that indicated that an unexpected packet was received.
#[derive(Debug)]
struct UnexpectedPacket(Packet);

impl fmt::Display for UnexpectedPacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("received unexpected packet")
    }
}

impl Error for UnexpectedPacket {}

/// The local listener listens for connections from processes.
#[derive(Debug)]
pub struct LocalListener {
    tcp_listener: TcpListener,
    controller: Controller,
}

impl LocalListener {
    pub async fn new(config: &DaemonConfig, controller: Controller) -> io::Result<Self> {
        let tcp_listener = TcpListener::bind(config.socket_addr).await?;
        Ok(Self {
            tcp_listener,
            controller,
        })
    }

    pub async fn accept(&mut self) -> io::Result<Option<ConnectionToProcess>> {
        let (socket, _) = self.tcp_listener.accept().await?;
        let process = ConnectionToProcess {
            stream: Framed::new(socket, Codec::new(1024)),
            controller: self.controller.clone(),
        };
        Ok(Some(process))
    }
}

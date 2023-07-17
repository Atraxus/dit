//! The dit daemon is the main entry point for the dit application. It is
//! responsible for creating the runtime consisting of a local peer, a tcp listener
//! and a controller. The listener also has a controller, which is used to
//! manage the remote peers that connect to the listener.

use crate::codec::Codec;
use crate::peer::Controller;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
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
                    self.controller.bootstrap(address).await?;
                }
                Ok(Packet::Pong(_)) => {
                    tracing::warn!("Received unexpected Pong packet");
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
    pub async fn connect(address: SocketAddr) -> tokio::io::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let stream = Framed::new(stream, Codec::new(1024));

        Ok(Self { stream })
    }

    /// Send a ping packet to the daemon.
    pub async fn ping(&mut self, value: u64) -> tokio::io::Result<()> {
        self.stream.send(Packet::Ping(value)).await
    }

    /// Receive a packet from the daemon.
    pub async fn receive(&mut self) -> tokio::io::Result<Option<Packet>> {
        match self.stream.next().await {
            Some(Ok(packet)) => Ok(Some(packet)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    pub async fn bootstrap(&mut self, address: SocketAddr) -> tokio::io::Result<()> {
        self.stream.send(Packet::Bootstrap(address)).await
    }
}

/// The local listener listens for connections from processes.
#[derive(Debug)]
pub struct LocalListener {
    pub tcp_listener: TcpListener,
}

impl LocalListener {
    pub async fn accept(
        &mut self,
        controller: Controller,
    ) -> io::Result<Option<ConnectionToProcess>> {
        let (socket, _) = self.tcp_listener.accept().await?;
        let process = ConnectionToProcess {
            stream: Framed::new(socket, Codec::new(1024)),
            controller,
        };
        Ok(Some(process))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalConfig;
    use crate::peer::Runtime;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};
    use tracing::Instrument;

    #[tokio::test]
    async fn encode_decode_multiple_roundtrip() {
        let test_packets = vec![
            Packet::Ping(123),
            Packet::Pong(123),
            Packet::Ping(456),
            Packet::Pong(456),
        ];

        let mut codec = Codec::new(1024);
        let mut buffer = BytesMut::new();

        for packet in test_packets.clone() {
            codec.encode(packet.clone(), &mut buffer).unwrap();
        }

        for packet in test_packets {
            let received = codec.decode(&mut buffer).unwrap().unwrap();
            assert_eq!(received, packet);
        }
    }

    #[tokio::test]
    async fn encode_decode_single_roundtrip() {
        let test_packet = Packet::Ping(123);

        let mut codec = Codec::new(1024);
        let mut buffer = BytesMut::new();

        codec.encode(test_packet.clone(), &mut buffer).unwrap();
        let received = codec.decode(&mut buffer).unwrap().unwrap();
        assert_eq!(received, test_packet);
    }

    #[tokio::test]
    async fn bootstrap() -> Result<(), io::Error> {
        let temp_dir_a = tempfile::tempdir().unwrap();
        let config_path_a = temp_dir_a.path().join("dit-config.toml");
        GlobalConfig::init(&config_path_a, /* overwrite = */ false).unwrap();
        let mut config_a = GlobalConfig::read(&config_path_a).unwrap();

        let temp_dir_b = tempfile::tempdir().unwrap();
        let config_path_b = temp_dir_b.path().join("dit-config.toml");
        GlobalConfig::init(&config_path_b, /* overwrite = */ false).unwrap();
        let mut config_b = GlobalConfig::read(&config_path_b).unwrap();

        config_a.peer.addrs.socket_addr = "127.0.0.1:7700".parse().unwrap();
        config_a.daemon.socket_addr = "127.0.0.1:7701".parse().unwrap();
        config_b.peer.addrs.socket_addr = "127.0.0.2:7700".parse().unwrap();
        config_b.daemon.socket_addr = "127.0.0.2:7701".parse().unwrap();

        let rt_a = Runtime::new(config_a.peer).await.unwrap();

        // Simulate running daemon for peer a
        let tcp_listener_a = TcpListener::bind(config_a.daemon.socket_addr).await?;

        let mut local_listener_a = LocalListener {
            tcp_listener: tcp_listener_a,
        };

        let listener_a = tokio::spawn(
            async move {
                if let Some(inbound) = local_listener_a.accept(rt_a.controller.clone()).await? {
                    tokio::spawn(inbound.run().in_current_span());
                }

                Ok::<(), io::Error>(())
            }
            .instrument(tracing::debug_span!("listener")),
        );

        // Create connection to peer a daemon from peer b
        let mut conn_to_daemon_a = ConnectionToDaemon::connect(config_a.daemon.socket_addr).await?;

        // Send bootstrap packet from peer b to peer a
        conn_to_daemon_a
            .bootstrap(config_b.peer.addrs.socket_addr)
            .await?;

        // Check the listener_a result to make sure it didn't fail
        let listener_result = listener_a.await.unwrap();

        assert!(listener_result.is_ok());
        Ok(())
    }
}

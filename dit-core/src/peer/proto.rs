use super::{DhtAddr, DhtAndSocketAddr, SocketAddr};

use serde::{Deserialize, Serialize};

/// A datagram that can be routed though the peer-to-peer network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Packet {
    pub src: DhtAndSocketAddr,
    pub dst: DhtAddr,
    pub ttl: u32,
    pub payload: Payload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Payload {
    Ping(u64),
    Pong(u64),
    NeighborsRequest,
    NeighborsResponse(Neighbors),

    /// Append the `SocketAddr` to the DHT entry at `dst`.
    PutRequest(SocketAddr),

    /// Indicates a successful put of the `SocketAddr` at `src.dht_addr`.
    PutResponse(SocketAddr),

    /// Requests the DHT entry at `dst`.
    GetRequest,

    /// Contains the DHT entry at `src.dht_addr`.
    GetResponse(Vec<SocketAddr>),
}

impl Payload {
    pub fn kind(&self) -> PayloadKind {
        match self {
            Payload::Ping { .. } => PayloadKind::Ping,
            Payload::Pong { .. } => PayloadKind::Pong,
            Payload::NeighborsRequest { .. } => PayloadKind::NeighborsRequest,
            Payload::NeighborsResponse { .. } => PayloadKind::NeighborsResponse,
            Payload::PutRequest { .. } => PayloadKind::PutRequest,
            Payload::PutResponse { .. } => PayloadKind::PutResponse,
            Payload::GetRequest { .. } => PayloadKind::GetRequest,
            Payload::GetResponse { .. } => PayloadKind::GetResponse,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PayloadKind {
    Ping,
    Pong,
    NeighborsRequest,
    NeighborsResponse,
    PutRequest,
    PutResponse,
    GetRequest,
    GetResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Neighbors {
    pub pred: Option<DhtAndSocketAddr>,
    pub succ: Option<DhtAndSocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::codec::Codec;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io;
    use tokio_util::codec::Framed;

    #[tokio::test]
    async fn encode_decode_roundtrip() {
        let max_packet_length = 1024;

        let test_packets = [
            Packet {
                src: DhtAndSocketAddr {
                    dht_addr: DhtAddr::random(),
                    socket_addr: "1.2.3.4:5".parse().unwrap(),
                },
                dst: DhtAddr::random(),
                ttl: 123,
                payload: Payload::NeighborsRequest,
            },
            Packet {
                src: DhtAndSocketAddr {
                    dht_addr: DhtAddr::random(),
                    socket_addr: "[::1]:1".parse().unwrap(),
                },
                dst: DhtAddr::random(),
                ttl: 456,
                payload: Payload::NeighborsResponse(Neighbors {
                    pred: Some(DhtAndSocketAddr {
                        dht_addr: DhtAddr::hash("pred"),
                        socket_addr: "[FEED::]:0".parse().unwrap(),
                    }),
                    succ: None,
                }),
            },
        ];

        for buffer_size in [1, 16, 1024] {
            let (sender, receiver) = io::duplex(buffer_size);
            let codec = Codec::new(max_packet_length);
            let mut framed_sender = Framed::new(sender, codec);
            let mut framed_receiver = Framed::new(receiver, codec);

            let send_packets = test_packets.clone();
            let sender_task = tokio::spawn(async move {
                for packet in send_packets {
                    framed_sender.send(packet).await.unwrap();
                }
                framed_sender.close().await.unwrap();
            });

            let receiver_task = tokio::spawn(async move {
                let mut received_packets = Vec::new();
                while let Some(packet) = framed_receiver.next().await.transpose().unwrap() {
                    received_packets.push(packet);
                }
                received_packets
            });

            sender_task.await.unwrap();
            let received_packets = receiver_task.await.unwrap();

            assert_eq!(received_packets, test_packets);
        }
    }
}

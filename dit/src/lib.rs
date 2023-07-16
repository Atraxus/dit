#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

use clap::{Parser, Subcommand};
use dit_core::daemon::{Config, ConnectionToDaemon, DaemonConfig, LocalListener, Packet};
use dit_core::peer::{DhtAddr, DhtAndSocketAddr, PeerConfig, Runtime};
use tokio::io;
use tokio::net::TcpListener;
use tracing::Instrument;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

pub async fn run_daemon(config: Config) -> Result<(), io::Error> {
    let rt = Runtime::new(config.peer_config).await?;

    let tcp_listener = TcpListener::bind(config.daemon_config.socket_addr).await?;

    let mut local_listener = LocalListener { tcp_listener };

    let listener = tokio::spawn(
        async move {
            loop {
                let Some(inbound) = local_listener.accept(rt.controller.clone()).await? else {
                    return Ok::<(), io::Error>(());
                };

                tokio::spawn(inbound.run().in_current_span());
            }
        }
        .instrument(tracing::debug_span!("listener")),
    );

    let local_peer = tokio::spawn(
        rt.local_peer
            .run()
            .instrument(tracing::debug_span!("local peer")),
    );

    let (listener_result, local_peer_result) = tokio::join!(listener, local_peer);

    listener_result.unwrap().unwrap();
    local_peer_result.unwrap();

    Ok(())
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Starts the daemon.
    Daemon,
    /// Pings the daemon.
    PingDaemon,
    /// Bootstrap the daemon.
    Bootstrap {
        /// The address of the peer to bootstrap the daemon to.
        #[clap(short, long)]
        address: String,
    },
}

#[tracing::instrument(name = "run_cli", skip(args))]
pub async fn run(args: Args) {
    tracing::debug!(?args);

    let daemon_config = DaemonConfig {
        socket_addr: "127.0.0.1:6661".parse().unwrap(),
    };
    let peer_config = PeerConfig {
        addrs: DhtAndSocketAddr {
            dht_addr: DhtAddr::random(),
            socket_addr: "127.0.0.1:6660".parse().unwrap(),
        },
        ttl: 8,
        query_queue_size: 1,
        max_packet_length: 1024,
    };
    let config = Config {
        daemon_config,
        peer_config,
    };

    match args.command {
        Command::Daemon => {
            tracing::info!("Starting dit daemon");
            run_daemon(config).await.unwrap();
        }
        Command::PingDaemon => {
            // Connect to the daemon (get socket from toml)
            let mut connection = ConnectionToDaemon::connect(config.daemon_config.socket_addr)
                .await
                .unwrap();

            // Send a message to the daemon
            connection.ping(42).await.unwrap(); // Sending a ping packet with 42 as value

            // If you want to receive a packet (for example a pong) after sending ping
            match connection.receive().await {
                Ok(Some(Packet::Pong(value))) => {
                    println!("Received Pong with value: {}", value);
                }
                Ok(Some(_)) => {
                    println!("Received unexpected packet");
                }
                Ok(None) => {
                    println!("No more packets to receive, connection was closed");
                }
                Err(e) => {
                    eprintln!("An error occurred while receiving a packet: {}", e);
                }
            }
        }
        Command::Bootstrap { address } => {
            // Connect to the daemon (get socket from toml)
            let mut connection = ConnectionToDaemon::connect(config.daemon_config.socket_addr)
                .await
                .unwrap();

            // Send a message to the daemon
            connection
                .bootstrap(address.parse().unwrap())
                .await
                .unwrap();
        }
    }
}

pub fn install_default_tracing_subscriber() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var("DIT_LOG")
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init()
}

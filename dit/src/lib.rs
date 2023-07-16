#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

use clap::{Parser, Subcommand};
use dit_core::config::GlobalConfig;
use dit_core::daemon::{ConnectionToDaemon, LocalListener, Packet};
use dit_core::peer::Runtime;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::io;
use tokio::net::TcpListener;
use tracing::Instrument;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

pub async fn run_daemon(config: GlobalConfig) -> Result<(), io::Error> {
    let rt = Runtime::new(config.peer).await?;

    let tcp_listener = TcpListener::bind(config.daemon.socket_addr).await?;

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
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Creates the config file.
    Init {
        /// Overwrites the file if it exists.
        #[arg(long)]
        overwrite: bool,
    },
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

    match args.command {
        Command::Init { overwrite } => {
            if let Some(config_dir) = args.config.parent() {
                if let Err(err) = fs::create_dir_all(config_dir) {
                    eprintln!(
                        "failed to create directory '{}': {err}",
                        config_dir.display()
                    );
                    return;
                }
            }
            if let Err(err) = GlobalConfig::init(args.config, overwrite) {
                eprintln!("failed to create config: {err}");
            };
        }
        Command::Daemon => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };
            let Ok(()) = validate_config_for_running_peer(&config) else {
                return;
            };
            tracing::info!("Starting dit daemon");
            run_daemon(config).await.unwrap();
        }
        Command::PingDaemon => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };

            // Connect to the daemon (get socket from toml)
            let mut connection = ConnectionToDaemon::connect(config.daemon.socket_addr)
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
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };

            // Connect to the daemon (get socket from toml)
            let mut connection = ConnectionToDaemon::connect(config.daemon.socket_addr)
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

/// Indicated that an error has been reported to the user and the program may exit.
struct ErrorReported;

fn read_config_or_report_error(path: &Path) -> Result<GlobalConfig, ErrorReported> {
    GlobalConfig::read(path).map_err(|err| {
        eprintln!("error: {err}");
        ErrorReported
    })
}

fn validate_config_for_running_peer(config: &GlobalConfig) -> Result<(), ErrorReported> {
    if config.peer.addrs.socket_addr.ip().is_unspecified() {
        eprintln!("error: invalid address for `peer.listener` in config");
        eprintln!("help: replace `0.0.0.0` with your public ip address");
        return Err(ErrorReported);
    }

    Ok(())
}

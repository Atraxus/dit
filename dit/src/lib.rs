#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

use clap::{Parser, Subcommand};
use dit_core::config::GlobalConfig;
use dit_core::daemon::{ConnectionToDaemon, LocalListener, Packet};
use dit_core::peer::Runtime;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::io;
use tracing::Instrument;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

pub async fn run_daemon(config: GlobalConfig) -> Result<(), io::Error> {
    let rt = Runtime::new(config.clone().peer).await?;

    let mut local_listener = LocalListener::new(&config.daemon, rt.controller).await?;
    let remote_listener = rt.listener;

    let local_listener_task = tokio::spawn(
        async move {
            loop {
                if let Some(inbound) = local_listener.accept().await? {
                    tokio::spawn(inbound.run().in_current_span());
                } else {
                    return Ok::<(), io::Error>(());
                }
            }
        }
        .instrument(tracing::debug_span!("local listener")),
    );

    let remote_listener_task = tokio::spawn(
        async move {
            loop {
                if let Some(remote_peer) = remote_listener.accept().await? {
                    tokio::spawn(remote_peer.run().in_current_span());
                } else {
                    return Ok::<(), io::Error>(());
                }
            }
        }
        .instrument(tracing::debug_span!("remote listener")),
    );

    let local_peer = tokio::spawn(
        rt.local_peer
            .run()
            .instrument(tracing::debug_span!("local peer")),
    );

    let (local_listener_result, remote_listener_result, local_peer_result) =
        tokio::join!(local_listener_task, remote_listener_task, local_peer);

    local_listener_result.unwrap().unwrap();
    remote_listener_result.unwrap().unwrap();
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
        address: SocketAddr,
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
                Ok(Packet::Pong(value)) => {
                    println!("Received Pong with value: {}", value);
                }
                Ok(_) => {
                    println!("Received unexpected packet");
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
            let mut connection = match ConnectionToDaemon::connect(config.daemon.socket_addr).await
            {
                Ok(ok) => ok,
                Err(err) => {
                    eprintln!("error: failed to connect to daemon: {err}");
                    return;
                }
            };

            // Send a message to the daemon
            match connection.bootstrap(address).await {
                Ok(()) => (),
                Err(err) => {
                    eprintln!("error: bootstrapping failed: {err}");
                }
            }
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

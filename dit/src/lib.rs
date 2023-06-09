#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

use clap::{Parser, Subcommand};
use dit_core::peer::{Config, DhtAndSocketAddr};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Starts the daemon.
    Daemon,
}

pub async fn run(args: Args) {
    match args.command {
        Command::Daemon => {
            // let config = Config {
            //     addr: DhtAndSocketAddr {
            //         dht_addr: todo!(),
            //         socket_addr: todo!(),
            //     },
            //     ttl: todo!(),
            //     query_queue_size: todo!(),
            // }
            // dit_core::peer::LocalPeer::new(config)
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

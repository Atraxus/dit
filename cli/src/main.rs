use clap::Parser;
use dit_cli::*;

#[tokio::main]
async fn main() {
    install_default_tracing_subscriber();
    let args = Args::parse();
    run(args).await
}

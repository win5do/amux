use std::path::PathBuf;

use amux::shim::Shim;
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    process_dir: PathBuf,
    #[arg(long)]
    socket: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let ready_fd = std::env::var("AMUX_SHIM_READY_FD")
        .ok()
        .and_then(|value| value.parse().ok());
    Shim::load(args.process_dir, args.socket)?
        .serve(ready_fd)
        .await
}

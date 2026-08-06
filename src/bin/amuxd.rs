use amux::{config::Config, daemon::Daemon};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let ready_fd = std::env::var("AMUX_DAEMON_READY_FD")
        .ok()
        .and_then(|value| value.parse().ok());
    Daemon::new(Config::load()?)?.serve(ready_fd).await
}

use std::{env, sync::OnceLock};

use anyhow::{Context, Result};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

static INITIALIZED: OnceLock<()> = OnceLock::new();

#[derive(Debug, Clone, Copy)]
pub enum ProcessRole {
    Daemon,
    Shim,
}

impl ProcessRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Daemon => "daemon",
            Self::Shim => "shim",
        }
    }
}

pub fn init(role: ProcessRole) -> Result<()> {
    if INITIALIZED.get().is_some() {
        return Ok(());
    }
    let filter = filter_from_values(
        env::var("AMUX_LOG").ok().as_deref(),
        env::var("RUST_LOG").ok().as_deref(),
    )?;
    let subscriber = tracing_subscriber::registry().with(filter).with(
        fmt::layer()
            .compact()
            .with_ansi(false)
            .with_target(true)
            .with_writer(std::io::stderr),
    );
    let installed = subscriber.try_init().is_ok();
    let _ = INITIALIZED.set(());
    if installed {
        std::panic::set_hook(Box::new(tracing_panic::panic_hook));
    }
    tracing::debug!(component = role.as_str(), "diagnostics initialized");
    Ok(())
}

fn filter_from_values(amux_log: Option<&str>, rust_log: Option<&str>) -> Result<EnvFilter> {
    let directive = amux_log.or(rust_log).unwrap_or("amux=info");
    EnvFilter::try_new(directive)
        .with_context(|| format!("invalid diagnostic log filter {directive:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amux_filter_takes_precedence_and_invalid_filters_fail() {
        assert_eq!(
            filter_from_values(Some("amux=debug"), Some("amux=error"))
                .unwrap()
                .to_string(),
            "amux=debug"
        );
        assert_eq!(
            filter_from_values(None, Some("amux=warn"))
                .unwrap()
                .to_string(),
            "amux=warn"
        );
        assert!(filter_from_values(Some("["), None).is_err());
    }
}

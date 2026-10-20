pub mod client;
pub mod config;
pub mod daemon;
pub mod lifecycle;
pub mod logging;
pub mod model;
pub mod observability;
pub mod output;
pub mod process_identity;
pub mod protocol;
pub mod shim;
pub mod store;
pub mod unix_http;

pub type Result<T> = anyhow::Result<T>;

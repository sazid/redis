use clap::{Parser, ValueEnum};

const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("GIT_HASH"),
    " - ",
    env!("BUILD_TYPE"),
    ")"
);
const AUTHOR: &str = env!("CARGO_PKG_AUTHORS");

#[derive(Parser, Debug)]
#[command(
    name = "redis",
    author = AUTHOR,
    version = VERSION,
    about,
    long_about = None,
)]
pub struct Config {
    /// Port to listen for incoming connections
    #[arg(short, long, default_value_t = 6379)]
    pub port: u16,
    /// Host to listen for incoming connections
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Enable append-only file persistence
    #[arg(long, default_value_t = true)]
    pub aof_enabled: bool,

    /// Path to the append-only file
    #[arg(long, default_value = "db.aof")]
    pub aof_path: String,

    /// When to flush AOF writes to disk
    #[arg(long, value_enum, default_value_t = FsyncPolicy::Always)]
    pub aof_fsync_policy: FsyncPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum FsyncPolicy {
    Always,
    #[value(name = "everysec")]
    EverySec,
    No,
}

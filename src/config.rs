use clap::{Parser, ValueEnum};

use crate::eviction::EvictionPolicy;

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
    #[arg(long, default_value_t = false)]
    pub aof_enabled: bool,

    /// Path to the append-only file
    #[arg(long, default_value = "db.aof")]
    pub aof_path: String,

    /// When to flush AOF writes to disk
    #[arg(long, value_enum, default_value_t = FsyncPolicy::Always)]
    pub aof_fsync_policy: FsyncPolicy,

    /// Maximum memory, in bytes, before writes are rejected or keys are evicted
    #[arg(long = "maxmemory", value_name = "BYTES")]
    pub max_memory: Option<usize>,

    /// Eviction policy used when maxmemory is configured
    #[arg(long = "maxmemory-policy", value_enum, default_value_t = EvictionPolicy::AllKeysSieve)]
    pub max_memory_policy: EvictionPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum FsyncPolicy {
    Always,
    #[value(name = "everysec")]
    EverySec,
    No,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn default_maxmemory_is_disabled() {
        let config = Config::try_parse_from(["redis"]).unwrap();

        assert_eq!(config.max_memory, None);
        assert_eq!(config.max_memory_policy, EvictionPolicy::AllKeysSieve);
    }

    #[test]
    fn parses_maxmemory_and_eviction_policy() {
        let config = Config::try_parse_from([
            "redis",
            "--maxmemory",
            "1024",
            "--maxmemory-policy",
            "noeviction",
        ])
        .unwrap();

        assert_eq!(config.max_memory, Some(1024));
        assert_eq!(config.max_memory_policy, EvictionPolicy::NoEviction);
    }
}

use mimalloc::MiMalloc;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

pub mod config;
pub mod resp;
pub mod server;

mod db;
mod eviction;

pub mod build {
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub const GIT_HASH: &str = env!("GIT_HASH");
    pub const BUILD_TYPE: &str = env!("BUILD_TYPE");
}

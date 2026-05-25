use mimalloc::MiMalloc;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

pub mod config;
pub mod resp;
pub mod server;

mod db;

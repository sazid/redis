use clap::Parser;
use redis::{config::Config, server::run};

fn main() -> std::io::Result<()> {
    let config = Config::parse();
    run(config)
}

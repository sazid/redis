mod config;
mod resp;

use clap::Parser;
use config::Config;

pub fn run() {
    let config = Config::parse();

    let host = config.host;
    let port = config.port;
    println!("Host: {host}");
    println!("Host: {port}");
}

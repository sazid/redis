use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// Port to listen for incoming connections
    #[arg(short, long, default_value_t = 6379)]
    pub port: u16,
    /// Host to listen for incoming connections
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,
}

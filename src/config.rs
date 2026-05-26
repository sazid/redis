use clap::Parser;

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
    name = "perds",
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
}

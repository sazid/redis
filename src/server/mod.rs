mod commands;

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use slab::Slab;

use crate::config::Config;
use crate::db::RedisDb;
use crate::resp::{self, RespError};

use commands::handle_request;

const SERVER: Token = Token(0);
const ACTIVE_EXPIRE_INTERVAL: Duration = Duration::from_millis(100);

struct Client {
    socket: TcpStream,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
}

fn key_to_token(key: usize) -> Token {
    // +1 because `Token(0)` is already used for the server socket
    Token(key + 1)
}

fn token_to_key(token: Token) -> Option<usize> {
    token.0.checked_sub(1)
}

pub fn run(config: Config) -> std::io::Result<()> {
    let address: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("invalid host/port");

    let mut listener = TcpListener::bind(address)?;

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(1024);

    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)?;

    println!("Listening on {address}");

    let mut clients: Slab<Client> = Slab::new();

    let mut db = RedisDb::new();

    let mut last_active_expire = Instant::now();

    loop {
        let elapsed = last_active_expire.elapsed();

        let poll_timeout = Some(if elapsed >= ACTIVE_EXPIRE_INTERVAL {
            Duration::ZERO
        } else {
            ACTIVE_EXPIRE_INTERVAL - elapsed
        });

        poll.poll(&mut events, poll_timeout)?;

        let now = Instant::now();
        db.update_time(now);

        if now.duration_since(last_active_expire) >= ACTIVE_EXPIRE_INTERVAL {
            db.active_expire_sample();
            last_active_expire = now;
        }

        for event in events.iter() {
            match event.token() {
                SERVER => {
                    // accept clients here
                    loop {
                        match listener.accept() {
                            Ok((mut stream, addr)) => {
                                let entry = clients.vacant_entry();
                                let token = key_to_token(entry.key());

                                poll.registry()
                                    .register(&mut stream, token, Interest::READABLE)?;

                                entry.insert(Client {
                                    socket: stream,
                                    read_buf: Vec::new(),
                                    write_buf: Vec::new(),
                                });

                                println!("Accepted client {addr} as {token:?}");
                            }

                            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                                break;
                            }

                            Err(err) => {
                                return Err(err);
                            }
                        }
                    }
                }
                token => {
                    // read from client here
                    let Some(key) = token_to_key(token) else {
                        continue;
                    };
                    let mut disconnected = false;

                    let Some(client) = clients.get_mut(key) else {
                        continue;
                    };

                    if event.is_readable() {
                        let mut buf = [0u8; 4096];

                        loop {
                            match client.socket.read(&mut buf) {
                                Ok(0) => {
                                    disconnected = true;
                                    break;
                                }

                                Ok(n) => {
                                    client.read_buf.extend_from_slice(&buf[..n]);

                                    println!(
                                        "Received from {token:?}: {:?}",
                                        String::from_utf8_lossy(&buf[..n]),
                                    );

                                    if !process_client_buffer(client, token, &mut db) {
                                        disconnected = true;
                                        break;
                                    }
                                }

                                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                                    break;
                                }

                                Err(err) => {
                                    eprintln!("read error from {token:?}: {err}");
                                    disconnected = true;
                                    break;
                                }
                            }
                        }
                    }

                    if !disconnected
                        && event.is_writable()
                        && !flush_client_write_buffer(client, token)?
                    {
                        disconnected = true;
                    }

                    if !disconnected {
                        let interest = client_interest(!client.write_buf.is_empty());

                        poll.registry()
                            .reregister(&mut client.socket, token, interest)?;
                    }

                    if disconnected && let Some(mut client) = clients.try_remove(key) {
                        poll.registry().deregister(&mut client.socket)?;
                        println!("Disconnected {token:?}");
                    }
                }
            }
        }
    }
}

/// true = keep the client connected
/// false = disconnect the client
fn process_client_buffer(client: &mut Client, token: Token, db: &mut RedisDb) -> bool {
    process_buffers(&mut client.read_buf, &mut client.write_buf, token, db)
}

/// Parse as many complete RESP commands as possible from `read_buf` and append
/// their encoded responses to `write_buf`.
///
/// This is separated from `Client` so pipelining behavior can be tested without
/// constructing a real TCP socket.
fn process_buffers(
    read_buf: &mut Vec<u8>,
    write_buf: &mut Vec<u8>,
    token: Token,
    db: &mut RedisDb,
) -> bool {
    while !read_buf.is_empty() {
        match resp::decode_one(read_buf) {
            Ok((value, remaining)) => {
                let consumed = read_buf.len() - remaining.len();

                println!("Parsed from {token:?}: {value:?}");

                let outcome = handle_request(value, db);
                if let Some(_command) = &outcome.persist {
                    // TODO: append to AOF before writing response.
                }

                write_buf.extend_from_slice(&outcome.response.encode());

                read_buf.drain(..consumed);
            }

            Err(RespError::IncompleteInput) | Err(RespError::MissingCrlf) => return true,

            Err(err) => {
                eprintln!("protocol error from {token:?}: {err:?}");
                return false;
            }
        }
    }

    true
}

fn client_interest(has_pending_writes: bool) -> Interest {
    if has_pending_writes {
        Interest::READABLE.add(Interest::WRITABLE)
    } else {
        Interest::READABLE
    }
}

fn flush_client_write_buffer(client: &mut Client, token: Token) -> io::Result<bool> {
    while !client.write_buf.is_empty() {
        match client.socket.write(&client.write_buf) {
            Ok(0) => return Ok(false),
            Ok(n) => {
                client.write_buf.drain(..n);
            }
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(true),
            Err(err) => {
                eprintln!("write error to {token:?}: {err}");
                return Err(err);
            }
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CLIENT: Token = Token(1);

    fn process(read_buf: &mut Vec<u8>, write_buf: &mut Vec<u8>, db: &mut RedisDb) -> bool {
        process_buffers(read_buf, write_buf, TEST_CLIENT, db)
    }

    #[test]
    fn pipelined_ping_commands_produce_ordered_responses() {
        let mut db = RedisDb::new();
        let mut read_buf = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n".to_vec();
        let mut write_buf = Vec::new();

        assert!(process(&mut read_buf, &mut write_buf, &mut db));

        assert!(read_buf.is_empty());
        assert_eq!(write_buf, b"+PONG\r\n+PONG\r\n");
    }

    #[test]
    fn pipelined_commands_share_db_state() {
        let mut db = RedisDb::new();
        let mut read_buf = concat!(
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            "*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n",
        )
        .as_bytes()
        .to_vec();
        let mut write_buf = Vec::new();

        assert!(process(&mut read_buf, &mut write_buf, &mut db));

        assert!(read_buf.is_empty());
        assert_eq!(write_buf, b"+OK\r\n$3\r\nbar\r\n:1\r\n");
    }

    #[test]
    fn partial_command_waits_for_more_bytes() {
        let mut db = RedisDb::new();
        let mut read_buf = b"*1\r\n$4\r\nPI".to_vec();
        let mut write_buf = Vec::new();

        assert!(process(&mut read_buf, &mut write_buf, &mut db));
        assert_eq!(read_buf, b"*1\r\n$4\r\nPI");
        assert!(write_buf.is_empty());

        read_buf.extend_from_slice(b"NG\r\n");

        assert!(process(&mut read_buf, &mut write_buf, &mut db));
        assert!(read_buf.is_empty());
        assert_eq!(write_buf, b"+PONG\r\n");
    }

    #[test]
    fn complete_commands_are_processed_before_partial_tail() {
        let mut db = RedisDb::new();
        let mut read_buf = concat!("*1\r\n$4\r\nPING\r\n", "*1\r\n$4\r\nPI")
            .as_bytes()
            .to_vec();
        let mut write_buf = Vec::new();

        assert!(process(&mut read_buf, &mut write_buf, &mut db));

        assert_eq!(read_buf, b"*1\r\n$4\r\nPI");
        assert_eq!(write_buf, b"+PONG\r\n");

        read_buf.extend_from_slice(b"NG\r\n");

        assert!(process(&mut read_buf, &mut write_buf, &mut db));
        assert!(read_buf.is_empty());
        assert_eq!(write_buf, b"+PONG\r\n+PONG\r\n");
    }

    #[test]
    fn protocol_error_disconnects_client() {
        let mut db = RedisDb::new();
        let mut read_buf = b"?invalid\r\n".to_vec();
        let mut write_buf = Vec::new();

        assert!(!process(&mut read_buf, &mut write_buf, &mut db));
        assert!(write_buf.is_empty());
    }
}

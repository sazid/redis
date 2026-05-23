use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use slab::Slab;

use crate::config::Config;
use crate::db::RedisDb;
use crate::resp::{self, RespError, RespValue};

const SERVER: Token = Token(0);

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

    loop {
        // TODO: change `None` to a 100ms timeout which will force the event loop
        // to wake up even if there's no events to process. This will need to be
        // coupled with an active key expiry system.
        poll.poll(&mut events, None)?;

        db.update_time(Instant::now());

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

                    if !disconnected && event.is_writable() {
                        if !flush_client_write_buffer(client, token)? {
                            disconnected = true;
                        }
                    }

                    if !disconnected {
                        let interest = client_interest(!client.write_buf.is_empty());

                        poll.registry()
                            .reregister(&mut client.socket, token, interest)?;
                    }

                    if disconnected {
                        if let Some(mut client) = clients.try_remove(key) {
                            poll.registry().deregister(&mut client.socket)?;
                            println!("Disconnected {token:?}");
                        }
                    }
                }
            }
        }
    }
}

/// true = keep the client connected
/// false = disconnect the client
fn process_client_buffer(client: &mut Client, token: Token, db: &mut RedisDb) -> bool {
    while !client.read_buf.is_empty() {
        match resp::decode_one(&client.read_buf) {
            Ok((value, remaining)) => {
                let consumed = client.read_buf.len() - remaining.len();

                println!("Parsed from {token:?}: {value:?}");

                let response = handle_request(value, db);
                client.write_buf.extend_from_slice(&response.encode());

                client.read_buf.drain(..consumed);
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

fn handle_request(value: RespValue, db: &mut RedisDb) -> RespValue {
    let RespValue::Array(Some(items)) = value else {
        return RespValue::Error("ERR expected array command".to_owned());
    };

    if items.is_empty() {
        return RespValue::Error("ERR empty command".to_owned());
    }

    let Some(command_name) = value_as_bytes(&items[0]) else {
        return RespValue::Error("ERR command name must be a bulk string".to_owned());
    };

    if command_name.eq_ignore_ascii_case(b"PING") {
        handle_ping(&items)
    } else if command_name.eq_ignore_ascii_case(b"ECHO") {
        handle_echo(&items)
    } else if command_name.eq_ignore_ascii_case(b"SET") {
        handle_set(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"GET") {
        handle_get(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"EXPIRE") {
        handle_expire(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"EXISTS") {
        handle_exists(&items, db)
    } else {
        RespValue::Error("ERR unknown command".to_owned())
    }
}

fn value_as_bytes(value: &RespValue) -> Option<&[u8]> {
    match value {
        RespValue::BulkString(Some(bytes)) => Some(bytes),
        RespValue::SimpleString(value) => Some(value.as_bytes()),
        _ => None,
    }
}

fn value_as_i64(value: &RespValue) -> Option<i64> {
    match value {
        RespValue::Integer(value) => Some(*value),

        RespValue::BulkString(Some(bytes)) => {
            let text = std::str::from_utf8(bytes).ok()?;
            text.parse().ok()
        }

        RespValue::SimpleString(value) => value.parse().ok(),

        _ => None,
    }
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

// --- Command handlers --- //

fn handle_ping(items: &[RespValue]) -> RespValue {
    match items {
        [_command] => RespValue::SimpleString("PONG".to_owned()),

        [_command, message] => {
            let Some(bytes) = value_as_bytes(message) else {
                return RespValue::Error("ERR invalid PING argument".to_owned());
            };

            RespValue::BulkString(Some(bytes.to_vec()))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'ping' command".to_owned()),
    }
}

fn handle_echo(items: &[RespValue]) -> RespValue {
    match items {
        [_command, message] => {
            let Some(bytes) = value_as_bytes(message) else {
                return RespValue::Error("ERR invalid ECHO argument".to_owned());
            };

            RespValue::BulkString(Some(bytes.to_vec()))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'echo' command".to_owned()),
    }
}

fn handle_set(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key, value] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid SET argument: key".to_owned());
            };
            let Some(value) = value_as_bytes(value) else {
                return RespValue::Error("ERR invalid SET argument: value".to_owned());
            };

            db.set(key.to_owned(), value.to_owned());

            RespValue::SimpleString("OK".to_owned())
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'set' command".to_owned()),
    }
}

fn handle_get(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid GET argument: key".to_owned());
            };

            RespValue::BulkString(db.get(key))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'get' command".to_owned()),
    }
}

fn handle_expire(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key, ttl] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid EXPIRE argument: key".to_owned());
            };

            let Some(ttl) = value_as_i64(ttl) else {
                return RespValue::Error("ERR invalid EXPIRE argument: ttl".to_owned());
            };

            if ttl <= 0 {
                let deleted = db.delete(key);
                return RespValue::Integer(if deleted { 1 } else { 0 });
            }

            let did_expire = db.expire(key, Duration::from_secs(ttl as u64));

            RespValue::Integer(if did_expire { 1 } else { 0 })
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'expire' command".to_owned()),
    }
}

fn handle_exists(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    if items.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'exists' command".to_owned());
    }

    let count = match items[1..].iter().try_fold(0_i64, |count, key| {
        let Some(key) = value_as_bytes(key) else {
            return Err(RespValue::Error(
                "ERR invalid EXISTS argument: key".to_owned(),
            ));
        };

        Ok(count + if db.exists(key) { 1 } else { 0 })
    }) {
        Ok(count) => count,
        Err(error) => return error,
    };

    RespValue::Integer(count)
}

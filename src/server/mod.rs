mod aof;
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
use aof::Aof;

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

fn replay_aof(path: impl AsRef<std::path::Path>, db: &mut RedisDb) -> io::Result<()> {
    let mut bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };

    println!("Replaying data...");
    let mut command_count = 0;

    while !bytes.is_empty() {
        let (value, remaining) = resp::decode_one(&bytes)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("{err:?}")))?;

        let consumed = bytes.len() - remaining.len();

        let outcome = handle_request(value, db);

        if let Some(err) = outcome.response.error_message() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("AOF command failed during replay: {err}"),
            ));
        }

        command_count += 1;

        bytes.drain(..consumed);
    }

    println!("Done, replayed {command_count} commands.");

    Ok(())
}

pub fn run(config: Config) -> std::io::Result<()> {
    let address: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("invalid host/port");

    let mut db = RedisDb::new();

    if config.aof_enabled {
        replay_aof(&config.aof_path, &mut db)?;
    }

    let mut aof_writer = if config.aof_enabled {
        Some(Aof::new(&config.aof_path, config.aof_fsync_policy)?)
    } else {
        None
    };

    let mut listener = TcpListener::bind(address)?;

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(1024);

    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)?;

    println!("Listening on {address}");

    let mut clients: Slab<Client> = Slab::new();

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
                            Ok((mut stream, _)) => {
                                let entry = clients.vacant_entry();
                                let token = key_to_token(entry.key());

                                poll.registry()
                                    .register(&mut stream, token, Interest::READABLE)?;

                                entry.insert(Client {
                                    socket: stream,
                                    read_buf: Vec::new(),
                                    write_buf: Vec::new(),
                                });
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

                                    if !process_client_buffer(
                                        client,
                                        token,
                                        &mut db,
                                        &mut aof_writer,
                                    ) {
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
                    }
                }
            }
        }
    }
}

/// true = keep the client connected
/// false = disconnect the client
fn process_client_buffer(
    client: &mut Client,
    token: Token,
    db: &mut RedisDb,
    aof_writer: &mut Option<Aof>,
) -> bool {
    process_buffers(
        &mut client.read_buf,
        &mut client.write_buf,
        token,
        db,
        aof_writer,
    )
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
    aof_writer: &mut Option<Aof>,
) -> bool {
    while !read_buf.is_empty() {
        match resp::decode_one(read_buf) {
            Ok((value, remaining)) => {
                let consumed = read_buf.len() - remaining.len();

                let outcome = handle_request(value, db);
                if outcome.persist
                    && let Some(aof_writer) = aof_writer.as_mut()
                    && let Err(err) = aof_writer.append(&read_buf[..consumed])
                {
                    eprintln!("AOF append error: {err}");
                    return false;
                }

                outcome.response.encode_into(write_buf);

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
    use crate::resp::RespValue;

    const TEST_CLIENT: Token = Token(1);

    fn resp_bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    fn command(items: &[RespValue]) -> RespValue {
        RespValue::Array(Some(items.to_vec()))
    }

    fn temp_aof_path(test_name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after Unix epoch")
            .as_nanos();

        path.push(format!(
            "perds-redis-{test_name}-{}-{nanos}.aof",
            std::process::id()
        ));
        path
    }

    fn write_aof(path: &std::path::Path, commands: &[RespValue]) {
        let mut bytes = Vec::new();

        for command in commands {
            bytes.extend_from_slice(&command.encode());
        }

        std::fs::write(path, bytes).expect("test should write AOF file");
    }

    fn process(read_buf: &mut Vec<u8>, write_buf: &mut Vec<u8>, db: &mut RedisDb) -> bool {
        let mut aof_writer = None;
        process_buffers(read_buf, write_buf, TEST_CLIENT, db, &mut aof_writer)
    }

    #[test]
    fn replay_aof_restores_set_key() {
        let path = temp_aof_path("replay-set");
        write_aof(
            &path,
            &[command(&[
                resp_bulk("SET"),
                resp_bulk("foo"),
                resp_bulk("bar"),
            ])],
        );

        let mut db = RedisDb::new();

        let result = replay_aof(&path, &mut db);
        std::fs::remove_file(&path).ok();

        assert!(result.is_ok());
        assert_eq!(db.get(b"foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn replay_aof_replays_multiple_commands_in_order() {
        let path = temp_aof_path("replay-multiple");
        write_aof(
            &path,
            &[
                command(&[resp_bulk("SET"), resp_bulk("foo"), resp_bulk("bar")]),
                command(&[resp_bulk("SET"), resp_bulk("baz"), resp_bulk("qux")]),
                command(&[resp_bulk("DEL"), resp_bulk("foo")]),
            ],
        );

        let mut db = RedisDb::new();

        let result = replay_aof(&path, &mut db);
        std::fs::remove_file(&path).ok();

        assert!(result.is_ok());
        assert_eq!(db.get(b"foo"), None);
        assert_eq!(db.get(b"baz"), Some(b"qux".to_vec()));
    }

    #[test]
    fn replay_aof_rejects_invalid_resp() {
        let path = temp_aof_path("replay-invalid-resp");
        std::fs::write(&path, b"?invalid\r\n").expect("test should write AOF file");

        let mut db = RedisDb::new();

        let result = replay_aof(&path, &mut db);
        std::fs::remove_file(&path).ok();

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert_eq!(db.key_count(), 0);
    }

    #[test]
    fn replay_aof_rejects_command_errors() {
        let path = temp_aof_path("replay-command-error");
        write_aof(&path, &[command(&[resp_bulk("UNKNOWN")])]);

        let mut db = RedisDb::new();

        let result = replay_aof(&path, &mut db);
        std::fs::remove_file(&path).ok();

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert_eq!(db.key_count(), 0);
    }

    #[test]
    fn replay_aof_missing_file_is_ok() {
        let path = temp_aof_path("replay-missing-file");
        std::fs::remove_file(&path).ok();

        let mut db = RedisDb::new();

        let result = replay_aof(&path, &mut db);

        assert!(result.is_ok());
        assert_eq!(db.key_count(), 0);
    }

    #[test]
    fn process_buffers_appends_only_successful_mutations_to_aof() {
        let path = temp_aof_path("append-successful-mutations");
        std::fs::remove_file(&path).ok();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let del_missing = b"*2\r\n$3\r\nDEL\r\n$7\r\nmissing\r\n";

        let mut db = RedisDb::new();
        let mut read_buf = [set.as_slice(), get.as_slice(), del_missing.as_slice()].concat();
        let mut write_buf = Vec::new();
        let mut aof_writer = Some(
            Aof::new(&path, crate::config::FsyncPolicy::No).expect("test should create AOF writer"),
        );

        assert!(process_buffers(
            &mut read_buf,
            &mut write_buf,
            TEST_CLIENT,
            &mut db,
            &mut aof_writer,
        ));

        drop(aof_writer);

        assert!(read_buf.is_empty());
        assert_eq!(write_buf, b"+OK\r\n$3\r\nbar\r\n:0\r\n");
        assert_eq!(std::fs::read(&path).unwrap(), set);

        std::fs::remove_file(&path).ok();
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

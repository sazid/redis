use std::io::{self, Read};
use std::net::SocketAddr;

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use slab::Slab;

use crate::config::Config;

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

    loop {
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            match event.token() {
                SERVER => {
                    // accept clients here
                    loop {
                        match listener.accept() {
                            Ok((mut stream, addr)) => {
                                // let token = Token(next_token);
                                // next_token = next_token.wrapping_add(1);

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

                                // clients.insert(token, stream);
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

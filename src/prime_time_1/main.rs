use std::io::{prelude::*, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
struct Request {
    method: String,
    number: f64,
}

#[derive(Serialize, Debug)]
struct Response {
    method: String,
    prime: bool,
}

impl From<Request> for Response {
    fn from(req: Request) -> Self {
        if !is_integer(req.number) {
            return Response {
                method: req.method,
                prime: false,
            };
        }

        Response {
            method: req.method,
            prime: primes::is_prime(req.number as u64),
        }
    }
}

impl Response {
    fn get_malformed() -> Self {
        Response {
            method: "malformed".to_string(),
            prime: false,
        }
    }
}

fn is_integer(n: f64) -> bool {
    n.trunc() == n
}

fn handle_connection(mut stream: TcpStream) {
    let stream_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    let mut reader = BufReader::new(&stream_clone);
    let mut buffer = String::new();

    while let Ok(bytes_read) = reader.read_line(&mut buffer) {
        let req: Request = match serde_json::from_str(&buffer[..bytes_read]) {
            Ok(r) => r,
            Err(_) => {
                let _ = stream.write_all(
                    serde_json::to_string(&Response::get_malformed())
                        .unwrap()
                        .as_bytes(),
                );
                break;
            }
        };

        let res = Response::from(req);
        let res = match serde_json::to_string(&res) {
            Ok(r) => r,
            Err(_) => break,
        };

        let res = format!("{}\n", res);
        let _ = stream.write_all(res.as_bytes());
        buffer.clear();
    }
}

fn main() -> std::io::Result<()> {
    let socket_add = SocketAddr::from(([0, 0, 0, 0], 5000));
    let listener = TcpListener::bind(socket_add).unwrap();

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(5)
        .build()
        .expect("Failed to build thread pool");

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };

        thread_pool.spawn(move || {
            println!("Connection established!");
            handle_connection(stream);
        });
    }

    Ok(())
}

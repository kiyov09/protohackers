use std::{collections::HashMap, net::UdpSocket};

#[derive(Debug)]
struct DB {
    db: HashMap<String, String>,
}

impl DB {
    fn new() -> Self {
        Self {
            db: HashMap::from([("version".into(), "Ken's Key-Value Store 1.0".into())]),
        }
    }

    fn insert(&mut self, key: String, value: String) {
        if key == "version" {
            return;
        }

        self.db.insert(key, value);
    }

    fn get(&self, key: &str) -> &str {
        self.db.get(key).map(|s| s.as_str()).unwrap_or("")
    }
}

fn main() -> std::io::Result<()> {
    let socket_add = std::net::SocketAddr::from(([0, 0, 0, 0], 5000));

    // For use with fly.io
    // See [here](https://fly.io/docs/app-guides/udp-and-tcp/) for more info
    // let socket_add = "fly-global-services:5000";

    let listener = UdpSocket::bind(socket_add).expect("Failed to bind UDP socket");

    let mut db = DB::new();
    let mut buf = [0u8; 1000];

    while let Ok((size, src)) = listener.recv_from(&mut buf) {
        let req = String::from_utf8_lossy(&buf[..size]);
        let equal = req.chars().position(|b| b == '=');

        match equal {
            Some(pos) => {
                let key = &req[..pos];
                let value = &req[pos + 1..];
                db.insert(key.trim().into(), value.into());
            }
            None => {
                println!("{}: {}", src, req);
                let value = db.get(&req);
                let res = format!("{}={}", req, value);
                listener.send_to(res.as_bytes(), src)?;
            }
        }
    }

    Ok(())
}

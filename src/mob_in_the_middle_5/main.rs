use std::io::{prelude::*, BufReader};
use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
};

const TONYS_ADDR: &str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

// True if the string is a valid boguscoin address
fn is_boguscoin_addr(s: &str) -> bool {
    let mut chars = s.chars();
    let first_char = chars.next();

    if first_char.is_none() {
        return false;
    }

    // Should start with a 7
    if first_char.unwrap() != '7' {
        return false;
    }

    // All characters should be alphanumeric
    let mut num_of_valid_chars = 0;
    let all_alphanum = s.chars().all(|c| {
        if c.is_ascii_alphanumeric() {
            num_of_valid_chars += 1;
            true
        } else {
            false
        }
    });

    // All characters should be alphanumeric and there should be between 26 and 35 of them
    all_alphanum && (26..36).contains(&num_of_valid_chars)
}

fn replace_boguscoin_addrs(msg: &str) -> String {
    msg.split(' ')
        .map(|word| {
            if is_boguscoin_addr(word) {
                TONYS_ADDR
            } else {
                word
            }
        })
        .collect::<Vec<&str>>()
        .join(" ")
}

enum Msg {
    FromClient(String),
    FromUpstream(String),
    Quit,
}

struct Proxy {
    upstream: TcpStream,
    client: TcpStream,
    receiver: Receiver<Msg>,
}

impl Proxy {
    fn new(client: TcpStream, rx: Receiver<Msg>, sd: Sender<Msg>) -> Self {
        let upstream = TcpStream::connect("chat.protohackers.com:16963")
            .expect("Failed to connect to upstream");

        let upstream_clone = upstream.try_clone().unwrap();

        std::thread::spawn(move || {
            let mut reader = BufReader::new(&upstream_clone);
            let mut buffer = String::new();

            while let Ok(bytes_read) = reader.read_line(&mut buffer) {
                // Close the connection if the user has closed the connection
                if bytes_read == 0 {
                    break;
                }

                // Send the message to the server
                sd.send(Msg::FromUpstream(buffer.clone()))
                    .expect("Failed to send message");

                buffer.clear();
            }
        });

        Self {
            upstream,
            client,
            receiver: rx,
        }
    }

    fn run(&mut self) {
        while let Ok(action) = self.receiver.recv() {
            match action {
                Msg::FromClient(msg) => {
                    self.upstream
                        .write_all(replace_boguscoin_addrs(&msg).as_bytes())
                        .expect("Failed to write to upstream");
                }
                Msg::FromUpstream(msg) => {
                    self.client
                        .write_all(replace_boguscoin_addrs(&msg).as_bytes())
                        .expect("Failed to write to upstream");
                }
                Msg::Quit => {
                    self.upstream.shutdown(std::net::Shutdown::Both).unwrap();
                    println!("Closing connection");
                    break;
                }
            }
        }
    }
}

fn handle_connection(stream: TcpStream) {
    let stream_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    let (sd, rc) = channel::<Msg>();

    let sd_clone = sd.clone();
    let sc_clone = stream_clone.try_clone().unwrap();

    std::thread::spawn(move || {
        let mut proxy = Proxy::new(sc_clone, rc, sd_clone);
        proxy.run();
    });

    let mut reader = BufReader::new(&stream_clone);
    let mut buffer = String::new();

    while let Ok(bytes_read) = reader.read_line(&mut buffer) {
        // Close the connection if the user has closed the connection
        if bytes_read == 0 {
            break;
        }

        sd.send(Msg::FromClient(buffer.clone()))
            .expect("Failed to send message");

        buffer.clear();
    }

    sd.send(Msg::Quit).expect("Failed to send quit message");
}

fn main() -> std::io::Result<()> {
    let socket_add = SocketAddr::from(([0, 0, 0, 0], 5000));
    let listener = TcpListener::bind(socket_add).unwrap();

    // Create a thread pool to handle the connections
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(10)
        .build()
        .expect("Failed to build thread pool");

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Spawn a thread to handle the connection
        thread_pool.spawn(move || {
            handle_connection(stream);
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_boguscoin_addr() {
        // Test cases from the problem description
        assert!(is_boguscoin_addr("7F1u3wSD5RbOHQmupo9nx4TnhQ"));
        assert!(is_boguscoin_addr("7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX"));
        assert!(is_boguscoin_addr("7LOrwbDlS8NujgjddyogWgIM93MV5N2VR"));
        assert!(is_boguscoin_addr("7adNeSwJkMakpEcln9HEtthSRtxdmEHOT8T"));

        // Invalid addrs

        // Start with a 6
        assert!(!is_boguscoin_addr("6adNeSwJkMakpEcln9HEtthSRtxdmEHOT8T"));
        // Less than 26 chars
        assert!(!is_boguscoin_addr("7adNeSwJkMakpEcln9HEtthSR"));
        // More than 35 chars
        assert!(!is_boguscoin_addr("7adNeSwJkMakpEcln9HEtthSRtxdmEHOT8TTT"));
        // Non-alphanumeric chars
        assert!(!is_boguscoin_addr("7adNeSwJkMakpEcln9HEtthSRtxdmE%"));

        // Test the mob addr too
        assert!(is_boguscoin_addr(TONYS_ADDR));
    }

    #[test]
    fn test_replace_addr() {
        let msg = [
            "send payment to 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX", // no newline
            "this has no addr",                                // no addr
            "send payment to 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX asap\n", // ends with a newline
        ];
        let updated_msg = [
            "send payment to 7YWHMfk9JZe0LM0g1ZauHuiSxhI",
            "this has no addr",
            "send payment to 7YWHMfk9JZe0LM0g1ZauHuiSxhI asap\n",
        ];

        for (msg, updated) in msg.iter().zip(updated_msg.iter()) {
            assert_eq!(replace_boguscoin_addrs(msg), *updated);
        }
    }
}

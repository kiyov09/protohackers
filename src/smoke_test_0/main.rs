use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener, TcpStream};

fn handle_connection(mut stream: TcpStream) {
    let mut buff = [0u8; 1024];

    while let Ok(bytes_read) = stream.read(&mut buff) {
        if bytes_read == 0 {
            break;
        }

        if stream.write(&buff[..bytes_read]).is_err() {
            break;
        }
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

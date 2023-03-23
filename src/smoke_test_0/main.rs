use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener, TcpStream};

fn handle_connection(mut stream: TcpStream) -> Result<(), std::io::Error> {
    let mut buff = [0u8; 1024];

    while let Ok(bytes_read) = stream.read(&mut buff) {
        if bytes_read == 0 {
            break;
        }
        stream.write_all(&buff[..bytes_read])?;
        buff = [0; 1024];
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    let socket_add = SocketAddr::from(([127, 0, 0, 1], 7878));
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
            handle_connection(stream).expect("Failed to handle connection");
        });
    }

    Ok(())
}

use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
};

fn which_one_to_build(line: &str) -> &str {
    line.lines()
        .next()
        .unwrap_or_default()
        .split(',')
        .max_by_key(|toy| toy.split_once('x').map(|(n, _)| n.parse::<u32>().unwrap()))
        .unwrap_or_default()
}

#[derive(Debug, PartialEq)]
enum CipherOp {
    End,
    ReverseBits,
    Xor(u8),
    XorPos,
    Add(u8),
    AddPos,
}

impl CipherOp {
    fn apply(&self, byte: &mut u8, pos: u8) {
        match self {
            CipherOp::End => (),
            CipherOp::ReverseBits => *byte = byte.reverse_bits(),
            CipherOp::Xor(x) => *byte ^= x,
            CipherOp::XorPos => *byte ^= pos,
            CipherOp::Add(x) => *byte = byte.wrapping_add(*x),
            CipherOp::AddPos => *byte = byte.wrapping_add(pos),
        }
    }

    fn reverse(&self, byte: &mut u8, pos: u8) {
        match self {
            CipherOp::Add(x) => *byte = byte.wrapping_sub(*x),
            CipherOp::AddPos => *byte = byte.wrapping_sub(pos),
            _ => self.apply(byte, pos),
        }
    }
}

impl From<u8> for CipherOp {
    fn from(n: u8) -> Self {
        match n {
            0 => CipherOp::End,
            1 => CipherOp::ReverseBits,
            2 => CipherOp::Xor(0),
            3 => CipherOp::XorPos,
            4 => CipherOp::Add(0),
            5 => CipherOp::AddPos,
            _ => panic!("Invalid cipher op"),
        }
    }
}

#[derive(Debug, PartialEq, Default)]
struct CipherSpec {
    ops: Vec<CipherOp>,
}

impl CipherSpec {
    async fn decode(&self, buf: &mut [u8], init_pos: u8) {
        self.ops.iter().rev().for_each(|op| {
            buf.iter_mut()
                .enumerate()
                .for_each(|(idx, v)| op.reverse(v, init_pos + idx as u8))
        });
    }

    async fn encode(&mut self, buf: &mut [u8], init_pos: u8) {
        self.ops.iter().for_each(|op| {
            buf.iter_mut()
                .enumerate()
                .for_each(|(idx, v)| op.apply(v, init_pos + idx as u8))
        });
    }
}

impl From<&[u8]> for CipherSpec {
    fn from(value: &[u8]) -> Self {
        Self {
            ops: value.iter().fold(Vec::new(), |mut acc, &n| {
                match acc.last_mut() {
                    // After a XOR or ADD, the next byte is the value to XOR or ADD
                    // so we can just update the last op if its value is 0.
                    // If not, we push a new op.
                    Some(CipherOp::Xor(x) | CipherOp::Add(x)) if x == &0 => *x = n,
                    _ => acc.push(CipherOp::from(n)),
                };
                acc
            }),
        }
    }
}

#[derive(Debug, Default)]
struct Client {
    cipher_spec: CipherSpec,
    recv_cursor: u8,
    sent_cursor: u8,
}

impl Client {
    async fn run(&mut self, socket: TcpStream) {
        socket.readable().await.expect("socket is not readable");

        let mut reader = BufWriter::new(socket);
        let mut buffer = BytesMut::with_capacity(5000);

        // Read the cipher spec
        while let Ok(byte) = reader.read_u8().await {
            buffer.put_u8(byte);
            if byte == 0x00 {
                self.cipher_spec = CipherSpec::from(&buffer[..]);
                break;
            }
        }
        buffer.clear();

        // Validate the cipher spec
        if (self.validate_cipher().await).is_err() {
            return;
        } else {
            self.recv_cursor = 0;
            self.sent_cursor = 0;
        }

        let mut request = String::new();

        loop {
            let bytes_read = reader
                .read_buf(&mut buffer)
                .await
                .expect("Cannot read from socket");

            if bytes_read == 0 {
                eprintln!("Connection closed by client");
                break;
            }

            // Decode the data and save it to the request string
            self.decode(&mut buffer[..bytes_read]).await;
            request.push_str(std::str::from_utf8(&buffer[..bytes_read]).unwrap());

            // Clear the buffer for the next read
            buffer.clear();

            // Process complete lines
            while request.contains('\n') {
                let (line, rest) = request.split_once('\n').unwrap();
                let response = self.process_data(line.as_bytes()).await;

                reader
                    .write_all(&response)
                    .await
                    .expect("Cannot write to socket");

                reader.flush().await.expect("Cannot flush socket");

                request = rest.to_string();
            }
        }
    }

    async fn process_data(&mut self, buf: &[u8]) -> Vec<u8> {
        let max_toy = which_one_to_build(std::str::from_utf8(buf).unwrap());
        self.encode(max_toy.as_bytes()).await
    }

    async fn decode(&mut self, buf: &mut [u8]) {
        self.cipher_spec.decode(buf, self.recv_cursor).await;
        self.recv_cursor += buf.len() as u8;
    }

    async fn encode(&mut self, buf: &[u8]) -> Vec<u8> {
        let mut buf = buf.to_vec();
        buf.push(b'\n');

        self.cipher_spec.encode(&mut buf, self.sent_cursor).await;

        self.sent_cursor += buf.len() as u8;
        buf
    }

    async fn validate_cipher(&mut self) -> Result<(), ()> {
        let test_data = b"Hello, world!\n";
        let mut to_decode = test_data.to_vec();

        self.decode(&mut to_decode).await;
        if to_decode == test_data {
            return Err(());
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let socket_add = SocketAddr::from(([0, 0, 0, 0], 5000));
    let listener = TcpListener::bind(socket_add).await.unwrap();

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move { Client::default().run(socket).await });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_toy_in_request_1() {
        let msg = "10x toy car,15x dog on a string,4x inflatable motorcycle";
        let max = "15x dog on a string";

        assert_eq!(max, which_one_to_build(msg));
    }

    #[test]
    fn max_toy_in_request_2() {
        let msg = "4x dog,5x car";
        let max = "5x car";

        assert_eq!(max, which_one_to_build(msg));
    }

    #[test]
    fn ops_creation_1() {
        let spec_in_hex: [u8; 4] = [0x02, 0x01, 0x01, 0x00];
        let spec = CipherSpec {
            ops: vec![CipherOp::Xor(1), CipherOp::ReverseBits, CipherOp::End],
        };

        assert_eq!(spec, CipherSpec::from(&spec_in_hex[..]));
    }

    #[test]
    fn ops_creation_2() {
        let spec_in_hex: [u8; 5] = [0x02, 0x7b, 0x05, 0x01, 0x00];
        let spec = CipherSpec {
            ops: vec![
                CipherOp::Xor(123),
                CipherOp::AddPos,
                CipherOp::ReverseBits,
                CipherOp::End,
            ],
        };

        assert_eq!(spec, CipherSpec::from(&spec_in_hex[..]));
    }

    #[test]
    fn testing_understanding_1() {
        let spec_in_hex = [0x02, 0x7b, 0x05, 0x01, 0x00];
        let hexs = [
            0xf2, 0x20, 0xba, 0x44, 0x18, 0x84, 0xba, 0xaa, 0xd0, 0x26, 0x44, 0xa4, 0xa8, 0x7e,
        ];
        let final_text = "4x dog,5x car\n";

        let spec = CipherSpec::from(&spec_in_hex[..]);

        let mut f = hexs;
        assert_eq!(f, hexs);

        for ops in spec.ops[..spec.ops.len() - 1].iter().rev() {
            for (idx, v) in f.iter_mut().enumerate() {
                *v = match ops {
                    CipherOp::End => todo!(),
                    CipherOp::ReverseBits => (*v as u8).reverse_bits(),
                    CipherOp::Xor(n) => *v ^ n,
                    CipherOp::XorPos => todo!(),
                    CipherOp::Add(_) => todo!(),
                    CipherOp::AddPos => v.wrapping_sub(idx as u8),
                }
            }
        }

        assert_eq!(final_text, String::from_utf8_lossy(&f));
    }

    #[test]
    fn testing_understanding_2() {
        let spec_in_hex = [0x02, 0x01, 0x01, 0x00];
        let hexs = [0x96, 0x26, 0xb6, 0xb6, 0x76];

        let final_hexs = [0x68, 0x65, 0x6c, 0x6c, 0x6f];
        let final_text = "hello";

        let spec = CipherSpec::from(&spec_in_hex[..]);

        let mut f = hexs;
        for ops in spec.ops[..spec.ops.len() - 1].iter().rev() {
            for (idx, v) in f.iter_mut().enumerate() {
                *v = match ops {
                    CipherOp::End => todo!(),
                    CipherOp::ReverseBits => (*v as u8).reverse_bits(),
                    CipherOp::Xor(n) => *v ^ n,
                    CipherOp::XorPos => todo!(),
                    CipherOp::Add(_) => todo!(),
                    CipherOp::AddPos => v.wrapping_add(idx as u8),
                }
            }
        }

        assert_eq!(final_hexs, f);
        assert_eq!(final_text, String::from_utf8_lossy(&f));
    }
}

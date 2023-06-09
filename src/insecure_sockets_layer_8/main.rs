use std::net::SocketAddr;

use bytes::{Buf, BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::WriteHalf, TcpListener, TcpStream},
};

/// Get the line of toys and return the toy with the largest volume
fn which_one_to_build(line: &str) -> &str {
    line.split(',')
        .max_by_key(|toy| toy.split_once('x').map(|(n, _)| n.parse::<u32>().unwrap()))
        .unwrap_or_default()
}

/// Represents a cipher operation
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
    /// Apply the cipher operation to the byte
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

    /// Reverse the cipher operation to the byte
    /// All operations are the same in reverse except for Add and AddPos
    /// which are sub in reverse mode
    fn reverse(&self, byte: &mut u8, pos: u8) {
        match self {
            CipherOp::Add(x) => *byte = byte.wrapping_sub(*x),
            CipherOp::AddPos => *byte = byte.wrapping_sub(pos),
            _ => self.apply(byte, pos),
        }
    }
}

/// Convert a byte to a cipher operation
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

/// Represents a cipher specification
/// A cipher spec is a list of cipher operations
#[derive(Debug, PartialEq, Default)]
struct CipherSpec {
    ops: Vec<CipherOp>,
}

impl CipherSpec {
    /// Apply the cipher spec to the buffer
    async fn execute(
        &self,
        buf: &mut [u8],
        init_pos: u8,
        ops_iter: impl Iterator<Item = &CipherOp>,
        fn_apply: impl Fn(&CipherOp, &mut u8, u8),
    ) {
        ops_iter.for_each(|op| {
            buf.iter_mut()
                .enumerate()
                .for_each(|(idx, v)| fn_apply(op, v, init_pos + idx as u8))
        });
    }

    /// Apply the cipher spec to the buffer in reverse mode
    async fn decode(&self, buf: &mut [u8], init_pos: u8) {
        self.execute(buf, init_pos, self.ops.iter().rev(), CipherOp::reverse)
            .await;
    }

    /// Apply the cipher spec to the buffer in normal mode
    async fn encode(&mut self, buf: &mut [u8], init_pos: u8) {
        self.execute(buf, init_pos, self.ops.iter(), CipherOp::apply)
            .await;
    }
}

/// Convert a byte slice to a cipher spec
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

/// Represents a client session
/// Each session begins with a cipher spec and then the client sends a requests
/// Some ops in the cipher spec depends on the position of the byte in the request stream
/// and doesn't reset to 0 after each request. This is true for both messages received and sent.
/// Each stream has its own cursor.
#[derive(Debug, Default)]
struct Client {
    cipher_spec: CipherSpec,
    recv_cursor: u8,
    sent_cursor: u8,
}

impl Client {
    /// Handle a client session
    /// - Parse the cipher spec
    /// - Validate the cipher spec
    /// - In a loop, read the request, decode it, process it, encode it and send it back
    async fn run(&mut self, mut socket: TcpStream) {
        socket.readable().await.expect("socket is not readable");

        let (reader, writer) = socket.split();

        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        // Buffer to read from client. According to the challenge description:
        // "Clients won't send lines longer than 5000 characters."
        let mut buffer = BytesMut::with_capacity(5000);

        // Read the cipher spec
        while let Ok(byte) = reader.read_u8().await {
            buffer.put_u8(byte);

            if byte == 0x00 {
                self.cipher_spec = CipherSpec::from(&buffer[..]);

                match self.validate_cipher().await {
                    Ok(_) => break,
                    Err(_) => {
                        eprintln!("Invalid cipher spec");
                        return;
                    }
                }
            }
        }
        buffer.clear();

        // This cursor is user to keep track of the portion of the buffer that has been decoded
        let mut cursor = 0;

        loop {
            let bytes_read = reader
                .read_buf(&mut buffer)
                .await
                .expect("Cannot read from socket");

            if bytes_read == 0 {
                eprintln!("Connection closed by client");
                break;
            }

            // Decode the buffer from the last decoded position till the amount of bytes read
            self.decode(&mut buffer[cursor..cursor + bytes_read]).await;
            // Update the cursor
            cursor += bytes_read;

            // Process the data
            while buffer.contains(&b'\n') {
                // Get the position of the first new line
                let nl_pos = buffer
                    .iter()
                    .position(|&b| b == b'\n')
                    .expect("We know it contains a new line");

                // Process the data in the buffer until the new line
                let Ok(response) = self.process_data(&buffer[..nl_pos]).await else {
                    eprintln!("Invalid UTF-8");
                    break;
                };

                // Send the response to the client
                Self::send_response(&mut writer, &response).await;
                // Advance till the non processed data
                buffer.advance(nl_pos + 1);

                // Update the cursor to be in the correct position
                // for the next chunk of encoded data
                cursor -= nl_pos + 1;
            }
        }
    }

    async fn send_response(writer: &mut BufWriter<WriteHalf<'_>>, response: &[u8]) {
        writer
            .write_all(response)
            .await
            .expect("Cannot write to socket");

        writer.flush().await.expect("Cannot flush socket");
    }

    /// Process a line of data
    async fn process_data(&mut self, buf: &[u8]) -> Result<Vec<u8>, std::str::Utf8Error> {
        let as_str = std::str::from_utf8(buf)?;
        let max_toy = which_one_to_build(as_str);
        Ok(self.encode(max_toy.as_bytes()).await)
    }

    /// Request the decoding of the buffer
    async fn decode(&mut self, buf: &mut [u8]) {
        self.cipher_spec.decode(buf, self.recv_cursor).await;
        self.recv_cursor += buf.len() as u8;
    }

    /// Request the encoding of the buffer
    /// This adds a newline at the end of the buffer
    async fn encode(&mut self, buf: &[u8]) -> Vec<u8> {
        let mut buf = buf.to_vec();
        buf.push(b'\n');

        self.cipher_spec.encode(&mut buf, self.sent_cursor).await;

        self.sent_cursor += buf.len() as u8;
        buf
    }

    /// Validate the cipher spec
    async fn validate_cipher(&mut self) -> Result<(), ()> {
        let test_data = b"Hello, world!\n";
        let mut to_decode = test_data.to_vec();

        self.decode(&mut to_decode).await;
        if to_decode == test_data {
            return Err(());
        }

        // Reset cursors
        self.recv_cursor = 0;
        self.sent_cursor = 0;

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

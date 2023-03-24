use std::collections::HashMap;
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener, TcpStream};

#[derive(Debug, PartialEq)]
struct InsertOp {
    timestamp: i32,
    price: i32,
}

#[derive(Debug, PartialEq)]
struct QueryOp {
    mintime: i32,
    maxtime: i32,
}

#[derive(Debug, PartialEq)]
enum Ops {
    Insert(InsertOp),
    Query(QueryOp),
}

impl TryFrom<&[u8; 9]> for Ops {
    type Error = &'static str;

    fn try_from(value: &[u8; 9]) -> Result<Self, Self::Error> {
        match value[0] as char {
            'I' => {
                let timestamp =
                    i32::from_be_bytes(value[1..5].try_into().map_err(|_| "Invalid timestamp")?);
                let price =
                    i32::from_be_bytes(value[5..9].try_into().map_err(|_| "Invalid price")?);
                Ok(Ops::Insert(InsertOp { timestamp, price }))
            }
            'Q' => {
                let mintime =
                    i32::from_be_bytes(value[1..5].try_into().map_err(|_| "Invalid mintime")?);
                let maxtime =
                    i32::from_be_bytes(value[5..9].try_into().map_err(|_| "Invalid maxtime")?);

                Ok(Ops::Query(QueryOp { mintime, maxtime }))
            }
            _ => Err("Invalid operation"),
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0u8; 9];
    let mut prices = HashMap::new();

    while stream.read_exact(&mut buffer).is_ok() {
        let ops = Ops::try_from(&buffer);

        match ops {
            Ok(Ops::Insert(insert_op)) => {
                prices.entry(insert_op.timestamp).or_insert(insert_op.price);
            }
            Ok(Ops::Query(query_op)) => {
                if query_op.mintime > query_op.maxtime {
                    let _ = stream.write_all(&0u32.to_be_bytes());
                    continue;
                }

                let all_prices_in_range = prices
                    .iter()
                    .filter_map(|(k, v)| {
                        if *k >= query_op.mintime && *k <= query_op.maxtime {
                            Some(*v)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<i32>>();

                if all_prices_in_range.is_empty() {
                    let _ = stream.write_all(&0u32.to_be_bytes());
                    continue;
                }

                let average_price = all_prices_in_range.iter().map(|i| *i as i64).sum::<i64>()
                    / all_prices_in_range.len() as i64;

                let _ = stream.write_all(&(average_price as i32).to_be_bytes());
            }
            Err(_) => {
                continue;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_parsing() {
        let bytes = [0x49, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x65];
        let ops = Ops::try_from(&bytes);

        assert_eq!(
            ops,
            Ok(Ops::Insert(InsertOp {
                timestamp: 12345,
                price: 101
            }))
        );
    }

    #[test]
    fn test_query_parsing() {
        let bytes = [0x51, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x40, 0x00];
        let ops = Ops::try_from(&bytes);

        assert_eq!(
            ops,
            Ok(Ops::Query(QueryOp {
                mintime: 12288,
                maxtime: 16384
            }))
        );
    }
}

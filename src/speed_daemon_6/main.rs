use bytes::{Buf, BufMut};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    io::{prelude::*, BufReader},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
    time::Duration,
};

use rand::Rng;

type Road = u16;
type Mile = u16;
type Car = (Road, Mile, u16, Plate);

#[derive(Debug, PartialEq)]
struct Error {
    msg: String,
}

impl Error {
    fn new(msg: &str) -> Self {
        Self {
            msg: msg.to_string(),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![0x10];
        encode_str(&mut buffer, &self.msg);
        buffer
    }

    fn from_bytes<T: Buf>(mut buf: T) -> Self {
        let msg = decode_str(&mut buf);
        Self { msg }
    }
}

#[derive(Debug, PartialEq, Clone)]
struct Plate {
    plate: String,
    timestamp: u32,
}

impl Plate {
    fn from_bytes<T: Buf>(mut buf: T) -> Self {
        let plate = decode_str(&mut buf);
        let timestamp = buf.get_u32();
        Self { plate, timestamp }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Ticket {
    plate: String,
    road: u16,
    mile1: u16,
    timestamp1: u32,
    mile2: u16,
    timestamp2: u32,
    speed: u16,
}

impl Ticket {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![];

        buffer.put_u8(0x21);

        encode_str(&mut buffer, &self.plate);
        buffer.put_u16(self.road);
        buffer.put_u16(self.mile1);
        buffer.put_u32(self.timestamp1);
        buffer.put_u16(self.mile2);
        buffer.put_u32(self.timestamp2);
        buffer.put_u16(self.speed);
        buffer
    }
}

#[derive(Debug, PartialEq)]
struct WantHeartbeat {
    interval: u32,
}

impl WantHeartbeat {
    fn from_bytes<T: Buf>(mut buf: T) -> Self {
        let interval = buf.get_u32();
        Self { interval }
    }
}

struct Heartbeat;
impl Heartbeat {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![];
        buffer.put_u8(0x41);
        buffer
    }
}

#[derive(Debug, PartialEq)]
struct IAmCamera {
    road: Road,
    mile: Mile,
    speed_limit: u16,
}

impl IAmCamera {
    fn from_bytes<T: Buf>(mut buf: T) -> Self {
        let road = buf.get_u16();
        let mile = buf.get_u16();
        let speed_limit = buf.get_u16();

        Self {
            road,
            mile,
            speed_limit,
        }
    }

    fn handle_connection(
        &mut self,
        rest: Vec<u8>,
        mut stream: TcpStream,
        tx: Sender<ClientServerMsg>,
    ) {
        let road = self.road;
        let mile = self.mile;
        let speed_limit = self.speed_limit;

        let mut rest = &rest[..];

        // Analyze reminder of buffer
        while rest.has_remaining() {
            let message_type = Message::from(rest[0]);
            rest = &rest[1..];

            match message_type {
                Message::Plate => {
                    let plate = Plate::from_bytes(&mut rest);
                    tx.send(ClientServerMsg::Plate((road, mile, speed_limit, plate)))
                        .unwrap();
                }
                Message::WantHeartbeat => {
                    let want_heartbeat = WantHeartbeat::from_bytes(&mut rest);

                    tx.send(ClientServerMsg::WantHeartbeat(
                        want_heartbeat,
                        stream.try_clone().expect("Can't clone stream"),
                    ))
                    .expect("Unable to send message");
                }
                _ => {
                    let error = Error::new("Invalid message");
                    let _ = stream.write_all(&error.to_bytes());
                    return;
                }
            }
        }

        std::thread::spawn(move || {
            let mut reader = BufReader::new(stream.try_clone().expect("Can't clone stream"));
            let mut buffer = [0u8; 1024];

            while let Ok(bytes_read) = reader.read(&mut buffer) {
                if bytes_read == 0 {
                    break;
                }

                let mut buf = &buffer[..bytes_read];

                while buf.has_remaining() {
                    let message_type = Message::from(buf[0]);
                    buf = &buf[1..];

                    match message_type {
                        Message::Plate => {
                            let plate = Plate::from_bytes(&mut buf);
                            tx.send(ClientServerMsg::Plate((road, mile, speed_limit, plate)))
                                .unwrap();
                        }
                        Message::WantHeartbeat => {
                            let want_heartbeat = WantHeartbeat::from_bytes(&mut buf);

                            tx.send(ClientServerMsg::WantHeartbeat(
                                want_heartbeat,
                                stream.try_clone().expect("Can't clone stream"),
                            ))
                            .expect("Unable to send message");
                        }
                        _ => {
                            let error = Error::new("Invalid message");
                            let _ = stream.write_all(&error.to_bytes());
                            return;
                        }
                    }
                }
            }
        });
    }
}

#[derive(Debug, PartialEq, Clone)]
struct IAmDispatcher {
    numroads: u8,
    roads: Vec<Road>,
}

impl IAmDispatcher {
    fn from_bytes<T: Buf>(mut buf: T) -> Self {
        let roads = decode_u16_vec(&mut buf);
        Self {
            numroads: roads.len() as u8,
            roads,
        }
    }

    fn handle_connection(
        &mut self,
        rest: Vec<u8>,
        mut stream: TcpStream,
        tx: Sender<ClientServerMsg>,
    ) {
        let (dtx, drx) = channel::<ServerClientMsg>();

        tx.send(ClientServerMsg::IAmDispatcher(self.clone(), dtx))
            .expect("Unable to register dispatcher");

        // Analyze reminder of buffer
        if !rest.is_empty() {
            let message_type = Message::from(rest[0]);
            let mut rest = &rest[1..];

            match message_type {
                Message::WantHeartbeat => {
                    let want_heartbeat = WantHeartbeat::from_bytes(&mut rest);

                    tx.send(ClientServerMsg::WantHeartbeat(
                        want_heartbeat,
                        stream.try_clone().expect("Can't clone stream"),
                    ))
                    .expect("Unable to send message");
                }
                _ => {
                    let error = Error::new("Invalid message");
                    let _ = stream.write_all(&error.to_bytes());
                    return;
                }
            }
        }

        let stream_clone_rec = stream.try_clone().expect("Can't clone stream");
        let mut stream_clone_send = stream.try_clone().expect("Can't clone stream");

        std::thread::spawn(move || {
            let mut reader = BufReader::new(stream_clone_rec);
            let mut buffer = [0u8; 1024];

            while let Ok(bytes_read) = reader.read(&mut buffer) {
                if bytes_read == 0 {
                    break;
                }

                let message_type = Message::from(buffer[0]);
                let mut buf = &buffer[1..bytes_read];

                match message_type {
                    Message::WantHeartbeat => {
                        let want_heartbeat = WantHeartbeat::from_bytes(&mut buf);

                        tx.send(ClientServerMsg::WantHeartbeat(
                            want_heartbeat,
                            stream_clone_send.try_clone().expect("Can't clone stream"),
                        ))
                        .expect("Unable to send message");
                    }
                    _ => {
                        let error = Error::new("Invalid message");
                        let _ = stream_clone_send.write_all(&error.to_bytes());
                        break;
                    }
                }
            }
        });

        let mut stream_clone_send = stream.try_clone().expect("Can't clone stream");
        while let Ok(message) = drx.recv() {
            match message {
                ServerClientMsg::Ticket(ticket) => {
                    if stream_clone_send.write_all(&ticket.to_bytes()).is_err() {
                        break;
                    }
                }
            }
        }
    }
}

enum Message {
    Invalid,
    Error,         // 0x10
    Plate,         // 0x20
    Ticket,        // 0x21,
    WantHeartbeat, // 0x40
    Heartbeat,     // 0x41
    IAmCamera,     // 0x80
    IAmDispatcher, // 0x81
}

impl From<u8> for Message {
    fn from(byte: u8) -> Self {
        match byte {
            0x10 => Message::Error,
            0x20 => Message::Plate,
            0x21 => Message::Ticket,
            0x40 => Message::WantHeartbeat,
            0x41 => Message::Heartbeat,
            0x80 => Message::IAmCamera,
            0x81 => Message::IAmDispatcher,
            _ => Message::Invalid,
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Invalid => write!(f, "Invalid"),
            Message::Error => write!(f, "Error"),
            Message::Plate => write!(f, "Plate"),
            Message::Ticket => write!(f, "Ticket"),
            Message::WantHeartbeat => write!(f, "WantHeartbeat"),
            Message::Heartbeat => write!(f, "Heartbeat"),
            Message::IAmCamera => write!(f, "IAmCamera"),
            Message::IAmDispatcher => write!(f, "IAmDispatcher"),
        }
    }
}

enum ClientServerMsg {
    Plate(Car),
    IAmDispatcher(IAmDispatcher, Sender<ServerClientMsg>),
    WantHeartbeat(WantHeartbeat, TcpStream),
}

enum ServerClientMsg {
    Ticket(Ticket),
}

struct Server {
    dispatchers: HashMap<Road, Vec<(IAmDispatcher, Sender<ServerClientMsg>)>>,
    cars: Vec<Car>,
    rx: Receiver<ClientServerMsg>,
    pending_tickets: Vec<Ticket>,
    send_tickets: HashSet<(String, u32)>,
}

impl Server {
    fn new(rx: Receiver<ClientServerMsg>) -> Self {
        Self {
            dispatchers: HashMap::new(),
            cars: Vec::new(),
            rx,
            pending_tickets: Vec::new(),
            send_tickets: HashSet::new(),
        }
    }

    fn run(&mut self) {
        while let Ok(msg) = self.rx.recv() {
            match msg {
                ClientServerMsg::Plate(car) => self.handle_plate_observation(car),
                ClientServerMsg::IAmDispatcher(dispatcher, dtx) => {
                    self.handle_dispatcher(dispatcher, dtx)
                }
                ClientServerMsg::WantHeartbeat(want_heartbeat, stream) => {
                    self.handle_want_heartbeat(want_heartbeat, stream)
                }
            }
        }
    }

    fn handle_plate_observation(&mut self, car: Car) {
        let car_clone = (car.0, car.1, car.2, car.3.clone());

        let mut prev_observations = self
            .cars
            .iter()
            .filter(|c| c.0 == car_clone.0 && c.3.plate == car_clone.3.plate)
            .collect::<Vec<_>>();

        // sort by timestamp
        prev_observations.sort_by_key(|c| c.3.timestamp);

        for prev in prev_observations {
            let first = if prev.3.timestamp < car.3.timestamp {
                prev
            } else {
                &car
            };
            let last = if prev.3.timestamp > car.3.timestamp {
                prev
            } else {
                &car
            };

            let distance_delta = last.1.abs_diff(first.1);
            let time_delta = last.3.timestamp.abs_diff(first.3.timestamp);

            // using distance_delta (miles) and time_delta (seconds) to calculate speed (mph)
            let speed = distance_delta as f64 / (time_delta as f64 / 3600.0);

            // TODO: improve this
            // if speed > ob1.2 as f64 {
            if to_much_speed(speed, car.2 as f64) {
                let ticket = Ticket {
                    plate: car.3.plate.clone(),
                    road: car.0,
                    mile1: first.1,
                    timestamp1: first.3.timestamp,
                    mile2: last.1,
                    timestamp2: last.3.timestamp,
                    speed: (speed * 100.0) as u16,
                };

                // // Checking if we already send a ticket for that car on the same day
                let day1 = ticket.timestamp1 / 86400;
                let day2 = ticket.timestamp2 / 86400;

                let tickets_iter = (day1..=day2)
                    .map(|day| (ticket.clone(), day))
                    .collect::<Vec<_>>();

                if tickets_iter
                    .iter()
                    .any(|(_, d)| self.send_tickets.contains(&(ticket.plate.clone(), *d)))
                {
                    continue;
                }

                tickets_iter.iter().for_each(|(_, d)| {
                    self.send_tickets.insert((ticket.plate.clone(), *d));
                });

                if let Some(dispatchers) = self.dispatchers.get(&car.0) {
                    let random_index = rand::thread_rng().gen_range(0..dispatchers.len());
                    let dispatcher = &dispatchers[random_index];

                    dispatcher
                        .1
                        .send(ServerClientMsg::Ticket(ticket.clone()))
                        .expect("Unable to send message");
                } else {
                    self.pending_tickets.push(ticket);
                }
            }
        }

        self.cars.push(car);
    }

    fn handle_dispatcher(&mut self, dispatcher: IAmDispatcher, dtx: Sender<ServerClientMsg>) {
        dispatcher.roads.iter().for_each(|road| {
            self.dispatchers
                .entry(*road)
                .or_insert_with(Vec::new)
                .push((dispatcher.clone(), dtx.clone()));
        });

        self.pending_tickets.retain(|ticket| {
            if dispatcher.roads.contains(&ticket.road) {
                dtx.send(ServerClientMsg::Ticket(ticket.clone())).is_err()
            } else {
                true
            }
        })
    }

    fn handle_want_heartbeat(&mut self, want_heartbeat: WantHeartbeat, mut stream: TcpStream) {
        // TODO: handle the case where this client already requested a heartbeat

        if want_heartbeat.interval != 0 {
            let interval = want_heartbeat.interval as f32 / 10.0; // deciseconds
            std::thread::spawn(move || loop {
                std::thread::sleep(Duration::from_secs_f32(interval));

                if stream.write_all(&Heartbeat.to_bytes()).is_err() {
                    break;
                }
            });
        }
    }
}

fn to_much_speed(speed: f64, limit: f64) -> bool {
    // true if speed is greater than limit by 0.5 or more
    speed - limit >= 0.5
}

fn handle_connection(mut stream: TcpStream, tx: Sender<ClientServerMsg>) {
    let stream_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    let mut reader = BufReader::new(stream_clone.try_clone().unwrap());
    let mut buffer = [0u8; 1024];

    // First of all, we need to know if this is a camera or a dispatcher
    while let Ok(bytes_read) = reader.read(&mut buffer) {
        if bytes_read == 0 {
            return;
        }

        let message_type: Message = buffer[0].into();
        let mut buf = &buffer[1..bytes_read];

        match message_type {
            Message::IAmCamera => {
                if buf.remaining() < 6 {
                    let error =
                        Error::new("Invalid: First message must be IAmCamera or IAmDispatcher");

                    let _ = stream.write_all(&error.to_bytes());
                    break;
                }

                let mut cam = IAmCamera::from_bytes(&mut buf);
                let dst = Vec::from(&buf[..buf.remaining()]);

                std::thread::spawn(move || {
                    cam.handle_connection(dst, stream_clone.try_clone().unwrap(), tx);
                });
                break;
            }
            Message::IAmDispatcher => {
                let mut dispatcher = IAmDispatcher::from_bytes(&mut buf);
                let dst = Vec::from(&buf[..buf.remaining()]);

                std::thread::spawn(move || {
                    dispatcher.handle_connection(dst, stream_clone.try_clone().unwrap(), tx);
                });
                break;
            }
            Message::WantHeartbeat => {
                let want_heartbeat = WantHeartbeat::from_bytes(&mut buf);
                tx.send(ClientServerMsg::WantHeartbeat(
                    want_heartbeat,
                    stream_clone.try_clone().unwrap(),
                ))
                .unwrap();
            }

            // That's are the only messages that could came from the clients
            // Anything else will generate an error message to the client
            _ => {
                let error = Error::new("Invalid: First message must be IAmCamera or IAmDispatcher");
                let _ = stream.write_all(&error.to_bytes());
                break;
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let socket_add = SocketAddr::from(([0, 0, 0, 0], 5000));
    let listener = TcpListener::bind(socket_add).unwrap();

    let (tx, rx) = channel::<ClientServerMsg>();

    // Spawn a thread to run the Server
    std::thread::spawn(move || {
        let mut server = Server::new(rx);
        server.run();
    });

    // Create a thread pool to handle the connections
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(150)
        .build()
        .expect("Failed to build thread pool");

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Clone the sender before moving it to the thread
        let tx = tx.clone();

        // Spawn a thread to handle the connection
        thread_pool.spawn(move || {
            handle_connection(stream, tx);
        });
    }

    Ok(())
}

fn decode_str<T: Buf>(mut buf: T) -> String {
    let length = buf.get_u8() as usize;
    let mut result = String::with_capacity(length);

    if buf.remaining() < length {
        todo!("handle error");
    }

    while result.len() < length {
        result.push(buf.get_u8() as char);
    }

    result
}

fn encode_str<T: BufMut>(buf: &mut T, string: &str) {
    buf.put_u8(string.len() as u8);
    buf.put(string.as_bytes());
}

fn decode_u16_vec<T: Buf>(mut buf: T) -> Vec<u16> {
    let length = buf.get_u8() as usize;
    let mut result = Vec::with_capacity(length);

    if buf.remaining() < length * 2 {
        todo!("handle error");
    }

    while result.len() < length {
        result.push(buf.get_u16());
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_str_message() {
        let message_0 = [0x00];
        let expected_0 = "";

        let message_1 = [0x03, 0x66, 0x6f, 0x6f];
        let expected_1 = "foo";

        let message_2 = [0x08, 0x45, 0x6C, 0x62, 0x65, 0x72, 0x65, 0x74, 0x68];
        let expected_2 = "Elbereth";

        assert_eq!(decode_str(&message_0[..]), expected_0);
        assert_eq!(decode_str(&message_1[..]), expected_1);
        assert_eq!(decode_str(&message_2[..]), expected_2);
    }

    #[test]
    fn encode_str_message() {
        let message_0 = "bad";
        let expected_0 = [0x03, 0x62, 0x61, 0x64];
        let mut buffer_0 = vec![];

        let message_1 = "illegal msg";
        let expected_1 = [
            0x0b, 0x69, 0x6c, 0x6c, 0x65, 0x67, 0x61, 0x6c, 0x20, 0x6d, 0x73, 0x67,
        ];
        let mut buffer_1 = vec![];

        encode_str(&mut buffer_0, message_0);
        assert_eq!(buffer_0, expected_0);

        encode_str(&mut buffer_1, message_1);
        assert_eq!(buffer_1, expected_1);
    }

    // Error
    #[test]
    fn error_to_bytes() {
        let error = Error::new("bad");
        let expected = [0x03, 0x62, 0x61, 0x64];
        assert_eq!(error.to_bytes(), expected);
    }

    #[test]
    fn error_from_bytes() {
        let bytes = [0x03, 0x62, 0x61, 0x64];
        let expected = Error::new("bad");
        assert_eq!(Error::from_bytes(&bytes[..]), expected);
    }

    // Plate
    #[test]
    fn plate_from_bytes() {
        let bytes = [0x04, 0x55, 0x4e, 0x31, 0x58, 0x00, 0x00, 0x03, 0xe8];
        let expected = Plate {
            plate: "UN1X".to_string(),
            timestamp: 1000,
        };

        assert_eq!(Plate::from_bytes(&bytes[..]), expected);
    }

    // Ticket
    #[test]
    fn ticket_to_bytes() {
        let ticket = Ticket {
            plate: "UN1X".to_string(),
            road: 66,
            mile1: 100,
            timestamp1: 123456,
            mile2: 110,
            timestamp2: 123816,
            speed: 10000,
        };
        let bytes = [
            0x04, 0x55, 0x4e, 0x31, 0x58, 0x00, 0x42, 0x00, 0x64, 0x00, 0x01, 0xe2, 0x40, 0x00,
            0x6e, 0x00, 0x01, 0xe3, 0xa8, 0x27, 0x10,
        ];

        assert_eq!(ticket.to_bytes(), bytes);
    }

    // WantHeartbeat
    #[test]
    fn want_heartbeat_from_bytes() {
        let bytes = [0x00, 0x00, 0x00, 0x0a];
        let expected = WantHeartbeat { interval: 10 };

        assert_eq!(WantHeartbeat::from_bytes(&bytes[..]), expected);
    }

    // IAmCamera
    #[test]
    fn i_am_camera_from_bytes() {
        let bytes = [0x00, 0x42, 0x00, 0x64, 0x00, 0x3c];
        let expected = IAmCamera {
            road: 66,
            mile: 100,
            speed_limit: 60,
        };

        assert_eq!(IAmCamera::from_bytes(&bytes[..]), expected);
    }

    // IAmDispatcher
    #[test]
    fn i_am_dispatcher_from_bytes() {
        let bytes = [0x01, 0x00, 0x42];
        let expected = IAmDispatcher {
            numroads: 1,
            roads: vec![66],
        };
        assert_eq!(IAmDispatcher::from_bytes(&bytes[..]), expected);

        let bytes = [0x03, 0x00, 0x42, 0x01, 0x70, 0x13, 0x88];
        let expected = IAmDispatcher {
            numroads: 3,
            roads: vec![66, 368, 5000],
        };
        assert_eq!(IAmDispatcher::from_bytes(&bytes[..]), expected);
    }
}

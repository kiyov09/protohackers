use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    io,
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

trait IntoResponse {
    fn into_response(&self) -> String;
}

impl<T> IntoResponse for T
where
    T: Display,
{
    fn into_response(&self) -> String {
        format!("{}", self)
    }
}

#[derive(Debug)]
struct Connect {
    session: usize,
}

impl FromStr for Connect {
    type Err = InvalidMessage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidMessage);
        }

        let session_id = s[..s.len() - 1]
            .parse::<i32>()
            .map_err(|_| InvalidMessage)?;

        if session_id.is_negative() {
            return Err(InvalidMessage);
        }

        Ok(Connect {
            // Till length - 1 to remove the trailing slash
            session: session_id as usize,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Data {
    session: usize,
    pos: usize,
    data: String,
}

impl Data {
    fn unescape(s: &str) -> String {
        // Replace \\ with \ and \/ with /
        s.replace("\\\\", "\\").replace("\\/", "/")
    }

    fn escape(s: &str) -> String {
        // Replace \ with \\ and / with \/
        s.replace('\\', "\\\\").replace('/', "\\/")
    }

    fn process_for_response(data: &str) -> String {
        let Some(last_nl) = data.rfind('\n') else {
            return String::new();
        };

        if last_nl == 0 {
            return String::from("\n");
        }

        data[..last_nl]
            .lines()
            .map(|line| line.chars().rev().collect::<String>())
            .map(|line| Self::escape(&line))
            .map(|line| line + "\n")
            .collect::<String>()
    }

    async fn send(&self, addr: SocketAddr, socket: &UdpSocket) -> io::Result<()> {
        socket
            .send_to(self.into_response().as_bytes(), addr)
            .await
            .map(|_| ())
    }

    fn is_valid(data: &str) -> bool {
        // UPDATE THIS
        let col = data.chars().collect::<Vec<char>>();
        let wins = col.windows(3);
        for win in wins {
            if win.len() < 2 {
                continue;
            }
            if win[1] == '\\' && (win[0] != '\\' && win[2] != '\\' && win[2] != '/') {
                return false;
            }
            if win[1] == '/' && win[0] != '\\' {
                return false;
            }
            if win[2] == '/' && win[1] != '\\' {
                return false;
            }
        }
        true
    }
}

impl Display for Data {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "/data/{}/{}/{}/", self.session, self.pos, self.data)
    }
}

impl FromStr for Data {
    type Err = InvalidMessage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidMessage);
        }

        let mut parts = s[..s.len() - 1].splitn(3, '/');

        let session = parts
            .next()
            .ok_or(InvalidMessage)?
            .parse::<i32>()
            .map_err(|_| InvalidMessage)?;

        if session.is_negative() {
            return Err(InvalidMessage);
        }

        let pos = parts
            .next()
            .ok_or(InvalidMessage)?
            .parse::<i32>()
            .map_err(|_| InvalidMessage)?;

        if pos.is_negative() {
            return Err(InvalidMessage);
        }

        let data = parts.next().ok_or(InvalidMessage)?;

        if Self::is_valid(data) {
            Ok(Data {
                session: session as usize,
                pos: pos as usize,
                data: data.to_string(),
            })
        } else {
            Err(InvalidMessage)
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct Ack {
    session: usize,
    length: usize,
}

impl Ack {
    async fn send(&self, addr: SocketAddr, socket: &UdpSocket) -> io::Result<()> {
        socket
            .send_to(self.into_response().as_bytes(), addr)
            .await?;
        Ok(())
    }
}

impl FromStr for Ack {
    type Err = InvalidMessage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidMessage);
        }

        let mut parts = s.split('/').take(2);

        let session = parts
            .next()
            .ok_or(InvalidMessage)?
            .parse::<i32>()
            .map_err(|_| InvalidMessage)?;

        if session.is_negative() {
            return Err(InvalidMessage);
        }

        let length = parts
            .next()
            .ok_or(InvalidMessage)?
            .parse::<i32>()
            .map_err(|_| InvalidMessage)?;

        if length.is_negative() {
            return Err(InvalidMessage);
        }

        Ok(Ack {
            session: session as usize,
            length: length as usize,
        })
    }
}

impl Display for Ack {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "/ack/{}/{}/", self.session, self.length)
    }
}

#[derive(Debug)]
struct Close {
    session: usize,
}

impl Close {
    async fn send(&self, addr: SocketAddr, socket: &UdpSocket) -> io::Result<()> {
        socket
            .send_to(self.into_response().as_bytes(), addr)
            .await?;
        Ok(())
    }
}

impl Display for Close {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "/close/{}/", self.session)
    }
}

impl FromStr for Close {
    type Err = InvalidMessage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidMessage);
        }

        let session_id = s[..s.len() - 1]
            .parse::<i32>()
            .map_err(|_| InvalidMessage)?;

        if session_id.is_negative() {
            return Err(InvalidMessage);
        }

        Ok(Close {
            session: session_id as usize,
        })
    }
}

#[derive(Debug)]
struct InvalidMessage;

#[derive(Debug)]
enum Message {
    Connect(Connect), // Ex: /connect/SESSION/
    Data(Data),       // Ex: /data/SESSION/POS/DATA/
    Ack(Ack),         // Ex: /ack/SESSION/LENGTH/
    Close(Close),     // Ex: /close/SESSION/
}

impl FromStr for Message {
    type Err = InvalidMessage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if !s.starts_with('/') || !s.ends_with('/') {
            return Err(InvalidMessage);
        }

        if s.as_bytes().len() > 1000 {
            return Err(InvalidMessage);
        }

        let second_slash = s
            .chars()
            .enumerate()
            .position(|(i, c)| i != 0 && c == '/')
            .ok_or(InvalidMessage)?;

        match &s[..second_slash] {
            "/connect" => {
                let connect = s[second_slash + 1..].parse()?;
                Ok(Message::Connect(connect))
            }
            "/data" => {
                let data = s[second_slash + 1..].parse()?;
                Ok(Message::Data(data))
            }
            "/ack" => {
                let ack = s[second_slash + 1..].parse()?;
                Ok(Message::Ack(ack))
            }
            "/close" => {
                let close = s[second_slash + 1..].parse()?;
                Ok(Message::Close(close))
            }
            _ => Err(InvalidMessage),
        }
    }
}

// Time to wait before resend a message to the client (in seconds)
const RETRY_TIMEOUT: u64 = 5;
// Time to wait before consider a session expired (in seconds)
const EXPIRE_TIMEOUT: u64 = 30;

#[derive(Debug, Clone)]
struct Session {
    id: usize,
    // Client address
    peer_addr: SocketAddr,
    // Data we have received from the client so far
    payload: String,
    // Cursor on the payload indicating what we have sent so far
    curr_pos: usize,
    // Messages sent to the client that have not been acked yet
    send_data: HashMap<usize, (Instant, Data)>,
    // Maybe I dont need this
    all_sent_pos: Vec<usize>,
    // Sender to communicate with the server thread
    sender: Sender<ServerMsg>,
}

impl Session {
    async fn handle_ack(&mut self, ack: Ack) {
        let larger = self.all_sent_pos.iter().max().unwrap_or(&0);

        if larger < &ack.length {
            // Missbehaving client
            return self
                .sender
                .send(ServerMsg::ToClient(
                    Message::Close(Close { session: self.id }),
                    self.peer_addr,
                ))
                .await
                .expect("Failed to send to client");
        }

        if larger > &ack.length {
            let data_after_length = self.payload[ack.length..].to_string();
            let reversed_data = Data::process_for_response(&data_after_length);

            let mut pos = ack.length;

            // Send message to client in chunks of 500 bytes + header
            for chunk in reversed_data.chars().collect::<Vec<_>>().chunks(750) {
                // Send data to client
                let retrans_data = Data {
                    session: self.id,
                    pos,
                    data: chunk.iter().collect(),
                };

                // Save for retransmission
                pos += chunk.len();
                self.send_data
                    .insert(pos, (Instant::now(), retrans_data.clone()));
                self.all_sent_pos.push(pos);

                self.sender
                    .send(ServerMsg::ToClient(
                        Message::Data(retrans_data.clone()),
                        self.peer_addr,
                    ))
                    .await
                    .expect("Failed to send to client");
            }

            return;
        }

        self.send_data.remove(&ack.length);
    }

    async fn handle_data(&mut self, d: Data) {
        if d.pos != self.payload.len() {
            return self
                .sender
                .send(ServerMsg::ToClient(
                    Message::Ack(Ack {
                        session: self.id,
                        length: self.payload.len(),
                    }),
                    self.peer_addr,
                ))
                .await
                .expect("Failed to send to client");
        }

        let incoming_data = Data::unescape(&d.data);
        self.payload.push_str(&incoming_data);

        self.sender
            .send(ServerMsg::ToClient(
                Message::Ack(Ack {
                    session: self.id,
                    length: self.payload.len(),
                }),
                self.peer_addr,
            ))
            .await
            .expect("Failed to send to client");

        let reversed_data = Data::process_for_response(&self.payload[self.curr_pos..]);

        if reversed_data.is_empty() {
            return;
        }

        let mut to_send = Vec::new();

        // Send message to client in chunks of 500 bytes + header
        for chunk in reversed_data.chars().collect::<Vec<_>>().chunks(750) {
            // Send data to client
            let data_for_client = Data {
                session: self.id,
                pos: self.curr_pos,
                data: chunk.iter().collect(),
            };

            // Save for retransmission
            self.curr_pos += chunk.len();

            self.send_data
                .insert(self.curr_pos, (Instant::now(), data_for_client.clone()));
            self.all_sent_pos.push(self.curr_pos);

            to_send.push(data_for_client);
        }

        for (idx, data) in to_send.into_iter().enumerate() {
            self.sender
                .send(ServerMsg::ToClient(Message::Data(data), self.peer_addr))
                .await
                .expect("Failed to send to client");
        }
    }
}

#[derive(Debug)]
enum ServerMsg {
    Socket(Message, SocketAddr),
    Activity,
    Retrans,
    ToClient(Message, SocketAddr),
}

struct Server {
    // Map session id to Session data and the last time it was active
    sessions: HashMap<usize, (Session, Instant)>,

    // Communication channels
    sender: Sender<ServerMsg>,
    receiver: Receiver<ServerMsg>,

    // Udp socket to read messages from
    socket: Arc<UdpSocket>,
}

impl Server {
    fn new(socket: UdpSocket) -> Self {
        let (sd, rc) = channel::<ServerMsg>(1000000);

        Self {
            sessions: HashMap::new(),
            sender: sd,
            receiver: rc,
            socket: socket.into(),
        }
    }

    async fn run(&mut self) {
        let _handler = self.spawn_activity(self.sender.clone()).await;
        let _handler = self.spawn_retrans(self.sender.clone()).await;

        self.spawn_socket(self.sender.clone()).await;

        while let Some(msg) = self.receiver.recv().await {
            match msg {
                ServerMsg::Socket(msg, addr) => match msg {
                    Message::Connect(conn) => self.handle_connect(conn.session, addr).await,
                    Message::Data(data) => self.handle_data(data, addr).await,
                    Message::Ack(ack) => self.handle_ack(ack, addr).await,
                    Message::Close(close) => self.handle_close(close, addr).await,
                },
                ServerMsg::Activity => {
                    let now = Instant::now();
                    let mut to_remove = Vec::new();

                    self.sessions.retain(|_, (sess, last)| {
                        if now.duration_since(*last).as_secs() > EXPIRE_TIMEOUT {
                            to_remove.push(sess.clone());
                            false
                        } else {
                            true
                        }
                    });

                    for sess in to_remove {
                        self.sessions.remove(&sess.id);
                        let _ = Close { session: sess.id }
                            .send(sess.peer_addr, &self.socket)
                            .await;
                    }
                }
                ServerMsg::Retrans => {
                    let now = Instant::now();
                    for (sess, _) in self.sessions.values_mut() {
                        for (_, (last, data)) in sess.send_data.iter() {
                            if now.duration_since(*last).as_secs() > RETRY_TIMEOUT {
                                data.send(sess.peer_addr, &self.socket)
                                    .await
                                    .expect("Failed to send data");
                            }
                        }
                    }
                }
                ServerMsg::ToClient(msg, addr) => match msg {
                    Message::Connect(_) => unreachable!("Invalid message"),
                    Message::Data(d) => d.send(addr, &self.socket).await.expect("Failed to send"),
                    Message::Ack(a) => a.send(addr, &self.socket).await.expect("Failed to send"),
                    Message::Close(c) => {
                        self.sessions.remove(&c.session);
                        c.send(addr, &self.socket).await.expect("Failed to send")
                    }
                },
            }
        }
    }

    async fn spawn_socket(&mut self, sender: Sender<ServerMsg>) {
        let socket = Arc::clone(&self.socket);

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];

            while let Ok((bytes_read, addr)) = socket.recv_from(&mut buf).await {
                let msg = String::from_utf8_lossy(&buf[..bytes_read]);

                match msg.parse::<Message>() {
                    Ok(msg) => sender
                        .send(ServerMsg::Socket(msg, addr))
                        .await
                        .expect("Failed to send message"),
                    Err(_) => {
                        continue;
                    }
                };
            }
        });
    }

    async fn spawn_activity(&mut self, sd: Sender<ServerMsg>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(EXPIRE_TIMEOUT)).await;
                sd.send(ServerMsg::Activity)
                    .await
                    .expect("Failed to send activity check");
            }
        })
    }

    async fn spawn_retrans(&mut self, sd: Sender<ServerMsg>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(RETRY_TIMEOUT)).await;
                sd.send(ServerMsg::Retrans)
                    .await
                    .expect("Failed to send retransmission check");
            }
        })
    }

    async fn handle_connect(&mut self, sess_id: usize, addr: SocketAddr) {
        let new_session = Session {
            id: sess_id,
            peer_addr: addr,
            payload: String::new(),
            curr_pos: 0,
            send_data: HashMap::new(),
            all_sent_pos: Vec::new(),
            sender: self.sender.clone(),
        };

        let _ = self
            .sessions
            .entry(sess_id)
            .or_insert((new_session, Instant::now()));

        Ack {
            session: sess_id,
            length: 0,
        }
        .send(addr, &self.socket)
        .await
        .expect("Failed to send ack")
    }

    async fn handle_close(&mut self, close: Close, addr: SocketAddr) {
        self.sessions.remove(&close.session);
        Close {
            session: close.session,
        }
        .send(addr, &self.socket)
        .await
        .expect("Failed to send close")
    }

    async fn handle_data(&mut self, data: Data, addr: SocketAddr) {
        let Some((sess, inst)) = self.sessions.get_mut(&data.session) else {
            return Close {
                session: data.session,
            }.send(addr, &self.socket).await.expect("Failed to send close");
        };

        *inst = Instant::now();
        sess.handle_data(data).await
    }

    async fn handle_ack(&mut self, ack: Ack, addr: SocketAddr) {
        let Some((sess, inst)) = self.sessions.get_mut(&ack.session) else {
            return Close {
                session: ack.session,
            }.send(addr, &self.socket).await.expect("Failed to send close");
        };

        *inst = Instant::now();
        sess.handle_ack(ack).await
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // let socket_add = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));

    // For use with fly.io
    // See [here](https://fly.io/docs/app-guides/udp-and-tcp/) for more info
    let socket_add = "fly-global-services:5000";
    let sock = UdpSocket::bind(socket_add).await?;

    let mut server = Server::new(sock);
    server.run().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
}

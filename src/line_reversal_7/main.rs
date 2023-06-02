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

fn get_positive_i32(s: &str) -> Result<i32, InvalidMessage> {
    s.parse::<i32>().map_err(|_| InvalidMessage).and_then(|i| {
        if i.is_negative() {
            Err(InvalidMessage)
        } else {
            Ok(i)
        }
    })
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

        let session_id = get_positive_i32(&s[..s.len() - 1])?;

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
    // Methods
    async fn send(&self, addr: SocketAddr, socket: &UdpSocket) -> io::Result<()> {
        socket
            .send_to(self.into_response().as_bytes(), addr)
            .await
            .map(|_| ())
    }

    // Associated Functions

    fn unescape(s: &str) -> String {
        s.replace("\\\\", "\\").replace("\\/", "/")
    }

    fn escape(s: &str) -> String {
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

        let session = get_positive_i32(parts.next().ok_or(InvalidMessage)?)?;
        let pos = get_positive_i32(parts.next().ok_or(InvalidMessage)?)?;
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
            .await
            .map(|_| ())
    }
}

impl FromStr for Ack {
    type Err = InvalidMessage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidMessage);
        }

        let mut parts = s.split('/').take(2);
        let session = get_positive_i32(parts.next().ok_or(InvalidMessage)?)?;
        let length = get_positive_i32(parts.next().ok_or(InvalidMessage)?)?;

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
            .await
            .map(|_| ())
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

        let session_id = get_positive_i32(&s[..s.len() - 1])?;

        Ok(Close {
            session: session_id as usize,
        })
    }
}

#[derive(Debug)]
struct InvalidMessage;

/// Messages that we can receive from the client.
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

/// Time to wait before resend a message to the client (in seconds)
const RETRY_TIMEOUT: u64 = 5;
/// Time to wait before consider a session expired (in seconds)
const EXPIRE_TIMEOUT: u64 = 10;

/// A client's session.
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
    // TODO: Maybe I dont need this
    all_sent_pos: Vec<usize>,
    // Sender to communicate with the server thread
    sender: Sender<ServerMsg>,
}

impl Session {
    async fn handle_ack(&mut self, ack: Ack) {
        let larger = self.all_sent_pos.iter().max().unwrap_or(&0);

        // Check if ack is valid and close session if it is not
        if larger < &ack.length {
            // Missbehaving client
            return self.send(Message::Close(Close { session: self.id })).await;
        }

        // Checking if we need to retransmit data
        if larger > &ack.length {
            // TODO:
            // This code has a lot of duplication with the *handle_data* method.
            // I need to review how to refactor this.

            // Get data after the acked position and reverse it for retransmission
            let data_after_length = self.payload[ack.length..].to_string();
            let reversed_data = Data::process_for_response(&data_after_length);

            // To be used as the marker for the next data to be sent
            let mut pos = ack.length;

            // Send message to client in chunks of 750 bytes
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

                // Send to client
                self.send(Message::Data(retrans_data.clone())).await;
            }

            return;
        }

        // Remove the data that was acked from the list of data to be retransmitted
        self.send_data.remove(&ack.length);
    }

    async fn handle_data(&mut self, d: Data) {
        // Ignore any data message that is sending data from a position that we already have
        if d.pos != self.payload.len() {
            // Let the client know that we already have that data
            return self
                .send(Message::Ack(Ack {
                    session: self.id,
                    length: self.payload.len(),
                }))
                .await;
        }

        // Unscape data and add it to the payload
        let incoming_data = Data::unescape(&d.data);
        self.payload.push_str(&incoming_data);

        // Let the client know that we received the data
        self.send(Message::Ack(Ack {
            session: self.id,
            length: self.payload.len(),
        }))
        .await;

        // Process data to be sent back to the client
        let reversed_data = Data::process_for_response(&self.payload[self.curr_pos..]);

        // As we sent only complete lines, if the data is empty means that there
        // is no complete line from the current position, so we dont need to
        // send anything back
        if reversed_data.is_empty() {
            return;
        }

        // Send message to client in chunks of 750 bytes
        for chunk in reversed_data.chars().collect::<Vec<_>>().chunks(750) {
            // Data for the client
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

            // Send reversed data back
            self.send(Message::Data(data_for_client)).await;
        }
    }

    async fn send(&self, msg: Message) {
        self.sender
            .send(ServerMsg::ToClient(msg, self.peer_addr))
            .await
            .expect("Failed to send to client");
    }
}

/// Internal messages for the threads to communicate with the server
#[derive(Debug)]
enum ServerMsg {
    Socket(Message, SocketAddr),
    Activity,
    Retrans,
    ToClient(Message, SocketAddr),
}

/// Server struct, this handles all the communication with the clients
/// and the retransmission of data.
struct Server {
    // Map session id to Session data and the last time it was active
    sessions: HashMap<usize, (Session, Instant)>,

    // Communication channels
    sender: Sender<ServerMsg>,
    receiver: Receiver<ServerMsg>,

    // Udp socket to read messages from
    socket: Arc<UdpSocket>,

    // Handlers for the threads
    activity_handler: Option<JoinHandle<()>>,
    retrans_handler: Option<JoinHandle<()>>,
}

impl Server {
    /// Creates a new server from a UdpSocket
    fn new(socket: UdpSocket) -> Self {
        let (sd, rc) = channel::<ServerMsg>(100);

        Self {
            sessions: HashMap::new(),
            sender: sd,
            receiver: rc,
            socket: socket.into(),

            activity_handler: None,
            retrans_handler: None,
        }
    }

    /// Put the server to work
    async fn run(&mut self) {
        // Spawn threads for activity checking and retransmission of data
        // TODO:
        // I need to find a way to stop this threads when they have nothing to do
        // and respawn them when needed.
        // let _handler = self.spawn_activity(self.sender.clone()).await;
        // let _handler = self.spawn_retrans(self.sender.clone()).await;

        // Spawn thread to handle incoming messages from clients
        self.spawn_socket(self.sender.clone()).await;

        while let Some(msg) = self.receiver.recv().await {
            match msg {
                // Message from the socket (network connections)
                ServerMsg::Socket(msg, addr) => match msg {
                    Message::Connect(conn) => self.handle_connect(conn.session, addr).await,
                    Message::Data(data) => self.handle_data(data, addr).await,
                    Message::Ack(ack) => self.handle_ack(ack, addr).await,
                    Message::Close(close) => self.handle_close(close, addr).await,
                },
                // Activity check
                ServerMsg::Activity => self.handle_activity_check().await,
                // Retransmission of data
                ServerMsg::Retrans => self.handle_retransmission_check().await,
                // Messages comming from the sessions (to send to the clients)
                ServerMsg::ToClient(msg, addr) => match msg {
                    // A session will never send a connect message
                    Message::Connect(_) => unreachable!("Invalid message"),
                    Message::Data(data) => {
                        if self.retrans_handler.is_none() {
                            self.retrans_handler =
                                Some(self.spawn_retrans(self.sender.clone()).await);
                        }
                        data.send(addr, &self.socket).await.expect("Failed to send")
                    }
                    Message::Ack(ack) => {
                        ack.send(addr, &self.socket).await.expect("Failed to send")
                    }
                    Message::Close(close) => self.handle_close(close, addr).await,
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

    async fn handle_activity_check(&mut self) {
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
            self.handle_close(Close { session: sess.id }, sess.peer_addr)
                .await;
        }
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

    async fn handle_retransmission_check(&mut self) {
        // TODO: Move this to its own method
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

        if self.activity_handler.is_none() {
            self.activity_handler = Some(self.spawn_activity(self.sender.clone()).await);
        }

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

        if self.sessions.is_empty() {
            if let Some(handler) = self.activity_handler.take() {
                handler.abort();
            }
            if let Some(handler) = self.retrans_handler.take() {
                handler.abort();
            }
        }

        close
            .send(addr, &self.socket)
            .await
            .expect("Failed to send close")
    }

    async fn handle_data(&mut self, data: Data, addr: SocketAddr) {
        // Get the session associated with the message or close the connection if it
        // has not been open yet
        let Some((sess, inst)) = self.sessions.get_mut(&data.session) else {
            return Close {
                session: data.session,
            }.send(addr, &self.socket).await.expect("Failed to send close");
        };

        // Update the last activity time
        *inst = Instant::now();

        // Let the session handle the data
        sess.handle_data(data).await
    }

    async fn handle_ack(&mut self, ack: Ack, addr: SocketAddr) {
        // Get the session associated with the message or close the connection if it
        // has not been open yet
        let Some((sess, inst)) = self.sessions.get_mut(&ack.session) else {
            return Close {
                session: ack.session,
            }.send(addr, &self.socket).await.expect("Failed to send close");
        };

        // Update the last activity time
        *inst = Instant::now();

        // Let the session handle the ack
        sess.handle_ack(ack).await
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // For running locally
    let socket_add = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));

    // For use with fly.io
    // See [here](https://fly.io/docs/app-guides/udp-and-tcp/) for more info
    // let socket_add = "fly-global-services:5000";

    // Create the socket
    let sock = UdpSocket::bind(socket_add).await?;

    // Start the server
    let mut server = Server::new(sock);
    server.run().await;

    Ok(())
}

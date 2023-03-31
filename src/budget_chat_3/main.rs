use std::io::{prelude::*, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{channel, Receiver, Sender};

#[derive(Debug)]
enum Action {
    Join(User),
    Disconnect(String),
    SendMessage(String, String),
}

#[derive(Debug)]
struct User {
    username: String,
    stream: TcpStream,
}

impl User {
    pub fn new(username: String, stream: TcpStream) -> Self {
        Self { username, stream }
    }

    fn is_valid_username(username: &str) -> bool {
        !username.is_empty() && username.chars().all(|c| c.is_alphanumeric())
    }
}

struct ChatServer {
    users: Vec<User>,
    receiver: Receiver<Action>,
}

impl ChatServer {
    pub fn new(rx: Receiver<Action>) -> Self {
        Self {
            users: Vec::new(),
            receiver: rx,
        }
    }

    pub fn run(&mut self) {
        while let Ok(action) = self.receiver.recv() {
            match action {
                Action::Join(user) => self.add_user(user),
                Action::Disconnect(username) => self.remove_user(username),
                Action::SendMessage(username, message) => {
                    self.send_message_from_user(&username, message)
                }
            }
        }
    }

    pub fn add_user(&mut self, mut user: User) {
        // The names of the users in the room
        let users = self
            .users
            .iter()
            .map(|u| u.username.clone())
            .collect::<Vec<String>>()
            .join(", ");

        // Send the list of users to the new user
        user.stream
            .write_all(format!("* Users in the room: {}\n", users).as_bytes())
            .unwrap();

        // Inform the other users that a new user has joined the room
        self.broadcast(&format!("* {} is now in the room!\n", user.username.trim()));

        // Add the new user to the list of users
        self.users.push(user);
    }

    pub fn remove_user(&mut self, username: String) {
        // Remove the user from the list of users
        self.users.retain(|u| u.username != username);

        // Inform the other users that a user has left the room
        self.broadcast(&format!("* {} left the room!\n", username.trim()));
    }

    pub fn send_message_from_user(&mut self, username: &str, message: String) {
        // Send the message to all users except the sender
        self.users
            .iter_mut()
            .filter(|u| u.username != username)
            .for_each(|u| {
                u.stream
                    .write_all(format!("[{}] {}", username.trim(), message).as_bytes())
                    .unwrap();
            });
    }

    // Send a message to all users
    fn broadcast<T: Into<String>>(&mut self, message: T) {
        let message = message.into();
        self.users.iter_mut().for_each(|u| {
            u.stream.write_all(message.as_bytes()).unwrap();
        });
    }
}

fn handle_connection(mut stream: TcpStream, sx: Sender<Action>) {
    let stream_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    // Send the welcome message
    let welcome_message = "Welcome! Before continue, set your username:\n";
    stream.write_all(welcome_message.as_bytes()).unwrap();

    // Prepare a buffer to read from the stream
    let mut reader = BufReader::new(&stream_clone);
    let mut buffer = String::new();

    // Read the username from the stream
    if reader.read_line(&mut buffer).is_err() {
        return;
    }

    // Check if the username is valid
    let username = buffer.trim().to_owned();
    if !User::is_valid_username(&username) {
        let invalid_username_message = "Invalid username! Try again.\n";
        stream
            .write_all(invalid_username_message.as_bytes())
            .expect("Failed to send message");

        return;
    }
    buffer.clear();

    // Create a new user and inform the server about it
    let new_user = User::new(
        username.clone(),
        stream_clone.try_clone().expect("Failed to clone stream"),
    );
    sx.send(Action::Join(new_user))
        .expect("Failed to add new user");

    // Read messages from the user and send them to the server for broadcasting
    while let Ok(bytes_read) = reader.read_line(&mut buffer) {
        // Close the connection if the user has closed the connection
        if bytes_read == 0 {
            println!("Connection closed!");
            break;
        }

        // Send the message to the server
        sx.send(Action::SendMessage(username.clone(), buffer.clone()))
            .expect("Failed to send message");

        buffer.clear();
    }

    // Inform the server that the user has disconnected
    sx.send(Action::Disconnect(username))
        .expect("Failed to inform other users");
}

fn main() -> std::io::Result<()> {
    let socket_add = SocketAddr::from(([0, 0, 0, 0], 5000));
    let listener = TcpListener::bind(socket_add).unwrap();

    // Create a channel to communicate between the ChatServer and the
    // therads that handle the connections
    let (sd, rc) = channel::<Action>();

    // Spawn a thread to run the ChatServer
    std::thread::spawn(move || {
        let mut server = ChatServer::new(rc);
        server.run();
    });

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

        // Clone the sender before moving it to the thread
        let sd = sd.clone();

        // Spawn a thread to handle the connection
        thread_pool.spawn(move || {
            println!("Connection established!");
            handle_connection(stream, sd);
        });
    }

    Ok(())
}

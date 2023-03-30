use std::io::{prelude::*, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

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

#[derive(Default)]
struct ChatServer {
    users: Vec<User>,
}

impl ChatServer {
    pub fn new() -> Self {
        Self::default()
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

fn handle_connection(mut stream: TcpStream, server: Arc<Mutex<ChatServer>>) {
    let stream_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    let welcome_message = "Welcome! Before continue, set your username:\n";
    stream.write_all(welcome_message.as_bytes()).unwrap();

    let mut reader = BufReader::new(&stream_clone);
    let mut buffer = String::new();

    if reader.read_line(&mut buffer).is_err() {
        return;
    }

    let username = buffer.trim().to_owned();
    if !User::is_valid_username(&username) {
        let invalid_username_message = "Invalid username! Try again.\n";
        stream
            .write_all(invalid_username_message.as_bytes())
            .unwrap();
        return;
    }
    buffer.clear();

    let new_user = User::new(username.clone(), stream_clone.try_clone().unwrap());
    {
        server.lock().unwrap().add_user(new_user);
    }

    while let Ok(bytes_read) = reader.read_line(&mut buffer) {
        if bytes_read == 0 {
            println!("Connection closed!");
            break;
        }

        server
            .lock()
            .unwrap()
            .send_message_from_user(&username, buffer.clone());

        buffer.clear();
    }

    server.lock().unwrap().remove_user(username);
}

fn main() -> std::io::Result<()> {
    let socket_add = SocketAddr::from(([0, 0, 0, 0], 5000));
    let listener = TcpListener::bind(socket_add).unwrap();

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(10)
        .build()
        .expect("Failed to build thread pool");

    let chat_server: Arc<Mutex<ChatServer>> = Arc::new(Mutex::new(ChatServer::new()));

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };

        let chat_server = Arc::clone(&chat_server);
        thread_pool.spawn(move || {
            println!("Connection established!");
            handle_connection(stream, chat_server);
        });
    }

    Ok(())
}

use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    net::SocketAddr,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpListener, TcpStream,
    },
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
};

// A revison of a file
type Rev = usize;
// The content of a file
type FileContent = String;

//
// A file stored in the VCS
// It's created/updated as the result of a successful PUT
//
#[derive(Debug, PartialEq, Eq)]
struct File {
    name: String,
    content: BTreeMap<Rev, FileContent>,
}

impl File {
    fn new(name: &str, content: String) -> Self {
        Self {
            name: String::from(name),
            content: BTreeMap::from([(1, content)]),
        }
    }

    // Updates a file with the given content
    // If the content is the same as the latest revision, nothing happens
    fn update(&mut self, content: String) {
        let trimmed = content.trim();

        if let Some((_, cont)) = self.content.last_key_value() {
            if cont.trim() == trimmed {
                return;
            }
        }

        let rev = self.content.len() + 1;
        self.content.insert(rev, content);
    }

    fn get_latest_rev(&self) -> Option<usize> {
        self.content.last_key_value().map(|(rev, _)| *rev)
    }
}

//
// A directory stored in the VCS
// They're created when a file is PUT in a directory that doesn't exist
//
#[derive(Debug, PartialEq, Eq)]
struct Dir {
    name: String,
    entries: Vec<DirEntry>,
}

impl Dir {
    fn new(entry_name: &str) -> Self {
        Self {
            name: String::from(entry_name),
            entries: Vec::new(),
        }
    }
    //
    // Gets a `Dir` who is a child of this directory
    //
    fn get_dir(&self, name: &str) -> Option<&Dir> {
        self.entries.iter().find_map(|entry| match entry {
            DirEntry::Dir(dir) if dir.name == name => Some(dir),
            _ => None,
        })
    }

    //
    // Gets a mutable `Dir` who is a child of this directory
    //
    fn get_dir_mut(&mut self, name: &str) -> Option<&mut Dir> {
        self.entries.iter_mut().find_map(|entry| match entry {
            DirEntry::Dir(dir) if dir.name == name => Some(dir),
            _ => None,
        })
    }

    //
    // Gets a `File` who is a child of this directory
    //
    fn get_file(&self, name: &str) -> Option<&File> {
        self.entries.iter().find_map(|entry| match entry {
            DirEntry::File(file) if file.name == name => Some(file),
            _ => None,
        })
    }

    //
    // List all entries in this directory
    // If a file and a directory have the same name, the directory is not listed
    //
    fn list(&self) -> Vec<&DirEntry> {
        let mut ent = self.entries.iter().collect::<Vec<_>>();
        // This'll make sure that files are listed before folders
        ent.sort();

        // So, if dedup needs to remove an entry, it'll remove the folder
        ent.dedup_by_key(|entry| match entry {
            DirEntry::File(file) => file.name.clone(),
            DirEntry::Dir(dir) => dir.name.clone(),
        });

        // Return the entries
        ent
    }

    fn update_or_create_file(&mut self, name: &str, content: String) -> &File {
        // Get a mutable reference to the file with the given name
        // who is a child of this directory
        let mutable_file = self
            .entries
            .iter_mut()
            .find(|entry| match entry {
                DirEntry::File(file) => file.name == name,
                _ => false,
            })
            .and_then(|entry| match entry {
                DirEntry::File(file) => Some(file),
                _ => None,
            });

        // Update the file if it exists, otherwise create it
        match mutable_file {
            Some(file) => {
                file.update(content);
            }
            None => {
                self.entries.push(DirEntry::File(File::new(name, content)));
            }
        };

        self.get_file(name).unwrap()
    }
}

//
// A directory entry, either a file or a directory
#[derive(Debug, PartialEq, Eq)]
enum DirEntry {
    Dir(Dir),
    File(File),
}

//
// Implement `Ord` and `PartialOrd` so that `DirEntry`s can be sorted
// This is used in `Dir::list`
// If a file and a directory have the same name, the file is "Less", otherwise
// depends on the name property
//
impl Ord for DirEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (DirEntry::File(f), DirEntry::Dir(d)) => {
                if f.name == d.name {
                    std::cmp::Ordering::Less
                } else {
                    f.name.cmp(&d.name)
                }
            }
            (DirEntry::Dir(d), DirEntry::File(f)) => {
                if f.name == d.name {
                    std::cmp::Ordering::Greater
                } else {
                    d.name.cmp(&f.name)
                }
            }
            (DirEntry::File(f1), DirEntry::File(f2)) => f1.name.cmp(&f2.name),
            (DirEntry::Dir(d1), DirEntry::Dir(d2)) => d1.name.cmp(&d2.name),
        }
    }
}

impl PartialOrd for DirEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

//
// To be used as part of the response to the `LIST` command
//
impl Display for DirEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DirEntry::Dir(d) => write!(f, "{}/ DIR", d.name),
            DirEntry::File(file) => write!(f, "{} r{}", file.name, file.content.len()),
        }
    }
}

//
// The VCS (Voracious Code Storage)
//
// It's a tree of directories and files starting at the root (/) directory
//
#[derive(Debug, PartialEq)]
struct Vcs {
    root: Dir,
}

impl Vcs {
    fn new() -> Self {
        Self {
            root: Dir {
                name: String::from("/"),
                entries: vec![],
            },
        }
    }

    fn get(&self, path: &str) -> Option<&File> {
        match path {
            "/" => None,
            _ => self.find_file(path),
        }
    }

    fn list(&self, path: &str) -> Option<Vec<&DirEntry>> {
        match path {
            "/" => Some(self.root.list()),
            _ => self.find_dir(path).map(|dir| dir.list()),
        }
    }

    fn put(&mut self, path: &str, clone: String) -> &File {
        let parts = Self::iter_from_path(path).collect::<Vec<_>>();
        let (dir_path, file_name) = parts.split_at(parts.len() - 1);

        // If the path is just a file name, put it in the root directory
        if dir_path.is_empty() {
            return self.root.update_or_create_file(file_name[0], clone);
        }

        // The current directory we are looking at
        let mut current_dir = &mut self.root;

        // Iterate through the path till we find the file
        // If we find a directory that doesn't exist, create it
        // and continue
        for entry_name in dir_path.iter().peekable() {
            if current_dir.get_dir_mut(entry_name).is_none() {
                current_dir
                    .entries
                    .push(DirEntry::Dir(Dir::new(entry_name)));
            }
            current_dir = current_dir
                .get_dir_mut(entry_name)
                .expect("We just checked this");
        }

        current_dir.update_or_create_file(file_name[0], clone)
    }

    //
    // TODO:
    // `find_dir` and `find_file` are very similar, maybe we extract the common code
    // into a function.
    //
    fn find_dir(&self, path: &str) -> Option<&Dir> {
        // The current directory we are looking at
        let mut current_dir = &self.root;

        // Yield the parts of the path
        let mut path = Self::iter_from_path(path).peekable();

        // Iterate through the path till we find the file
        while let Some(entry_name) = path.next() {
            // Get the dir entry with the name `dir_name`
            match path.peek() {
                Some(_) => {
                    // Find the folder with the name `entry_name`
                    current_dir = current_dir.get_dir(entry_name)?
                }
                None => {
                    // Find the entry with the name `entry_name`
                    return current_dir.get_dir(entry_name);
                }
            }
        }

        // We should have found the file by now
        None
    }

    fn find_file(&self, path: &str) -> Option<&File> {
        // The current directory we are looking at
        let mut current_dir = &self.root;

        // Yield the parts of the path
        let mut path = Self::iter_from_path(path).peekable();

        // Iterate through the path till we find the file
        while let Some(entry_name) = path.next() {
            // Get the dir entry with the name `dir_name`
            match path.peek() {
                Some(_) => {
                    // Find the folder with the name `entry_name`
                    current_dir = current_dir.get_dir(entry_name)?
                }
                None => {
                    // Find the entry with the name `entry_name`
                    return current_dir.get_file(entry_name);
                }
            }
        }

        // We should have found the file by now
        None
    }

    //
    // Ex:
    // /foo/bar/baz -> ["foo", "bar", "baz"] (because skip the first /)
    // /foo/bar/baz/ -> ["foo", "bar", "baz"] (because skip trailing /)
    //
    fn iter_from_path(path: &str) -> impl Iterator<Item = &str> {
        path.split('/').skip(1).take_while(|part| !part.is_empty())
    }
}

//
// Just fancy name for readability
//
type ResponseMsg = String;

//
// Client -> Server commands
//
#[derive(Debug, PartialEq)]
enum Command {
    Help,
    Get(String, Option<String>),
    List(String),
    Put(String, String),
}

//
// Handles the request from the client (Commands) and
// invokes the appropriate methods on the VCS
//
struct Server {
    vcs: Vcs,

    // Sender side of the channel. It's clone and shared with
    // any new client that connects to the server
    tx: Sender<(Command, oneshot::Sender<ResponseMsg>)>,
    // Receiver side of the channel. Receives the commands from
    // the client
    rx: Receiver<(Command, oneshot::Sender<ResponseMsg>)>,
}

impl Server {
    fn new() -> Self {
        let (tx, rx) = channel(100);

        Server {
            vcs: Vcs::new(),
            tx,
            rx,
        }
    }

    //
    // Clients will use this to obtain a Sender to be able to communicate
    // with the server
    //
    fn get_tx(&self) -> Sender<(Command, oneshot::Sender<ResponseMsg>)> {
        self.tx.clone()
    }

    //
    // This is the main loop of the server. It will wait for commands
    // from the client and execute them
    //
    async fn run(&mut self) {
        while let Some((cmd, response_channel)) = self.rx.recv().await {
            match cmd {
                Command::Help => {
                    response_channel
                        .send("OK usage: HELP|GET|PUT|LIST\n".to_string())
                        .expect("Can't send response");
                }
                Command::Get(path, rev) => match self.vcs.get(&path) {
                    Some(file) => {
                        let content = match rev {
                            Some(rev) => {
                                let rev = if let Some(with_r_removed) = rev.strip_prefix('r') {
                                    with_r_removed
                                } else {
                                    rev.as_str()
                                };
                                let rev = rev
                                    .chars()
                                    .take_while(|c| c.is_numeric())
                                    .collect::<String>()
                                    .parse::<usize>()
                                    .unwrap_or(0);

                                if let Some(content) = file.content.get(&rev) {
                                    content
                                } else {
                                    response_channel
                                        .send("ERR no such revision\n".to_string())
                                        .expect("Can't send response");

                                    continue;
                                }
                            }
                            None => file.content.last_key_value().expect("We know it's there").1,
                        };

                        response_channel
                            .send(format!("OK {}\n{}", content.len(), content))
                            .expect("Can't send response");
                    }
                    None => response_channel
                        .send("ERR no such file\n".to_string())
                        .expect("Can't send response"),
                },
                Command::List(path) => {
                    let files = self.vcs.list(&path);
                    let files = files.unwrap_or(vec![]);

                    response_channel
                        .send(format!(
                            "OK {}\n{}",
                            files.len(),
                            files
                                .iter()
                                .map(|f| match f {
                                    DirEntry::File(file) =>
                                        format!("{} r{}\n", file.name, file.content.len()),
                                    DirEntry::Dir(dir) => format!("{}/ DIR\n", dir.name),
                                })
                                .collect::<Vec<_>>()
                                .join("")
                        ))
                        .expect("Can't send response");
                }
                Command::Put(path, content) => {
                    let file = self.vcs.put(&path, content);
                    response_channel
                        .send(format!(
                            "OK r{}\n",
                            file.get_latest_rev().unwrap_or_default()
                        ))
                        .expect("Can't send response");
                }
            }
        }
    }
}

//
// All possible errors related to the client
// sending bad commands
//
#[derive(Debug, Clone)]
enum BadRequestError {
    IllegalMethod(String),
    BadUsage(&'static str),
    IllegalDir,
    IllegalFile,
    TextOnly, // TODO
}

//
// Used to transform each variant into the string
// we'll send back to the client.
//
impl Display for BadRequestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BadRequestError::IllegalMethod(s) => writeln!(f, "ERR illegal method: {}", s),
            BadRequestError::BadUsage(s) => writeln!(f, "ERR usage: {}", s),
            BadRequestError::IllegalDir => writeln!(f, "ERR illegal dir name"),
            BadRequestError::IllegalFile => writeln!(f, "ERR illegal file name"),
            BadRequestError::TextOnly => writeln!(f, "ERR text files only"),
        }
    }
}

//
// A single client connection
//
struct Client {
    tx: Sender<(Command, oneshot::Sender<ResponseMsg>)>,
}

impl Client {
    //
    // Main loop of the client. It will:
    // - Read from the socket
    // - Try to turn the input into a Command
    // - Send the command to the server
    // - Wait for the response from the server
    // - Send the response back to the client
    //
    async fn run(&mut self, mut socket: TcpStream) {
        println!(">>> Client connected");

        let (reader, writer) = socket.split();

        let mut writer = BufWriter::new(writer);
        self.send_ready_and_flush(&mut writer).await;

        let mut reader = BufReader::new(reader);
        let mut buf = String::new();

        while let Ok(n) = reader.read_line(&mut buf).await {
            if n == 0 {
                break;
            }

            let cmd = buf.trim().to_string();
            match Self::process_request(cmd, &mut reader).await {
                Ok(cmd) => {
                    let (tx, rx) = oneshot::channel();
                    self.tx.send((cmd, tx)).await.expect("send failed");

                    match rx.await {
                        Ok(response) => {
                            writer.write_all(response.as_bytes()).await.unwrap();
                            self.send_ready_and_flush(&mut writer).await;
                        }
                        Err(_) => {
                            println!(">>> Channel closed");
                            break;
                        }
                    }
                }
                Err(error) => {
                    writer
                        .write_all(error.to_string().as_bytes())
                        .await
                        .unwrap();

                    if let BadRequestError::IllegalMethod(_) = error {
                        writer.flush().await.unwrap();
                        break;
                    }

                    self.send_ready_and_flush(&mut writer).await;
                }
            }

            buf.clear();
        }
    }

    async fn process_request(
        cmd: String,
        reader: &mut BufReader<ReadHalf<'_>>,
    ) -> Result<Command, BadRequestError> {
        let mut parts = cmd.split_whitespace();
        let method = parts.next().unwrap_or_default();

        match method.to_uppercase().as_str() {
            "HELP" => Ok(Command::Help),
            "GET" => Self::handle_get(parts),
            "LIST" => Self::handle_list(parts),
            "PUT" => Self::handle_put(parts, reader).await,
            _ => Err(BadRequestError::IllegalMethod(method.to_string())),
        }
    }

    fn handle_list(parts: std::str::SplitWhitespace<'_>) -> Result<Command, BadRequestError> {
        // Should be one (only one) part left in the iterator
        // And that part shoulw be a valid folder path

        // Make it peakable so we can check if there are more parts
        let mut parts = parts.peekable();

        // If not even one part, then it's a bad usage
        let path = parts.next().ok_or(BadRequestError::BadUsage("LIST dir"))?;

        // If there are more parts, then it's a bad usage
        if parts.peek().is_some() {
            return Err(BadRequestError::BadUsage("LIST dir"));
        }

        // Ok or IlegalDir based on the validity of the path
        if is_valid_dir_path(path) {
            Ok(Command::List(path.to_string()))
        } else {
            Err(BadRequestError::IllegalDir)
        }
    }

    fn handle_get(parts: std::str::SplitWhitespace<'_>) -> Result<Command, BadRequestError> {
        let bad_usage = BadRequestError::BadUsage("GET file [revision]");

        // Make it peakable so we can check further
        let mut parts = parts.peekable();

        // If not even one part, then it's a bad usage
        let path = parts.next().ok_or(bad_usage.clone())?;

        if !is_valid_file_path(path) {
            return Err(BadRequestError::IllegalFile);
        }

        // Check if there's a revision and if it's valid
        let rev = parts.next();

        if parts.peek().is_some() {
            return Err(bad_usage);
        }

        Ok(Command::Get(path.to_string(), rev.map(|s| s.to_string())))
    }

    async fn handle_put(
        parts: std::str::SplitWhitespace<'_>,
        reader: &mut BufReader<ReadHalf<'_>>,
    ) -> Result<Command, BadRequestError> {
        let bad_usage = BadRequestError::BadUsage("PUT file length newline data");

        // Make it peakable so we can check further
        let mut parts = parts.peekable();

        // If not even one part, then it's a bad usage
        let path = parts.next().ok_or(bad_usage.clone())?;

        let length = parts.next().ok_or(bad_usage.clone())?;
        let length = length.parse::<usize>().unwrap_or_default();

        if parts.peek().is_some() {
            return Err(bad_usage);
        }

        if !is_valid_file_path(path) {
            return Err(BadRequestError::IllegalFile);
        }

        if length == 0 {
            return Ok(Command::Put(path.to_string(), "".to_string()));
        }

        // Read length from reader
        let mut buf = vec![0; length];
        reader.read_exact(&mut buf).await.unwrap();

        // Check if the data contains non-text data
        if contains_non_text_data(&buf) {
            return Err(BadRequestError::TextOnly);
        }

        Ok(Command::Put(
            path.to_string(),
            String::from_utf8(buf).expect("Already checked"),
        ))
    }

    async fn send_ready_and_flush(&mut self, writer: &mut BufWriter<WriteHalf<'_>>) {
        writer.write_all(b"READY\n").await.unwrap();
        writer.flush().await.unwrap();
    }
}

//
// Helper functions
//

fn is_valid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '.' || c == '/' || c == '_' || c == '-'
}

fn is_valid_dir_path(path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    if !path.starts_with('/') {
        return false;
    }
    path.chars().all(is_valid_char)
}

fn is_valid_file_path(path: &str) -> bool {
    is_valid_dir_path(path) && !path.ends_with('/')
}

fn contains_non_text_data(data: &[u8]) -> bool {
    data.iter()
        .any(|&byte| !byte.is_ascii_graphic() && !byte.is_ascii_whitespace())
}

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let socket_add = SocketAddr::from(([0, 0, 0, 0], 5000));
    let listener = TcpListener::bind(socket_add).await.unwrap();

    let mut server = Server::new();
    let tx = server.get_tx();

    tokio::spawn(async move {
        server.run().await;
    });

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        let tx = tx.clone();

        tokio::spawn(async move {
            let mut client = Client { tx };
            client.run(socket).await;
        });
    }
}

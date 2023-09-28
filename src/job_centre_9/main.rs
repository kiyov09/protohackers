use std::{
    collections::{BinaryHeap, HashMap, VecDeque},
    net::SocketAddr,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::mpsc::{self, channel},
};

use uuid::Uuid;

type QueueName = String;
type JobId = Uuid;
type ClientId = Uuid;

#[derive(Deserialize, Debug)]
struct PutRequest {
    pri: u32,
    queue: QueueName,
    job: serde_json::Value,
}

#[derive(Deserialize, Debug)]
struct GetRequest {
    queues: Vec<QueueName>,
    wait: Option<bool>,
}

#[derive(Deserialize, Debug)]
struct DeleteRequest {
    id: JobId,
}

#[derive(Deserialize, Debug)]
struct AbortRequest {
    id: JobId,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "request")]
enum Request {
    #[serde(rename = "put")]
    Put(PutRequest),
    #[serde(rename = "get")]
    Get(GetRequest),
    #[serde(rename = "delete")]
    Delete(DeleteRequest),
    #[serde(rename = "abort")]
    Abort(AbortRequest),
}

#[derive(Serialize, Debug)]
#[serde(untagged)]
enum OkResponse {
    Empty,
    #[serde(serialize_with = "OkResponse::serialize_job")]
    Job(Job),
    Id {
        id: JobId,
    },
}

impl OkResponse {
    fn serialize_job<S>(job: &Job, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        job.serialize(serializer)
    }
}

#[derive(Serialize, Debug)]
#[serde(tag = "status")]
enum Response {
    #[serde(rename = "ok", serialize_with = "Response::serialize_ok")]
    Ok(OkResponse),
    #[serde(rename = "error")]
    Error { error: String },
    #[serde(rename = "no-job")]
    NoJob,
}

impl Response {
    fn serialize_ok<S>(variant: &OkResponse, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        variant.serialize(serializer)
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
struct Job {
    id: JobId,
    queue: QueueName,
    job: serde_json::Value,
    #[serde(rename = "pri")]
    priority: u32,
}

impl Job {
    fn new(queue: QueueName, job: serde_json::Value, priority: u32) -> Self {
        Job {
            id: Uuid::new_v4(),
            queue,
            job,
            priority,
        }
    }
}

impl From<PutRequest> for Job {
    fn from(req: PutRequest) -> Self {
        Job::new(req.queue, req.job, req.pri)
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
enum CommandResult {
    Queued(JobId),
    Job(Job),
    NoJob,
    Ok,
    Invalid(String),
}

#[derive(Debug)]
enum Command {
    Register(mpsc::Sender<CommandResult>),
    Unregister,
    Put(Job),
    Get {
        queues: Vec<QueueName>,
        wait: Option<bool>,
    },
    Delete(JobId),
    Abort(JobId),
    // NOTE:
    // We need to ensure responses are sent in the same order as the requests
    // are received. This means any error we catch in the client need to be sent
    // to the server so it can be sent back to the client in the correct order.
    Error(String),
}

impl From<Request> for Command {
    fn from(value: Request) -> Self {
        match value {
            Request::Put(req) => Command::Put(req.into()),
            Request::Get(req) => Command::Get {
                queues: req.queues,
                wait: req.wait,
            },
            Request::Delete(req) => Command::Delete(req.id),
            Request::Abort(req) => Command::Abort(req.id),
        }
    }
}

#[derive(Debug)]
struct ClientCommand {
    id: ClientId,
    command: Command,
}

struct Server {
    tx: mpsc::Sender<ClientCommand>,
    rx: mpsc::Receiver<ClientCommand>,

    clients: HashMap<ClientId, mpsc::Sender<CommandResult>>,
    queues: HashMap<QueueName, BinaryHeap<Job>>,
    jobs: HashMap<JobId, QueueName>, // to find the queue of a job quickly
    being_worked_on: Vec<(Job, ClientId)>, // to find the client working on a job quickly
    waiting_clients: HashMap<QueueName, VecDeque<ClientId>>,
}

impl Server {
    fn new() -> Self {
        let (tx, rx) = channel::<ClientCommand>(10);

        Server {
            tx,
            rx,
            clients: HashMap::new(),
            queues: HashMap::new(),
            jobs: HashMap::new(),
            being_worked_on: Vec::new(),
            waiting_clients: HashMap::new(),
        }
    }

    fn get_tx(&self) -> mpsc::Sender<ClientCommand> {
        self.tx.clone()
    }

    async fn run(&mut self) {
        while let Some(ClientCommand { id, command }) = self.rx.recv().await {
            let result = match command {
                Command::Unregister => {
                    // If client is working on a job, abort it
                    if let Some((job, _)) = self
                        .being_worked_on
                        .iter()
                        .find(|(_, client_id)| client_id == &id)
                    {
                        let _ = self.abort(job.id).await;
                    }

                    // Remove the client
                    self.clients.remove(&id);

                    // Remove the client from any waiting queues
                    for (_, clients) in self.waiting_clients.iter_mut() {
                        clients.retain(|client_id| client_id != &id);
                    }

                    None
                }
                Command::Register(client_tx) => {
                    // Register the client
                    self.clients.insert(id, client_tx);
                    None
                }
                Command::Put(job) => {
                    // Add the job to the queue
                    Some(self.put(job).await)
                }
                Command::Get { queues, wait } => {
                    match self.get(queues.clone()).await {
                        Ok(job) => {
                            let client_tx = self.clients.get(&id);

                            if let Some(tx) = client_tx {
                                self.assign_to_client(id, job, tx.clone()).await;
                            }

                            None // assign_to_client already sends the response
                        }
                        Err(_) => match wait {
                            Some(w) if w => {
                                self.subscribe(queues, id);
                                None
                            }
                            _ => Some(CommandResult::NoJob),
                        },
                    }
                }
                Command::Delete(job_id) => {
                    // Delete the job
                    Some(self.delete(job_id).await)
                }
                Command::Abort(job_id) => {
                    // If Client is working on the job...
                    let res = if self
                        .being_worked_on
                        .iter()
                        .any(|(job, client_id)| job.id == job_id && client_id == &id)
                    {
                        // Abort the job
                        self.abort(job_id).await
                    } else {
                        // Otherwise, return an error
                        CommandResult::Invalid("Client not working on job".to_string())
                    };

                    Some(res)
                }
                Command::Error(msg) => Some(CommandResult::Invalid(msg)),
            };

            // Send response to client
            if let Some(cmd_result) = result {
                let tx = self
                    .clients
                    .get(&id)
                    .expect("Something is very bad if client is not registered");
                let _ = tx.send(cmd_result).await;
            }
        }
    }

    async fn put(&mut self, job: Job) -> CommandResult {
        // Only put the job in the queue if there's no client waiting
        let id = job.id;
        let queue = job.queue.clone();

        // Add the job to the jobs map
        self.jobs.entry(id).or_insert(queue.clone());

        // If there's a waiting client, send it to them
        if let Some(clients) = self.waiting_clients.get_mut(&queue) {
            let client_info = clients
                .pop_front()
                .map(|client_id| (client_id, self.clients.get(&client_id)));

            if let Some((client_id, tx)) = client_info {
                let tx = tx.expect("We know for sure the client has a sender here");

                self.assign_to_client(client_id, job.clone(), tx.clone())
                    .await;

                return CommandResult::Queued(id);
            }
        }

        // Add it to the queue
        self.queues
            .entry(queue.clone())
            .or_default()
            .push(job.clone());

        // Inform it was queued
        CommandResult::Queued(id)
    }

    async fn get(&mut self, queues: Vec<QueueName>) -> Result<Job, String> {
        // Get the most prioritized job from the queues
        let mut queue = String::new();
        let mut max_priority = 0;

        // Find the queue with the highest priority job
        for q in queues {
            if let Some(job) = self.queues.get_mut(&q).and_then(|q| q.peek()) {
                if job.priority > max_priority {
                    queue = q;
                    max_priority = job.priority;
                }
            }
        }

        // Remove the job from the queue
        self.queues
            .get_mut(&queue)
            .and_then(|q| q.pop())
            .ok_or_else(|| "No job found".to_string())
    }

    async fn delete(&mut self, id: JobId) -> CommandResult {
        // Remove the jobs from the set of ones being worked on
        self.being_worked_on.retain(|(job, _)| job.id != id);

        // And remove it from the queue it's in too
        // If the job is not in the jobs map, it could means:
        // - Never existed
        // - Already deleted
        //
        // So we return an error
        self.jobs
            .remove(&id)
            .and_then(|queue| self.queues.get_mut(&queue))
            .map_or_else(
                || CommandResult::NoJob,
                |queue| {
                    // At this point, we know the job exists, so we can remove it from the queue
                    queue.retain(|job| job.id != id);
                    CommandResult::Ok
                },
            )
    }

    async fn abort(&mut self, id: JobId) -> CommandResult {
        // Get the position of the job in the being_worked_on vec
        // (if it's not there, return an error)
        let job_pos = self
            .being_worked_on
            .iter()
            .position(|(job, _)| job.id == id);

        if job_pos.is_none() {
            return CommandResult::NoJob;
        }
        let job_pos = job_pos.expect("Already checked that job_pos is not None");

        // Remove the job from the being_worked_on vec
        let (job, _) = self.being_worked_on.swap_remove(job_pos);

        // ... and put it back in the queue
        self.put(job).await;

        // Indicate success
        CommandResult::Ok
    }

    fn subscribe(&mut self, queues: Vec<QueueName>, client_id: ClientId) {
        // Subscribe the client to jobs being put in the queues
        for q in queues {
            self.waiting_clients
                .entry(q)
                .or_default()
                .push_back(client_id);
        }
    }

    async fn assign_to_client(
        &mut self,
        client_id: ClientId,
        job: Job,
        client_tx: mpsc::Sender<CommandResult>,
    ) {
        self.being_worked_on.push((job.clone(), client_id));
        let _ = client_tx.send(CommandResult::Job(job.clone())).await;
    }
}

struct Client {
    id: ClientId,
    tx: mpsc::Sender<ClientCommand>,
}

impl Client {
    fn new(tx: mpsc::Sender<ClientCommand>) -> Self {
        Client {
            id: Uuid::new_v4(),
            tx,
        }
    }

    async fn run(&mut self, socket: TcpStream) {
        socket.readable().await.expect("socket is not readable");

        let (reader, writer) = socket.into_split();
        let (result_tx, rx) = mpsc::channel::<CommandResult>(100);

        // Register the client with the server
        let _ = self
            .send_command(Command::Register(result_tx.clone()))
            .await;

        // Spawn task to receive messages from the server
        self.spawn_receive_task(rx, writer).await;

        let mut reader = BufReader::new(reader);
        let mut buf = String::new();

        while let Ok(bytes_read) = reader.read_line(&mut buf).await {
            if bytes_read == 0 {
                break;
            }
            self.handle_request(&buf).await;
            buf.clear();
        }

        // Unregister the client from the server
        let _ = self.send_command(Command::Unregister).await;
    }

    async fn handle_request(&mut self, raw_request: &str) {
        println!("--> Client {} send request: {}", self.id, raw_request);

        let cmd = serde_json::from_str::<Request>(raw_request)
            .map(|req| req.into())
            .unwrap_or_else(|_e| Command::Error("Invalid request".to_string()));

        self.send_command(cmd)
            .await
            .expect("Failed to send result to server");
    }

    async fn spawn_receive_task(
        &self,
        mut rx: mpsc::Receiver<CommandResult>,
        writer: OwnedWriteHalf,
    ) {
        // Handle responses from the server
        tokio::spawn(async move {
            let mut writer = BufWriter::new(writer);

            while let Some(res) = rx.recv().await {
                let res = match res {
                    CommandResult::Ok => Response::Ok(OkResponse::Empty),
                    CommandResult::Queued(id) => Response::Ok(OkResponse::Id { id }),
                    CommandResult::Job(job) => Response::Ok(OkResponse::Job(job)),
                    CommandResult::NoJob => Response::NoJob,
                    CommandResult::Invalid(error) => Response::Error { error },
                };
                let res = serde_json::to_string(&res).expect("Uups, my bad!");

                writer
                    .write_all((res + "\n").as_bytes())
                    .await
                    .expect("Failed to write to socket");

                writer.flush().await.expect("Cannot flush socket");
            }
        });
    }

    async fn send_command(
        &mut self,
        command: Command,
    ) -> Result<(), mpsc::error::SendError<ClientCommand>> {
        self.tx
            .send(ClientCommand {
                id: self.id,
                command,
            })
            .await
    }
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
            let mut client = Client::new(tx);
            client.run(socket).await;
        });
    }
}

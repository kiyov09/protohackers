use std::{
    collections::{BinaryHeap, HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Sender},
        Mutex,
    },
};

use uuid::Uuid;

type QueueName = String;
type JobId = Uuid;

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

struct Server {
    queues: HashMap<QueueName, BinaryHeap<Job>>,
    jobs: HashMap<JobId, QueueName>, // Map a job_id to the name of the queue it's in
    being_worked_on: Vec<Job>,
    waiting_clients: HashMap<QueueName, VecDeque<Sender<Job>>>,
}

impl Server {
    fn new() -> Self {
        Server {
            queues: HashMap::new(),
            jobs: HashMap::new(),
            being_worked_on: Vec::new(),
            waiting_clients: HashMap::new(),
        }
    }

    async fn put(&mut self, job: Job) -> JobId {
        // Only put the job in the queue if there's no client waiting

        let id = job.id;
        let queue = job.queue.clone();

        // Add the job to the jobs map
        self.jobs.entry(id).or_insert(queue.clone());

        // If there's a waiting client, send it to them
        if let Some(clients) = self.waiting_clients.get_mut(&queue) {
            // TODO: This code it wrong, Why?
            // - The client could have been disconnected
            // - The client could have been subscribed to multiple queues
            //   and had already received a job from another queue
            if let Some(client) = clients.pop_front() {
                let _ = client.send(job).await;
                return id;
            }
        }

        // Add it to the queue
        self.queues
            .entry(queue.clone())
            .or_default()
            .push(job.clone());

        // Return the id
        id
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
        let job = self.queues.get_mut(&queue).and_then(|q| q.pop());

        // If there's a job, mark it as being worked on,
        // and return it
        if let Some(job) = job {
            // Add it to the being_worked_on set
            self.being_worked_on.push(job.clone());
            // Return the job
            Ok(job)
        } else {
            // Otherwise, return an error
            Err("No job found".to_string())
        }
    }

    fn subscribe(&mut self, queues: Vec<QueueName>, tx: Sender<Job>) {
        // Subscribe the client to jobs being put in the queues
        for q in queues {
            self.waiting_clients
                .entry(q)
                .or_default()
                .push_back(tx.clone());
        }
    }

    async fn delete(&mut self, id: JobId) -> Result<(), String> {
        // Remove the jobs from the set of ones being worked on
        self.being_worked_on.retain(|job| job.id != id);

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
                || Err("Job not found".to_string()),
                |queue| {
                    // At this point, we know the job exists, so we can remove it from the queue
                    queue.retain(|job| job.id != id);
                    Ok(())
                },
            )
    }

    async fn abort(&mut self, id: JobId) -> Result<(), String> {
        // Get the position of the job in the being_worked_on vec
        // (if it's not there, return an error)
        let job_pos = self
            .being_worked_on
            .iter()
            .position(|job| job.id == id)
            .ok_or_else(|| "Job not found".to_string())?;

        // Remove the job from the being_worked_on vec
        let job = self.being_worked_on.swap_remove(job_pos);
        // ... and put it back in the queue
        self.put(job).await;

        // Indicate success
        Ok(())
    }
}

struct Client {
    working_on: Option<JobId>,
    server: Arc<Mutex<Server>>,
}

impl Client {
    fn new(server_ref: Arc<Mutex<Server>>) -> Self {
        Client {
            working_on: None,
            server: server_ref,
        }
    }

    async fn run(&mut self, mut socket: TcpStream) {
        socket.readable().await.expect("socket is not readable");

        let (reader, writer) = socket.split();

        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        let mut buf = String::new();

        while let Ok(bytes_read) = reader.read_line(&mut buf).await {
            if bytes_read == 0 {
                break;
            }

            match serde_json::from_str::<Request>(&buf) {
                Ok(req) => {
                    let res = self.handle_request(req).await;
                    let res = serde_json::to_string(&res).expect("Uups, my bad!");

                    writer
                        .write_all((res + "\n").as_bytes())
                        .await
                        .expect("Failed to write to socket");

                    writer.flush().await.expect("Cannot flush socket");
                }
                Err(_e) => {
                    let res = Response::Error {
                        error: "Invalid request".to_string(),
                    };
                    let res = serde_json::to_string(&res).expect("Uups, my bad!");

                    writer
                        .write_all((res + "\n").as_bytes())
                        .await
                        .expect("Failed to write to socket");

                    writer.flush().await.expect("Cannot flush socket");
                }
            }

            buf.clear();
        }

        // Check if we were working on a job
        // and if so, abort it without sending a response
        if let Some(id) = self.working_on {
            self.abort(AbortRequest { id }).await;
        }
    }

    async fn handle_request(&mut self, req: Request) -> Response {
        match req {
            Request::Put(req) => self.put(req).await,
            Request::Get(req) => self.get(req).await,
            Request::Delete(req) => self.delete(req).await,
            Request::Abort(req) => self.abort(req).await,
        }
    }

    async fn put(&self, req: PutRequest) -> Response {
        let id = self.server.lock().await.put(req.into()).await;
        Response::Ok(OkResponse::Id { id })
    }

    async fn get(&mut self, req: GetRequest) -> Response {
        let job = self.server.lock().await.get(req.queues.clone()).await;

        if let Ok(job) = job {
            self.working_on = Some(job.id);
            Response::Ok(OkResponse::Job(job))
        } else {
            match req.wait {
                Some(w) if w => {
                    let (tx, mut rx) = channel(10);

                    self.server.lock().await.subscribe(req.queues.clone(), tx);

                    match rx.recv().await {
                        Some(j) => {
                            self.working_on = Some(j.id);
                            Response::Ok(OkResponse::Job(j))
                        }
                        None => Response::NoJob,
                    }
                }
                _ => Response::NoJob,
            }
        }
    }

    async fn delete(&self, req: DeleteRequest) -> Response {
        self.server
            .lock()
            .await
            .delete(req.id)
            .await
            .map_or_else(|_| Response::NoJob, |_| Response::Ok(OkResponse::Empty))
    }

    async fn abort(&mut self, req: AbortRequest) -> Response {
        // Only the client working on a job can abort it, so

        // ... no job to abort, error
        if self.working_on.is_none() {
            return Response::Error {
                error: "No job to abort".to_string(),
            };
        }

        if let Some(id) = self.working_on {
            // ... the job to abort is not the one the client is working on, error
            if id != req.id {
                return Response::Error {
                    error: "Can't abort a job that you're not working on".to_string(),
                };
            }
        }

        // Take it out of the client's working_on field to leave a None in place
        let job = self.working_on.take().unwrap();

        // Use the sender end of the channel to send the request to the server
        self.server
            .lock()
            .await
            .abort(job)
            .await
            .map_or_else(|_| Response::NoJob, |_| Response::Ok(OkResponse::Empty))
    }
}

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let socket_add = SocketAddr::from(([0, 0, 0, 0], 5000));
    let listener = TcpListener::bind(socket_add).await.unwrap();

    let server = Arc::new(Mutex::new(Server::new()));

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        let server = server.clone();

        tokio::spawn(async move {
            let mut client = Client::new(server);
            client.run(socket).await;
        });
    }
}

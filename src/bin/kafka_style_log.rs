use anyhow::Context;
use distributed_system_challenges::{
    main_loop,
    writters::{MessageWritter, StdoutJsonWritter},
    Body, Message, Node,
};
use redis::{Commands, Connection};
use serde::{Deserialize, Serialize, Serializer};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Send {
        key: KeyId,
        msg: usize,
    },
    SendOk {
        offset: Offset,
    },
    Poll {
        offsets: HashMap<KeyId, Offset>,
    },
    PollOk {
        #[serde(serialize_with = "serialize_as_pairs")]
        msgs: HashMap<KeyId, HashMap<Offset, usize>>,
    },
    CommitOffsets {
        offsets: HashMap<KeyId, Offset>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: HashSet<KeyId>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<KeyId, Offset>,
    },
    InternalSend {
        log_entry: LogEntry,
    },
    InternalCommitOffsets {
        offsets: HashMap<KeyId, Offset>,
    },
}

type NodeId = String;
type KeyId = String;
type Offset = usize;
type Logs = HashMap<KeyId, HashSet<LogEntry>>;

// TODO: we don't need to duplicate the whole structure, we could
// just keep messages ids seen by other nodes which will reduce
// size of the state at the cost of increasing some complexity
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SeenLogs {
    logs: Logs,
    offsets: HashMap<KeyId, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    msg_id: usize,
    key: KeyId,
    offset: Offset,
    msg: usize,
    seen_by: HashSet<NodeId>,
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.offset == other.offset
    }
}

impl Eq for LogEntry {}

impl Hash for LogEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.offset.hash(state);
    }
}

struct LogStore {
    src: String,
    logs: Logs,
    offsets: HashMap<KeyId, usize>,
}

impl LogStore {
    fn new(src: &str) -> Self {
        Self {
            src: src.to_owned(),
            logs: Default::default(),
            offsets: Default::default(),
        }
    }

    fn init(&mut self, scr: &str) {
        self.src = scr.to_owned();
    }

    fn insert(&mut self, log_entry: LogEntry) -> anyhow::Result<()> {
        self.logs
            .entry(log_entry.key.to_owned())
            .and_modify(|entries| {
                entries.insert(log_entry.clone());
            })
            .or_insert(HashSet::from([log_entry]));

        Ok(())
    }

    fn append(
        &mut self,
        key: &str,
        msg_id: usize,
        offset: usize,
        msg: usize,
    ) -> anyhow::Result<LogEntry> {
        let seen_by = HashSet::from([self.src.to_owned()]);

        let log_entry = LogEntry {
            msg_id,
            key: key.to_owned(),
            offset,
            msg,
            seen_by,
        };

        self.logs
            .entry(key.to_owned())
            .or_default()
            .insert(log_entry.clone());

        Ok(log_entry)
    }

    fn commit(&mut self, offsets: &HashMap<String, usize>) -> anyhow::Result<()> {
        for (key, offset) in offsets {
            let Some(committed) = self.offsets.get_mut(key) else {
                continue;
            };

            *committed = *offset;
        }

        Ok(())
    }

    fn list_logs(&self, keys: &HashMap<KeyId, usize>) -> anyhow::Result<Logs> {
        let mut committed_logs = HashMap::new();

        for (key, offset) in keys {
            let Some(entries) = self.logs.get(key) else {
                continue;
            };

            let logs = entries
                .iter()
                .filter(|entry| entry.offset >= *offset)
                .cloned()
                .collect::<HashSet<_>>();

            committed_logs.insert(key.clone(), logs);
        }

        Ok(committed_logs)
    }

    fn list_committed_offsets(
        &self,
        keys: &HashSet<KeyId>,
    ) -> anyhow::Result<HashMap<String, usize>> {
        let mut committed_offsets = HashMap::default();

        for key in keys {
            let Some(offset) = self.offsets.get(key) else {
                continue;
            };

            committed_offsets.insert(key.clone(), *offset);
        }

        Ok(committed_offsets)
    }
}

struct KafkaStyleLogNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: NodeId,
    message_id: usize,
    cluster: HashSet<NodeId>,
    neighbors: HashSet<NodeId>,
    known: Arc<Mutex<HashMap<NodeId, SeenLogs>>>,
    connection: Arc<Mutex<Connection>>,
    log_store: Arc<Mutex<LogStore>>,
}

impl<'a> KafkaStyleLogNode<'a> {
    fn new(
        writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
        connection: Arc<Mutex<Connection>>,
    ) -> Self {
        let node_id = "uninit";
        Self {
            node_id: node_id.to_owned(),
            message_id: 0,
            cluster: HashSet::new(),
            neighbors: HashSet::new(),
            known: Arc::new(Mutex::new(HashMap::new())),
            writter,
            connection,
            log_store: Arc::new(Mutex::new(LogStore::new(node_id))),
        }
    }

    fn send_message(&mut self, message: &Message<Payload>) -> anyhow::Result<()> {
        self.writter.send_message(message)?;
        self.message_id += 1;

        Ok(())
    }

    fn send_messages(&mut self, messages: &[Message<Payload>]) -> anyhow::Result<()> {
        self.writter.send_messages(messages)?;
        self.message_id += 1;

        Ok(())
    }

    fn handle_init(
        &mut self,
        message: &Message<Payload>,
        node_id: &str,
        node_ids: &[String],
    ) -> anyhow::Result<()> {
        self.node_id = node_id.to_owned();

        let nodes = node_ids
            .iter()
            .filter(|n| *n != node_id)
            .map(|n| n.to_owned())
            .collect::<HashSet<_>>();
        let mut cluster = nodes.clone();
        cluster.insert(node_id.to_owned());

        let known = nodes
            .iter()
            .map(|id| (id.to_owned(), SeenLogs::default()))
            .collect::<Vec<_>>();

        self.neighbors = nodes;
        self.cluster = cluster;
        self.log_store.lock().unwrap().init(node_id);
        self.known.lock().unwrap().extend(known);

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(&reply)
    }

    fn handle_send(
        &mut self,
        message: &Message<Payload>,
        key: &str,
        msg: usize,
    ) -> anyhow::Result<()> {
        let offset = self
            .connection
            .lock()
            .unwrap()
            .incr(format!("{key}::offset"), 1)?;

        let log_entry = self
            .log_store
            .lock()
            .unwrap()
            .append(key, self.message_id, offset, msg)?;

        self.broadcast_send(&log_entry)?;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::SendOk { offset },
            ),
        );

        self.send_message(&reply)
    }

    fn handle_poll(
        &mut self,
        message: &Message<Payload>,
        offsets: HashMap<String, usize>,
    ) -> anyhow::Result<()> {
        let committed_logs = self.log_store.lock().unwrap().list_logs(&offsets)?;

        let msgs = committed_logs
            .iter()
            .map(|(key, entries)| {
                let offsets = entries
                    .iter()
                    .map(|entry| (entry.offset, entry.msg))
                    .collect::<HashMap<_, _>>();

                (key.to_owned(), offsets)
            })
            .collect::<HashMap<_, _>>();

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::PollOk { msgs },
            ),
        );

        self.send_message(&reply)
    }

    fn handle_commit_offsets(
        &mut self,
        message: &Message<Payload>,
        offsets: HashMap<String, usize>,
    ) -> anyhow::Result<()> {
        self.log_store.lock().unwrap().commit(&offsets)?;

        self.broadcast_commit_offsets(&offsets)?;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::CommitOffsetsOk,
            ),
        );

        self.send_message(&reply)
    }

    fn handle_list_committed_offsets(
        &mut self,
        message: &Message<Payload>,
        keys: &HashSet<KeyId>,
    ) -> anyhow::Result<()> {
        let offsets = self
            .log_store
            .lock()
            .unwrap()
            .list_committed_offsets(keys)?;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::ListCommittedOffsetsOk { offsets },
            ),
        );

        self.send_message(&reply)
    }

    fn handle_internal_send(&mut self, log_entry: &LogEntry) -> anyhow::Result<()> {
        self.log_store.lock().unwrap().insert(log_entry.clone())
    }

    fn handle_internal_commit_offsets(
        &mut self,
        offsets: &HashMap<String, usize>,
    ) -> anyhow::Result<()> {
        self.log_store.lock().unwrap().commit(offsets)
    }

    fn broadcast_send(&mut self, log_entry: &LogEntry) -> anyhow::Result<()> {
        let internal_send_messages = self
            .neighbors
            .iter()
            .map(|n| {
                Message::new(
                    self.node_id.to_owned(),
                    n.to_owned(),
                    Body::new(
                        Some(self.message_id),
                        None,
                        Payload::InternalSend {
                            log_entry: LogEntry {
                                seen_by: self.cluster.clone(),
                                ..log_entry.clone()
                            },
                        },
                    ),
                )
            })
            .collect::<Vec<_>>();
        self.send_messages(&internal_send_messages)
    }

    fn broadcast_commit_offsets(&mut self, offsets: &HashMap<String, usize>) -> anyhow::Result<()> {
        let internal_commit_offsets_messages = self
            .neighbors
            .iter()
            .map(|n| {
                Message::new(
                    self.node_id.to_owned(),
                    n.to_owned(),
                    Body::new(
                        Some(self.message_id),
                        None,
                        Payload::InternalCommitOffsets {
                            offsets: offsets.clone(),
                        },
                    ),
                )
            })
            .collect::<Vec<_>>();
        self.send_messages(&internal_commit_offsets_messages)
    }
}

impl Node<Payload> for KafkaStyleLogNode<'_> {
    fn init(&mut self, _tx: std::sync::mpsc::Sender<Message<Payload>>) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        match &message.body().payload {
            Payload::Init { node_id, node_ids } => self.handle_init(&message, node_id, node_ids)?,
            Payload::InitOk => {}
            Payload::Send { key, msg } => self.handle_send(&message, key, *msg)?,
            Payload::SendOk { .. } => {}
            Payload::Poll { offsets } => self.handle_poll(&message, offsets.clone())?,
            Payload::PollOk { .. } => {}
            Payload::CommitOffsets { offsets } => {
                self.handle_commit_offsets(&message, offsets.clone())?
            }
            Payload::CommitOffsetsOk => {}
            Payload::ListCommittedOffsets { keys } => {
                self.handle_list_committed_offsets(&message, keys)?
            }
            Payload::ListCommittedOffsetsOk { .. } => {}
            Payload::InternalSend { log_entry } => self.handle_internal_send(log_entry)?,
            Payload::InternalCommitOffsets { offsets } => {
                self.handle_internal_commit_offsets(offsets)?
            }
        };

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let redis_client =
        redis::Client::open("redis://localhost/").context("Error connecting to Redis server")?;

    let connection = redis_client.get_connection()?;
    let connection = Arc::new(Mutex::new(connection));

    let stdout = std::io::stdout().lock();
    let mut stdout_json_writter: Box<dyn MessageWritter<Message<Payload>>> =
        Box::new(StdoutJsonWritter::new(stdout));

    let mut node = KafkaStyleLogNode::new(&mut stdout_json_writter, connection);
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

fn serialize_as_pairs<S>(
    msgs: &HashMap<String, HashMap<Offset, usize>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let result = msgs
        .iter()
        .map(|(key, entries)| {
            let mut pairs = entries
                .iter()
                .map(|(offset, message)| (*offset, *message))
                .collect::<Vec<_>>();

            pairs.sort_by_key(|(k, _)| *k);

            let pairs = pairs.iter().map(|(k, v)| vec![*k, *v]).collect();

            (key.to_owned(), pairs)
        })
        .collect::<HashMap<String, Vec<Vec<_>>>>();

    let json = serde_json::value::to_value(&result).expect("Error deserializing HashMap");

    json.serialize(serializer)
}

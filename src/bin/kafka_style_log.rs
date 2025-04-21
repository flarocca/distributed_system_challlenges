use anyhow::Context;
use distributed_system_challenges::{main_loop, Body, Message, Node};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{StdoutLock, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
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
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<usize>>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    TriggerGossip,
    Gossip {
        seen: SeenLogs,
    },
}

type Logs = HashMap<String, Vec<LogEntry>>;

// TODO: we don't need to duplicate the whole structure, we could
// just keep messages ids seen by other nodes which will reduce
// size of the state at the cost of increasing some complexity
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SeenLogs {
    uncommitted: Logs,
    committed: Logs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    msg_id: usize,
    key: String,
    offset: usize,
    msg: usize,
}

struct KafkaStyleLogNode {
    node_id: String,
    message_id: usize,
    neighbors: Vec<String>,
    known: HashMap<String, SeenLogs>,
    // Node specific state
    offset: AtomicUsize,
    uncommitted_logs: Arc<Mutex<Logs>>,
    committed_logs: Arc<Mutex<Logs>>,
}

impl KafkaStyleLogNode {
    fn new() -> Self {
        Self {
            node_id: "uninit".to_owned(),
            message_id: 0,
            neighbors: Vec::new(),
            known: HashMap::new(),
            offset: AtomicUsize::default(),
            uncommitted_logs: Arc::new(Mutex::new(HashMap::new())),
            committed_logs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn send_message<T>(&mut self, stdout: &mut StdoutLock, message: &T) -> anyhow::Result<()>
    where
        T: ?Sized + Serialize,
    {
        serde_json::to_writer(&mut *stdout, message).context("Error serializing response")?;
        stdout
            .write_all(b"\n")
            .context("Error writing response to stdout")?;

        self.message_id += 1;

        Ok(())
    }

    fn send_messages<T>(&mut self, stdout: &mut StdoutLock, messages: &Vec<T>) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        for message in messages {
            serde_json::to_writer(&mut *stdout, message).context("Error serializing response")?;
            stdout
                .write_all(b"\n")
                .context("Error writing response to stdout")?;
        }

        self.message_id += 1;

        Ok(())
    }

    fn handle_init(
        &mut self,
        message: &Message<Payload>,
        node_id: &str,
        node_ids: &[String],
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        self.node_id = node_id.to_owned();

        let nodes = node_ids
            .iter()
            .filter(|n| *n != node_id)
            .map(|n| n.to_owned())
            .collect::<Vec<_>>();

        self.known
            .extend(nodes.iter().map(|id| (id.clone(), SeenLogs::default())));
        self.neighbors = nodes;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_send(
        &mut self,
        message: &Message<Payload>,
        key: &str,
        msg: usize,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        // TODO: handle the case for when the key/msg already exists, in which case
        // it should return the actual offset
        let cloned = self.uncommitted_logs.clone();
        let mut uncommitted_logs = cloned.lock().unwrap();
        let offset = self.offset.load(Ordering::Relaxed);

        let log_entry = LogEntry {
            msg_id: self.message_id,
            key: key.to_owned(),
            offset,
            msg,
        };

        uncommitted_logs
            .entry(key.to_owned())
            .or_default()
            .push(log_entry);

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::SendOk { offset },
            ),
        );

        self.offset.fetch_add(1, Ordering::Relaxed);

        self.send_message(stdout, &reply)
    }

    fn handle_poll(
        &mut self,
        message: &Message<Payload>,
        offsets: HashMap<String, usize>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        // TODO: non existing keys are ignored. This might not be the best
        // alternative. Maybe an empty vector should be returned instead
        let committed_cloned = self.committed_logs.clone();
        let uncommitted_cloned = self.uncommitted_logs.clone();

        let committed_logs = committed_cloned.lock().unwrap();
        let uncommitted_logs = uncommitted_cloned.lock().unwrap();

        let mut msgs = HashMap::new();

        for (key, offset) in &offsets {
            let mut log_entries = Vec::new();

            if committed_logs.contains_key(key) {
                let log = committed_logs.get(key).expect("Key not found");

                let logs = log
                    .iter()
                    .filter(|l| l.offset >= *offset)
                    .map(|l| vec![l.offset, l.msg])
                    .collect::<Vec<_>>();

                log_entries.extend(logs.clone());
            }

            if uncommitted_logs.contains_key(key) {
                let log = uncommitted_logs.get(key).expect("Key not found");

                let logs = log
                    .iter()
                    .filter(|l| l.offset >= *offset)
                    .map(|l| vec![l.offset, l.msg])
                    .collect::<Vec<_>>();

                log_entries.extend(logs.clone());
            }

            msgs.insert(key.clone(), log_entries.clone());
        }

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::PollOk { msgs },
            ),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_commit_offsets(
        &mut self,
        message: &Message<Payload>,
        offsets: HashMap<String, usize>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let committed_cloned = self.committed_logs.clone();
        let uncommitted_cloned = self.uncommitted_logs.clone();

        let mut committed_logs = committed_cloned.lock().unwrap();
        let mut uncommitted_logs = uncommitted_cloned.lock().unwrap();

        for request in offsets {
            let Some(entries) = uncommitted_logs.get_mut(&request.0) else {
                continue;
            };

            let committed = entries
                .iter()
                .filter(|l| l.offset <= request.1)
                .cloned()
                .collect::<Vec<_>>();

            entries.retain(|l| l.offset > request.1);
            committed_logs
                .entry(request.0.clone())
                .or_default()
                .extend(committed.clone());
        }

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::CommitOffsetsOk,
            ),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_list_committed_offsets(
        &mut self,
        message: &Message<Payload>,
        keys: &Vec<String>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let committed_cloned = self.committed_logs.clone();

        let committed_logs = committed_cloned.lock().unwrap();

        let mut offsets = HashMap::new();

        for key in keys {
            let Some(entries) = committed_logs.get(key) else {
                continue;
            };

            offsets.insert(key.clone(), entries.len());
        }

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::ListCommittedOffsetsOk { offsets },
            ),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_gossip(&mut self, src: &str, seen: SeenLogs) {
        let node = self.known.get_mut(src).expect("Unknown node");

        let committed_cloned = self.committed_logs.clone();
        let uncommitted_cloned = self.uncommitted_logs.clone();

        let mut committed_logs = committed_cloned.lock().unwrap();
        let mut uncommitted_logs = uncommitted_cloned.lock().unwrap();

        node.uncommitted.extend(seen.uncommitted.clone());
        node.committed.extend(seen.committed.clone());

        for (key, entries) in &seen.committed {
            for entry in entries {
                committed_logs
                    .entry(key.clone())
                    .or_default()
                    .push(entry.clone());
            }
        }

        for (key, entries) in &seen.uncommitted {
            for entry in entries {
                uncommitted_logs
                    .entry(key.clone())
                    .or_default()
                    .push(entry.clone());
            }
        }
    }

    fn handle_trigger_gossip(&mut self, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        if self.neighbors.is_empty() {
            return Ok(());
        }

        let committed_cloned = self.committed_logs.clone();
        let uncommitted_cloned = self.uncommitted_logs.clone();

        let committed_logs = committed_cloned.lock().unwrap();
        let uncommitted_logs = uncommitted_cloned.lock().unwrap();

        let messages = self
            .neighbors
            .iter()
            .map(|n| {
                let mut n_not_seen = SeenLogs::default();

                for (key, value) in committed_logs.iter() {
                    for entry in value {
                        if !self
                            .known
                            .get(n)
                            .expect("Unknown node")
                            .committed
                            .contains_key(key)
                        {
                            n_not_seen
                                .committed
                                .entry(key.clone())
                                .or_default()
                                .push(entry.clone());
                        }
                    }
                }

                for (key, value) in uncommitted_logs.iter() {
                    for entry in value {
                        if !self
                            .known
                            .get(n)
                            .expect("Unknown node")
                            .uncommitted
                            .contains_key(key)
                        {
                            n_not_seen
                                .uncommitted
                                .entry(key.clone())
                                .or_default()
                                .push(entry.clone());
                        }
                    }
                }

                Message::new(
                    self.node_id.to_owned(),
                    n.to_owned(),
                    Body::new(
                        Some(self.message_id),
                        None,
                        Payload::Gossip { seen: n_not_seen },
                    ),
                )
            })
            .collect::<Vec<_>>();

        self.send_messages(stdout, &messages)
    }
}

impl Node<Payload> for KafkaStyleLogNode {
    fn init(&mut self, tx: std::sync::mpsc::Sender<Message<Payload>>) -> anyhow::Result<()> {
        let node_id = self.node_id.clone();
        let _ = std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(300));

            let trigger_gossip = Message::<Payload>::new(
                node_id.clone(),
                node_id.clone(),
                Body::new(None, None, Payload::TriggerGossip),
            );

            if tx.send(trigger_gossip).is_err() {
                break;
            }
        });

        Ok(())
    }

    fn handle_message(
        &mut self,
        message: Message<Payload>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        // TODO: check iof message has been seen already
        match &message.body().payload {
            Payload::Init { node_id, node_ids } => {
                self.handle_init(&message, node_id, node_ids, stdout)?
            }
            Payload::InitOk => {}
            Payload::Send { key, msg } => self.handle_send(&message, key, *msg, stdout)?,
            Payload::SendOk { .. } => {}
            Payload::Poll { offsets } => self.handle_poll(&message, offsets.clone(), stdout)?,
            Payload::PollOk { .. } => {}
            Payload::CommitOffsets { offsets } => {
                self.handle_commit_offsets(&message, offsets.clone(), stdout)?
            }
            Payload::CommitOffsetsOk => {}
            Payload::ListCommittedOffsets { keys } => {
                self.handle_list_committed_offsets(&message, keys, stdout)?
            }
            Payload::ListCommittedOffsetsOk { .. } => {}
            Payload::TriggerGossip => self.handle_trigger_gossip(stdout)?,
            Payload::Gossip { seen } => self.handle_gossip(message.src(), seen.clone()),
        };

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = KafkaStyleLogNode::new();
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

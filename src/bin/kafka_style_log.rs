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
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
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
        offset: Offset,
    },
    Poll {
        offsets: HashMap<String, Offset>,
    },
    PollOk {
        #[serde(serialize_with = "serialize_as_pairs")]
        msgs: HashMap<String, HashMap<Offset, usize>>,
    },
    CommitOffsets {
        offsets: HashMap<String, Offset>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, Offset>,
    },
    TriggerGossip,
    Gossip {
        seen: SeenLogs,
    },
    Anchor,
}

type NodeId = String;
type KeyId = String;
type Offset = usize;
type Logs = HashMap<KeyId, Vec<LogEntry>>;

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
    key: KeyId,
    offset: Offset,
    msg: usize,
    seen_by: HashSet<NodeId>,
}

struct KafkaStyleLogNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: NodeId,
    message_id: usize,
    neighbors: HashSet<NodeId>,
    known: HashMap<NodeId, SeenLogs>,
    connection: Arc<Mutex<Connection>>,
    gossip_interval: Duration,
    gossip_batch_size: usize,
    // Node specific state
    uncommitted_logs: Arc<Mutex<Logs>>,
    committed_logs: Arc<Mutex<Logs>>,
    anchored_uncommitted_logs: Arc<Mutex<Logs>>,
    anchored_committed_logs: Arc<Mutex<Logs>>,
}

impl<'a> KafkaStyleLogNode<'a> {
    fn new(
        writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
        connection: Arc<Mutex<Connection>>,
    ) -> Self {
        Self {
            node_id: "uninit".to_owned(),
            message_id: 0,
            neighbors: HashSet::new(),
            known: HashMap::new(),
            writter,
            connection,
            gossip_interval: Duration::from_millis(300),
            gossip_batch_size: 100,
            uncommitted_logs: Arc::new(Mutex::new(HashMap::new())),
            committed_logs: Arc::new(Mutex::new(HashMap::new())),
            anchored_uncommitted_logs: Arc::new(Mutex::new(HashMap::new())),
            anchored_committed_logs: Arc::new(Mutex::new(HashMap::new())),
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

        self.known
            .extend(nodes.iter().map(|id| (id.clone(), SeenLogs::default())));
        self.neighbors = nodes;

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
        // TODO: handle the case for when the key/msg already exists, in which case
        // it should return the actual offset
        let cloned = self.uncommitted_logs.clone();
        let mut uncommitted_logs = cloned.lock().unwrap();

        let offset = self
            .connection
            .lock()
            .unwrap()
            .incr(format!("{key}::offset"), 1)?;

        let mut seen_by = HashSet::new();
        seen_by.insert(self.node_id.to_owned());
        let log_entry = LogEntry {
            msg_id: self.message_id,
            key: key.to_owned(),
            offset,
            msg,
            seen_by,
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

        self.send_message(&reply)
    }

    fn handle_poll(
        &mut self,
        message: &Message<Payload>,
        offsets: HashMap<String, usize>,
    ) -> anyhow::Result<()> {
        // TODO: non existing keys are ignored. This might not be the best
        // alternative. Maybe an empty vector should be returned instead
        let committed_cloned = self.anchored_committed_logs.clone();
        let uncommitted_cloned = self.anchored_uncommitted_logs.clone();

        let committed_logs = committed_cloned.lock().unwrap();
        let uncommitted_logs = uncommitted_cloned.lock().unwrap();

        let mut msgs = HashMap::new();

        for (key, offset) in &offsets {
            let mut log_entries = HashMap::new();

            if committed_logs.contains_key(key) {
                let log = committed_logs.get(key).expect("Key not found");

                let logs = log
                    .iter()
                    .filter(|l| l.offset >= *offset)
                    .map(|l| (l.offset, l.msg))
                    .collect::<HashMap<_, _>>();

                log_entries.extend(logs.clone());
            }

            if uncommitted_logs.contains_key(key) {
                let log = uncommitted_logs.get(key).expect("Key not found");

                let logs = log
                    .iter()
                    .filter(|l| l.offset >= *offset)
                    .map(|l| (l.offset, l.msg))
                    .collect::<HashMap<_, _>>();

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

        self.send_message(&reply)
    }

    fn handle_commit_offsets(
        &mut self,
        message: &Message<Payload>,
        offsets: HashMap<String, usize>,
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

        self.send_message(&reply)
    }

    fn handle_list_committed_offsets(
        &mut self,
        message: &Message<Payload>,
        keys: &Vec<KeyId>,
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

        self.send_message(&reply)
    }

    fn handle_gossip(&mut self, src: &str, seen: SeenLogs) -> anyhow::Result<()> {
        let node = self.known.get_mut(src).expect("Unknown node");

        let committed_cloned = self.committed_logs.clone();
        let uncommitted_cloned = self.uncommitted_logs.clone();

        let mut committed_logs = committed_cloned.lock().unwrap();
        let mut uncommitted_logs = uncommitted_cloned.lock().unwrap();

        node.uncommitted.extend(seen.uncommitted.clone());
        node.committed.extend(seen.committed.clone());

        for (key, entries) in &seen.committed {
            for entry in entries {
                let mut new_entry = entry.clone();
                new_entry.seen_by.insert(self.node_id.to_owned());

                committed_logs
                    .entry(key.clone())
                    .or_default()
                    .push(new_entry);
            }
        }

        for (key, entries) in &seen.uncommitted {
            for entry in entries {
                let mut new_entry = entry.clone();
                new_entry.seen_by.insert(self.node_id.to_owned());

                uncommitted_logs
                    .entry(key.clone())
                    .or_default()
                    .push(new_entry);
            }
        }

        self.anchor_messages(&mut uncommitted_logs)
    }

    fn handle_trigger_gossip(&mut self) -> anyhow::Result<()> {
        if self.neighbors.is_empty() {
            return Ok(());
        }

        let committed_cloned = self.committed_logs.clone();
        let uncommitted_cloned = self.uncommitted_logs.clone();

        let mut committed_logs = committed_cloned.lock().unwrap();
        let mut uncommitted_logs = uncommitted_cloned.lock().unwrap();

        let mut messages = Vec::new();

        for neighbor in &self.neighbors {
            let mut n_not_seen = SeenLogs::default();

            for (key, value) in committed_logs.iter() {
                for entry in value {
                    if !self.known.contains_key(neighbor)
                        || !self
                            .known
                            .get(neighbor)
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

                    if n_not_seen.committed.len() == self.gossip_batch_size {
                        break;
                    }
                }

                if n_not_seen.committed.len() == self.gossip_batch_size {
                    break;
                }
            }

            for (key, value) in uncommitted_logs.iter() {
                for entry in value {
                    if !self.known.contains_key(key)
                        || !self
                            .known
                            .get(neighbor)
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

                    if n_not_seen.uncommitted.len() == self.gossip_batch_size {
                        break;
                    }
                }

                if n_not_seen.uncommitted.len() == self.gossip_batch_size {
                    break;
                }
            }

            messages.push((neighbor.to_owned(), n_not_seen));
        }

        let seen_logs = messages
            .iter()
            .filter(|(_, seen)| !seen.uncommitted.is_empty() || !seen.committed.is_empty())
            .collect::<Vec<_>>();

        let mut messages = Vec::new();

        for (neighbor, n_not_seen) in &seen_logs {
            let known = self.known.get_mut(neighbor).expect("Unknown node");
            known.committed.extend(n_not_seen.committed.clone());
            known.uncommitted.extend(n_not_seen.uncommitted.clone());

            messages.push(Message::new(
                self.node_id.to_owned(),
                neighbor.to_owned(),
                Body::new(
                    Some(self.message_id),
                    None,
                    Payload::Gossip {
                        seen: n_not_seen.clone(),
                    },
                ),
            ));
        }

        for (neighbor, n_not_seen) in &seen_logs {
            for seen in n_not_seen.uncommitted.iter() {
                for c in seen.1.iter() {
                    let entries = uncommitted_logs.get_mut(seen.0).expect("Invalid key");
                    if let Some(entry) = entries.iter_mut().find(|e| e.msg_id == c.msg_id) {
                        entry.seen_by.insert(neighbor.to_owned());
                    }
                }
            }

            for seen in n_not_seen.committed.iter() {
                for c in seen.1.iter() {
                    let entries = committed_logs.get_mut(seen.0).expect("Invalid key");
                    if let Some(entry) = entries.iter_mut().find(|e| e.msg_id == c.msg_id) {
                        entry.seen_by.insert(neighbor.to_owned());
                    }
                }
            }
        }

        if !messages.is_empty() {
            return self.send_messages(&messages);
        }

        Ok(())
    }

    fn handle_anchor(&mut self) -> anyhow::Result<()> {
        let uncommitted_cloned = self.uncommitted_logs.clone();
        let mut uncommitted_logs = uncommitted_cloned.lock().unwrap();

        self.anchor_messages(&mut uncommitted_logs)
    }

    fn anchor_messages(&mut self, uncommitted_logs: &mut MutexGuard<Logs>) -> anyhow::Result<()> {
        let mut anchored_uncommitted_logs = self.anchored_uncommitted_logs.lock().unwrap();

        let mut cluster = self.neighbors.clone();
        cluster.insert(self.node_id.to_owned());

        for (key, entries) in uncommitted_logs.iter_mut() {
            let anchor_entries = entries
                .iter()
                .filter(|entry| entry.seen_by == cluster)
                .cloned()
                .collect::<Vec<_>>();

            entries.retain(|entry| entry.seen_by != cluster);
            anchored_uncommitted_logs
                .entry(key.to_owned())
                .or_default()
                .extend(anchor_entries);
        }

        Ok(())
    }
}

impl Node<Payload> for KafkaStyleLogNode<'_> {
    fn init(&mut self, tx: std::sync::mpsc::Sender<Message<Payload>>) -> anyhow::Result<()> {
        let node_id = self.node_id.clone();
        let gossip_interval = self.gossip_interval;
        let _ = std::thread::spawn(move || loop {
            std::thread::sleep(gossip_interval);

            let trigger_gossip = Message::<Payload>::new(
                node_id.clone(),
                node_id.clone(),
                Body::new(None, None, Payload::TriggerGossip),
            );

            if tx.send(trigger_gossip).is_err() {
                break;
            }

            let anchor = Message::<Payload>::new(
                node_id.clone(),
                node_id.clone(),
                Body::new(None, None, Payload::Anchor),
            );

            if tx.send(anchor).is_err() {
                break;
            }
        });

        Ok(())
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        // TODO: check iof message has been seen already
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
            Payload::TriggerGossip => self.handle_trigger_gossip()?,
            Payload::Gossip { seen } => self.handle_gossip(message.src(), seen.clone())?,
            Payload::Anchor => self.handle_anchor()?,
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

//#[cfg(test)]
//mod tests {
//    use std::{collections::HashMap, io::Write};
//
//    use crate::Payload;
//
//    #[test]
//    fn test_serializer() {
//        let mut stdout = std::io::stdout().lock();
//
//        let mut entries = HashMap::new();
//        entries.insert(1, 10);
//        entries.insert(2, 11);
//        entries.insert(9, 123);
//        entries.insert(5, 9786);
//        entries.insert(6, 1);
//        let mut msgs = HashMap::new();
//        msgs.insert(1.to_string(), entries);
//
//        let payload = Payload::PollOk { msgs };
//
//        serde_json::to_writer(&mut stdout, &payload).expect("Error serializing response");
//
//        stdout
//            .write_all(b"\n")
//            .expect("Error writing response to stdout");
//    }
//}

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
        keys: Vec<KeyId>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<KeyId, Offset>,
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
type Logs = HashMap<KeyId, HashSet<LogEntry>>;

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

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.offset == other.offset && self.msg_id == other.msg_id
    }
}

impl Eq for LogEntry {}

impl Hash for LogEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.offset.hash(state);
        self.msg_id.hash(state);
    }
}

impl LogEntry {
    fn filter_and_map_by_offset(
        log_entries: &HashSet<LogEntry>,
        offset: usize,
    ) -> HashMap<usize, usize> {
        log_entries
            .iter()
            .filter(|l| l.offset >= offset)
            .map(|l| (l.offset, l.msg))
            .collect::<HashMap<_, _>>()
    }
}

struct LogStore {
    src: String,
    uncommitted_logs: Logs,
    committed_logs: Logs,
}

impl LogStore {
    fn new(src: &str) -> Self {
        Self {
            src: src.to_owned(),
            uncommitted_logs: Default::default(),
            committed_logs: Default::default(),
        }
    }

    fn init(&mut self, scr: &str) {
        self.src = scr.to_owned();
    }

    fn append_committed(&mut self, logs: &Logs) {
        for (key, entries) in logs {
            for entry in entries {
                let mut new_entry = entry.clone();
                new_entry.seen_by.insert(self.src.to_owned());

                self.committed_logs
                    .entry(key.clone())
                    .or_default()
                    .insert(new_entry);
            }
        }
    }

    fn append_uncommitted(&mut self, logs: &Logs) {
        for (key, entries) in logs {
            for entry in entries {
                let mut new_entry = entry.clone();
                new_entry.seen_by.insert(self.src.to_owned());

                self.uncommitted_logs
                    .entry(key.clone())
                    .or_default()
                    .insert(new_entry);
            }
        }
    }

    fn append(
        &mut self,
        key: &str,
        msg_id: usize,
        offset: usize,
        msg: usize,
    ) -> anyhow::Result<()> {
        let mut seen_by = HashSet::new();
        seen_by.insert(self.src.to_owned());

        let log_entry = LogEntry {
            msg_id,
            key: key.to_owned(),
            offset,
            msg,
            seen_by,
        };

        self.uncommitted_logs
            .entry(key.to_owned())
            .or_default()
            .insert(log_entry);

        Ok(())
    }

    fn commit(&mut self, offsets: HashMap<String, usize>) -> anyhow::Result<()> {
        for (key, offset) in offsets {
            let Some(entries) = self.uncommitted_logs.get_mut(&key) else {
                continue;
            };

            if entries.is_empty() {
                continue;
            }

            let committed = entries
                .iter()
                .filter(|l| l.offset <= offset)
                .cloned()
                .collect::<Vec<_>>();

            entries.retain(|l| l.offset > offset);
            self.committed_logs
                .entry(key.clone())
                .or_default()
                .extend(committed.clone());
        }

        Ok(())
    }

    //fn list_committed(&mut self, keys: &Vec<KeyId>) -> anyhow::Result<HashMap<String, usize>> {
    //    let mut offsets = HashMap::new();
    //
    //    for key in keys {
    //        let Some(entries) = self.committed_logs.get(key) else {
    //            continue;
    //        };
    //
    //        offsets.insert(key.clone(), entries.len());
    //    }
    //
    //    Ok(offsets)
    //}
}

struct KafkaStyleLogNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: NodeId,
    message_id: usize,
    cluster: HashSet<NodeId>,
    neighbors: HashSet<NodeId>,
    known: Arc<Mutex<HashMap<NodeId, SeenLogs>>>,
    connection: Arc<Mutex<Connection>>,
    gossip_interval: Duration,
    gossip_batch_size: usize,
    log_store: Arc<Mutex<LogStore>>,
    anchored_uncommitted_logs: Arc<Mutex<Logs>>,
    anchored_committed_logs: Arc<Mutex<Logs>>,
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
            gossip_interval: Duration::from_millis(100),
            gossip_batch_size: 200,
            log_store: Arc::new(Mutex::new(LogStore::new(node_id))),
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

        self.log_store
            .lock()
            .unwrap()
            .append(key, self.message_id, offset, msg)?;

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
        let committed_cloned = self.anchored_committed_logs.clone();
        let uncommitted_cloned = self.anchored_uncommitted_logs.clone();

        let committed_logs = committed_cloned.lock().unwrap();
        let uncommitted_logs = uncommitted_cloned.lock().unwrap();

        let mut msgs = HashMap::new();

        for (key, offset) in &offsets {
            let mut log_entries = HashMap::new();

            log_entries.extend(self.filter_logs(&committed_logs, key, *offset));
            log_entries.extend(self.filter_logs(&uncommitted_logs, key, *offset));

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
        self.log_store.lock().unwrap().commit(offsets)?;

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
        //let offsets = self.log_store.lock().unwrap().list_committed(keys)?;
        let committed_cloned = self.anchored_committed_logs.clone();

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
        eprintln!(
            "[{} -> {}] Gossip - Committed: [{:#?}]",
            src, self.node_id, seen.committed,
        );
        eprintln!(
            "[{} -> {}] Gossip - Uncommitted: [{:#?}]",
            src, self.node_id, seen.uncommitted
        );

        let known_cloned = self.known.clone();
        let mut known = known_cloned.lock().unwrap();
        let node = known.get_mut(src).expect("Unknown node");

        node.uncommitted.extend(seen.uncommitted.clone());
        node.committed.extend(seen.committed.clone());

        let cloned_log_store = self.log_store.clone();
        let mut log_store = cloned_log_store.lock().unwrap();

        log_store.append_committed(&seen.committed);
        log_store.append_uncommitted(&seen.uncommitted);

        self.anchor_messages(&mut log_store)
    }

    fn handle_trigger_gossip(&mut self) -> anyhow::Result<()> {
        if self.neighbors.is_empty() || self.node_id == "uninit" {
            return Ok(());
        }

        let cloned_log_store = self.log_store.clone();
        let mut log_store = cloned_log_store.lock().unwrap();

        let known_cloned = self.known.clone();
        let mut known = known_cloned.lock().unwrap();

        let mut messages = Vec::new();

        for neighbor in &self.neighbors {
            let mut n_not_seen = SeenLogs::default();

            for (key, value) in log_store.committed_logs.iter() {
                for entry in value {
                    if !known.contains_key(neighbor)
                        || !known
                            .get(neighbor)
                            .expect("Unknown node")
                            .committed
                            .contains_key(key)
                    {
                        n_not_seen
                            .committed
                            .entry(key.clone())
                            .or_default()
                            .insert(entry.clone());
                    }

                    if n_not_seen.committed.len() == self.gossip_batch_size {
                        break;
                    }
                }

                if n_not_seen.committed.len() == self.gossip_batch_size {
                    break;
                }
            }

            for (key, value) in log_store.uncommitted_logs.iter() {
                for entry in value {
                    if !known.contains_key(key)
                        || !known
                            .get(neighbor)
                            .expect("Unknown node")
                            .uncommitted
                            .contains_key(key)
                    {
                        n_not_seen
                            .uncommitted
                            .entry(key.clone())
                            .or_default()
                            .insert(entry.clone());
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
            let node = known.get_mut(neighbor).expect("Unknown node");
            node.committed.extend(n_not_seen.committed.clone());
            node.uncommitted.extend(n_not_seen.uncommitted.clone());

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
            for (key, uncommitted) in n_not_seen.uncommitted.iter() {
                for u in uncommitted.iter() {
                    let entries = log_store
                        .uncommitted_logs
                        .get_mut(key)
                        .expect("Invalid key");

                    if let Some(entry) = entries.take(u) {
                        let mut new_entry = entry;
                        new_entry.seen_by.insert(neighbor.to_owned());
                        entries.insert(new_entry);
                    }
                }
            }

            for (key, committed) in n_not_seen.committed.iter() {
                for c in committed.iter() {
                    let entries = log_store.committed_logs.get_mut(key).expect("Invalid key");

                    if let Some(entry) = entries.take(c) {
                        let mut new_entry = entry;
                        new_entry.seen_by.insert(neighbor.to_owned());
                        entries.insert(new_entry);
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
        if self.node_id == "uninit" {
            return Ok(());
        }

        let cloned_log_store = self.log_store.clone();
        let mut log_store = cloned_log_store.lock().unwrap();
        self.anchor_messages(&mut log_store)
    }

    fn anchor_messages(&mut self, log_store: &mut MutexGuard<LogStore>) -> anyhow::Result<()> {
        let mut anchored_uncommitted_logs = self.anchored_uncommitted_logs.lock().unwrap();
        let mut anchored_committed_logs = self.anchored_committed_logs.lock().unwrap();

        for (key, entries) in log_store.uncommitted_logs.iter_mut() {
            let anchor_entries = entries
                .iter()
                .filter(|entry| entry.seen_by == self.cluster)
                .cloned()
                .collect::<Vec<_>>();

            entries.retain(|entry| entry.seen_by != self.cluster);
            anchored_uncommitted_logs
                .entry(key.to_owned())
                .or_default()
                .extend(anchor_entries);
        }

        for (key, entries) in log_store.committed_logs.iter_mut() {
            let anchor_entries = entries
                .iter()
                .filter(|entry| entry.seen_by == self.cluster)
                .cloned()
                .collect::<Vec<_>>();

            entries.retain(|entry| entry.seen_by != self.cluster);
            anchored_committed_logs
                .entry(key.to_owned())
                .or_default()
                .extend(anchor_entries);
        }

        Ok(())
    }

    fn filter_logs(
        &self,
        logs: &MutexGuard<Logs>,
        key: &str,
        offset: usize,
    ) -> HashMap<usize, usize> {
        if logs.contains_key(key) {
            let log = logs.get(key).expect("Key not found");

            return LogEntry::filter_and_map_by_offset(log, offset);
        }

        HashMap::default()
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
//    use std::collections::HashSet;
//
//    #[test]
//    fn test_windows_3() {
//        let mut entries = HashSet::new();
//        entries.insert(1);
//        entries.insert(2);
//        entries.insert(3);
//        entries.insert(4);
//        entries.insert(5);
//        entries.insert(6);
//        entries.insert(7);
//
//        let mut entries = entries.into_iter().collect::<Vec<_>>();
//        entries.sort();
//
//        let mut max_offset = entries.clone().into_iter().next().unwrap();
//        //let offsets = entries.into_iter().collect::<Vec<_>>();
//
//        for pair in entries.windows(2) {
//            println!("Pair: {} - {}", pair[0], pair[1]);
//            if pair[1] == pair[0] + 1 {
//                max_offset = pair[1];
//            } else {
//                break;
//            }
//        }
//
//        assert_eq!(max_offset, 7);
//    }
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

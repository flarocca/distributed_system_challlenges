use std::collections::{HashMap, HashSet};

use distributed_system_challenges::{
    main_loop,
    writters::{MessageWritter, StdoutJsonWritter},
    Body, Message, Node,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    TriggerGossip,
    Gossip {
        seen: HashSet<usize>,
    },
}

struct BroadcastNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: String,
    message_id: usize,
    messages: HashSet<usize>,
    neighbors: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
}

impl<'a> BroadcastNode<'a> {
    fn new(writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>) -> Self {
        Self {
            writter,
            node_id: "uninit".to_owned(),
            message_id: 0,
            messages: HashSet::new(),
            neighbors: Vec::new(),
            known: HashMap::new(),
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
        self.known
            .extend(node_ids.iter().map(|id| (id.clone(), HashSet::new())));

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(&reply)
    }

    fn handle_broadcast(&mut self, message: &Message<Payload>, value: usize) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::BroadcastOk,
            ),
        );

        self.messages.insert(value);
        self.send_message(&reply)
    }

    fn handle_read(&mut self, message: &Message<Payload>) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::ReadOk {
                    messages: self.messages.clone(),
                },
            ),
        );

        self.send_message(&reply)
    }

    fn handle_topology(
        &mut self,
        message: &Message<Payload>,
        topology: &HashMap<String, Vec<String>>,
    ) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(Some(self.message_id), message.msg_id(), Payload::TopologyOk),
        );

        self.neighbors = topology
            .get(&self.node_id)
            .map_or_else(Vec::new, |v| v.clone());

        self.send_message(&reply)
    }

    fn handle_gossip(&mut self, src: &str, seen: HashSet<usize>) {
        self.known
            .get_mut(src)
            .expect("Unknown node")
            .extend(seen.iter().copied());
        self.messages.extend(seen);
    }

    fn handle_trigger_gossip(&mut self) -> anyhow::Result<()> {
        if self.neighbors.is_empty() {
            return Ok(());
        }

        let messages = self
            .neighbors
            .iter()
            .map(|n| {
                let n_not_seen = self
                    .messages
                    .difference(self.known.get(n).expect("Unknown node"))
                    .copied()
                    .collect();

                Message::new(
                    self.node_id.clone(),
                    n.to_owned(),
                    Body::new(
                        Some(self.message_id),
                        None,
                        Payload::Gossip { seen: n_not_seen },
                    ),
                )
            })
            .collect::<Vec<_>>();

        self.send_messages(&messages)
    }
}

impl Node<Payload> for BroadcastNode<'_> {
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

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        match &message.body().payload {
            Payload::Init { node_id, node_ids } => self.handle_init(&message, node_id, node_ids)?,
            Payload::InitOk => {}
            Payload::Broadcast { message: value } => self.handle_broadcast(&message, *value)?,

            Payload::BroadcastOk => {}
            Payload::Read => self.handle_read(&message)?,
            Payload::ReadOk { .. } => {}
            Payload::Topology { topology } => self.handle_topology(&message, topology)?,
            Payload::TopologyOk => {}
            Payload::TriggerGossip => self.handle_trigger_gossip()?,
            Payload::Gossip { seen } => self.handle_gossip(message.src(), seen.clone()),
        };

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let stdout = std::io::stdout().lock();
    let mut stdout_json_writter: Box<dyn MessageWritter<Message<Payload>>> =
        Box::new(StdoutJsonWritter::new(stdout));

    let mut node = BroadcastNode::new(&mut stdout_json_writter);
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

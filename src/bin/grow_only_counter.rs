use distributed_system_challenges::{
    main_loop,
    writters::{MessageWritter, StdoutJsonWritter},
    Body, Message, Node,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Add {
        delta: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: usize,
    },
    TriggerGossip,
    Gossip {
        seen: HashMap<usize, usize>,
    },
}

struct GrowOnlyCounterNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: String,
    message_id: usize,
    messages: HashMap<usize, usize>,
    value: usize,
    neighbors: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
}

impl<'a> GrowOnlyCounterNode<'a> {
    fn new(writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>) -> Self {
        Self {
            writter,
            node_id: "uninit".to_owned(),
            message_id: 0,
            messages: HashMap::new(),
            value: 0,
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

        let nodes = node_ids
            .iter()
            .filter(|n| *n != node_id)
            .map(|n| n.to_owned())
            .collect::<Vec<_>>();

        self.known
            .extend(nodes.iter().map(|id| (id.clone(), HashSet::new())));
        self.neighbors = nodes;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(&reply)
    }

    fn handle_add(&mut self, message: &Message<Payload>, delta: usize) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(Some(self.message_id), message.msg_id(), Payload::AddOk),
        );

        self.messages
            .insert(message.msg_id().expect("No message id"), delta);
        self.value += delta;

        self.send_message(&reply)
    }

    fn handle_read(&mut self, message: &Message<Payload>) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::ReadOk { value: self.value },
            ),
        );

        self.send_message(&reply)
    }

    fn handle_gossip(&mut self, src: &str, seen: HashMap<usize, usize>) -> anyhow::Result<()> {
        self.known
            .get_mut(src)
            .expect("Unknown node")
            .extend(seen.keys().copied());

        for (msg_id, value) in seen {
            if self.messages.insert(msg_id, value).is_none() {
                self.value += value;
            }
        }

        Ok(())
    }

    fn handle_trigger_gossip(&mut self) -> anyhow::Result<()> {
        if self.neighbors.is_empty() {
            return Ok(());
        }

        let messages = self
            .neighbors
            .iter()
            .map(|n| {
                let mut n_not_seen: HashMap<usize, usize> = HashMap::new();
                for (msg_id, value) in self.messages.iter() {
                    if !self.known.get(n).expect("Unknown node").contains(msg_id) {
                        n_not_seen.insert(*msg_id, *value);
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

        self.send_messages(&messages)
    }
}

impl Node<Payload> for GrowOnlyCounterNode<'_> {
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
            Payload::Init { node_id, node_ids } => self.handle_init(&message, node_id, node_ids),
            Payload::InitOk => Ok(()),
            Payload::Add { delta } => self.handle_add(&message, *delta),
            Payload::AddOk => Ok(()),
            Payload::Read => self.handle_read(&message),
            Payload::ReadOk { .. } => Ok(()),
            Payload::TriggerGossip => self.handle_trigger_gossip(),
            Payload::Gossip { seen } => self.handle_gossip(message.src(), seen.clone()),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdout = std::io::stdout().lock();
    let mut stdout_json_writter: Box<dyn MessageWritter<Message<Payload>>> =
        Box::new(StdoutJsonWritter::new(stdout));

    let mut node = GrowOnlyCounterNode::new(&mut stdout_json_writter);
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

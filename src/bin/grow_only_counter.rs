use anyhow::Context;
use distributed_system_challenges::{main_loop, Body, Message, Node};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
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

struct GrowOnlyCounterNode {
    node_id: String,
    message_id: usize,
    messages: HashMap<usize, usize>,
    value: usize,
    neighbors: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
}

impl GrowOnlyCounterNode {
    fn new() -> Self {
        Self {
            node_id: "uninit".to_owned(),
            message_id: 0,
            messages: HashMap::new(),
            value: 0,
            neighbors: Vec::new(),
            known: HashMap::new(),
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
            .extend(nodes.iter().map(|id| (id.clone(), HashSet::new())));
        self.neighbors = nodes;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_add(
        &mut self,
        message: &Message<Payload>,
        delta: usize,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(Some(self.message_id), message.msg_id(), Payload::AddOk),
        );

        self.messages
            .insert(message.msg_id().expect("No message id"), delta);
        self.value += delta;

        self.send_message(stdout, &reply)
    }

    fn handle_read(
        &mut self,
        message: &Message<Payload>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::ReadOk { value: self.value },
            ),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_gossip(&mut self, src: &str, seen: HashMap<usize, usize>) {
        self.known
            .get_mut(src)
            .expect("Unknown node")
            .extend(seen.keys().copied());

        for (msg_id, value) in seen {
            if self.messages.insert(msg_id, value).is_none() {
                self.value += value;
            }
        }
    }

    fn handle_trigger_gossip(&mut self, stdout: &mut StdoutLock) -> anyhow::Result<()> {
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

        self.send_messages(stdout, &messages)
    }
}

impl Node<Payload> for GrowOnlyCounterNode {
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
        match &message.body().payload {
            Payload::Init { node_id, node_ids } => {
                self.handle_init(&message, node_id, node_ids, stdout)?
            }
            Payload::InitOk => {}
            Payload::Add { delta } => self.handle_add(&message, *delta, stdout)?,

            Payload::AddOk => {}
            Payload::Read => self.handle_read(&message, stdout)?,
            Payload::ReadOk { .. } => {}
            Payload::TriggerGossip => self.handle_trigger_gossip(stdout)?,
            Payload::Gossip { seen } => self.handle_gossip(message.src(), seen.clone()),
        };

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = GrowOnlyCounterNode::new();
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

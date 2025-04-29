use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
};

use anyhow::Context;
use distributed_system_challenges::{main_loop, Body, Message, Node};
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

struct BroadcastNode {
    node_id: String,
    message_id: usize,
    messages: HashSet<usize>,
    neighbors: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
}

impl BroadcastNode {
    fn new() -> Self {
        Self {
            node_id: "uninit".to_owned(),
            message_id: 0,
            messages: HashSet::new(),
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
        self.known
            .extend(node_ids.iter().map(|id| (id.clone(), HashSet::new())));

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_broadcast(
        &mut self,
        message: &Message<Payload>,
        value: usize,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
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
                Payload::ReadOk {
                    messages: self.messages.clone(),
                },
            ),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_topology(
        &mut self,
        message: &Message<Payload>,
        topology: &HashMap<String, Vec<String>>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(Some(self.message_id), message.msg_id(), Payload::TopologyOk),
        );

        self.neighbors = topology
            .get(&self.node_id)
            .map_or_else(Vec::new, |v| v.clone());

        self.send_message(stdout, &reply)
    }

    fn handle_gossip(&mut self, src: &str, seen: HashSet<usize>) {
        self.known
            .get_mut(src)
            .expect("Unknown node")
            .extend(seen.iter().copied());
        self.messages.extend(seen);
    }

    fn handle_trigger_gossip(&mut self, stdout: &mut StdoutLock) -> anyhow::Result<()> {
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

        self.send_messages(stdout, &messages)
    }
}

impl Node<Payload> for BroadcastNode {
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
        //match &message.body().payload {
        //    Payload::Init { node_id, node_ids } => {
        //        self.handle_init(&message, node_id, node_ids, stdout)?
        //    }
        //    Payload::InitOk => {}
        //    Payload::Broadcast { message: value } => {
        //        self.handle_broadcast(&message, *value, stdout)?
        //    }
        //
        //    Payload::BroadcastOk => {}
        //    Payload::Read => self.handle_read(&message, stdout)?,
        //    Payload::ReadOk { .. } => {}
        //    Payload::Topology { topology } => self.handle_topology(&message, topology, stdout)?,
        //    Payload::TopologyOk => {}
        //    Payload::TriggerGossip => self.handle_trigger_gossip(stdout)?,
        //    Payload::Gossip { seen } => self.handle_gossip(message.src(), seen.clone()),
        //};

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = BroadcastNode::new();
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

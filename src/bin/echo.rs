use anyhow::Context;
use distributed_system_challenges::{main_loop, Body, Message, Node};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

struct EchoNode {
    node_id: String,
    message_id: usize,
}

impl EchoNode {
    fn new() -> Self {
        Self {
            node_id: "uninit".to_owned(),
            message_id: 0,
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

    fn handle_init(
        &mut self,
        message: &Message<Payload>,
        node_id: &str,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        self.node_id = node_id.to_owned();

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(stdout, &reply)
    }

    fn handle_echo(
        &mut self,
        message: &Message<Payload>,
        echo: &str,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::EchoOk {
                    echo: echo.to_owned(),
                },
            ),
        );

        self.send_message(stdout, &reply)
    }
}

impl Node<Payload> for EchoNode {
    fn init(&mut self, _: std::sync::mpsc::Sender<Message<Payload>>) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_message(
        &mut self,
        message: Message<Payload>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match &message.body().payload {
            Payload::Init { node_id, .. } => self.handle_init(&message, node_id, stdout)?,
            Payload::InitOk => {}
            Payload::Echo { echo } => self.handle_echo(&message, echo, stdout)?,

            Payload::EchoOk { .. } => {}
        };

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = EchoNode::new();
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

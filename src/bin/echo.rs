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
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

struct EchoNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: String,
    message_id: usize,
}

impl<'a> EchoNode<'a> {
    fn new(writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>) -> Self {
        Self {
            writter,
            node_id: "uninit".to_owned(),
            message_id: 0,
        }
    }

    fn send_message(&mut self, message: &Message<Payload>) -> anyhow::Result<()> {
        self.writter.send_message(message)?;
        self.message_id += 1;

        Ok(())
    }

    fn handle_init(&mut self, message: &Message<Payload>, node_id: &str) -> anyhow::Result<()> {
        self.node_id = node_id.to_owned();

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(&reply)
    }

    fn handle_echo(&mut self, message: &Message<Payload>, echo: &str) -> anyhow::Result<()> {
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

        self.send_message(&reply)
    }
}

impl Node<Payload> for EchoNode<'_> {
    fn init(&mut self, _tx: std::sync::mpsc::Sender<Message<Payload>>) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        match &message.body().payload {
            Payload::Init { node_id, .. } => self.handle_init(&message, node_id)?,
            Payload::InitOk => {}
            Payload::Echo { echo } => self.handle_echo(&message, echo)?,

            Payload::EchoOk { .. } => {}
        };

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let stdout = std::io::stdout().lock();
    let mut stdout_json_writter: Box<dyn MessageWritter<Message<Payload>>> =
        Box::new(StdoutJsonWritter::new(stdout));

    let mut node = EchoNode::new(&mut stdout_json_writter);
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

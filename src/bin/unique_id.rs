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
    Generate,
    GenerateOk {
        id: String,
    },
}

struct UniqueIdNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: String,
    message_id: usize,
}

impl<'a> UniqueIdNode<'a> {
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

    fn handle_generate(&mut self, message: &Message<Payload>) -> anyhow::Result<()> {
        let id = format!("{}-{}", self.node_id, uuid::Uuid::new_v4().simple());
        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::GenerateOk { id },
            ),
        );

        self.send_message(&reply)
    }
}

impl Node<Payload> for UniqueIdNode<'_> {
    fn init(&mut self, _: std::sync::mpsc::Sender<Message<Payload>>) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        match &message.body().payload {
            Payload::Init { node_id, .. } => self.handle_init(&message, node_id),
            Payload::InitOk => Ok(()),
            Payload::Generate => self.handle_generate(&message),
            Payload::GenerateOk { .. } => Ok(()),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdout = std::io::stdout().lock();
    let mut stdout_json_writter: Box<dyn MessageWritter<Message<Payload>>> =
        Box::new(StdoutJsonWritter::new(stdout));

    let mut node = UniqueIdNode::new(&mut stdout_json_writter);
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

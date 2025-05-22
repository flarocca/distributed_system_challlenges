use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use distributed_system_challenges::{
    Body, Message, Node, main_loop,
    writters::{MessageWritter, StdoutJsonWritter},
};
use serde::{Deserialize, Serialize, Serializer};

type NodeId = String;
type KeyId = usize;
type LogValue = usize;

#[derive(Debug, Clone, Deserialize)]
enum Error {
    KetDoesNotExist,
    PreconditionFailed,
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let error_code = match self {
            Error::KetDoesNotExist => 20,
            Error::PreconditionFailed => 22,
        };
        serializer.serialize_u64(error_code as u64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init {
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },
    InitOk,
    Read {
        key: KeyId,
    },
    ReadOk {
        value: LogValue,
    },
    Write {
        key: KeyId,
        value: LogValue,
    },
    WriteOk,
    Cas {
        key: KeyId,
        from: LogValue,
        to: LogValue,
    },
    CasOk,
    Error {
        code: Error,
        text: String,
    },
}

struct RaftNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: NodeId,
    message_id: usize,
    cluster: HashSet<NodeId>,
    neighbors: HashSet<NodeId>,
    store: Arc<Mutex<HashMap<KeyId, LogValue>>>,
}

impl<'a> RaftNode<'a> {
    fn new(writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>) -> Self {
        let node_id = "uninit";
        Self {
            node_id: node_id.to_owned(),
            message_id: 0,
            cluster: HashSet::new(),
            neighbors: HashSet::new(),
            writter,
            store: Arc::new(Mutex::new(HashMap::new())),
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

        self.neighbors = nodes;
        self.cluster = cluster;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(Some(self.message_id), message.msg_id(), Payload::InitOk),
        );

        self.send_message(&reply)
    }

    fn handle_read(&mut self, message: &Message<Payload>, key: KeyId) -> anyhow::Result<()> {
        let clone_store = self.store.clone();
        let store = clone_store.lock().unwrap();
        let value = store.get(&key).cloned();

        let payload = match value {
            Some(v) => Payload::ReadOk { value: v },
            None => Payload::Error {
                code: Error::KetDoesNotExist,
                text: format!("Key {} not found", key),
            },
        };

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(Some(self.message_id), message.msg_id(), payload),
        );

        self.send_message(&reply)
    }

    fn handle_write(
        &mut self,
        message: &Message<Payload>,
        key: KeyId,
        value: LogValue,
    ) -> anyhow::Result<()> {
        let clone_store = self.store.clone();
        let mut store = clone_store.lock().unwrap();
        store.insert(key, value);

        self.broadcast(&Payload::Write { key, value })?;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(Some(self.message_id), message.msg_id(), Payload::WriteOk),
        );

        self.send_message(&reply)
    }

    fn handle_cas(
        &mut self,
        message: &Message<Payload>,
        key: KeyId,
        from: LogValue,
        to: LogValue,
    ) -> anyhow::Result<()> {
        let clone_store = self.store.clone();
        let mut store = clone_store.lock().unwrap();
        let value = store.get(&key).cloned();

        let payload = match value {
            Some(v) => {
                if v == from {
                    store.insert(key, to);
                    Payload::CasOk
                } else {
                    Payload::Error {
                        code: Error::PreconditionFailed,
                        text: format!("Expected {}, but had {}", v, from),
                    }
                }
            }
            None => Payload::Error {
                code: Error::KetDoesNotExist,
                text: format!("Key {} not found", key),
            },
        };

        self.broadcast(&Payload::Write { key, value: to })?;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(Some(self.message_id), message.msg_id(), payload),
        );

        self.send_message(&reply)
    }

    fn broadcast(&mut self, payload: &Payload) -> anyhow::Result<()> {
        let messages = self
            .neighbors
            .iter()
            .map(|neighbor| {
                Message::new(
                    self.node_id.to_owned(),
                    neighbor.to_owned(),
                    Body::new(Some(self.message_id), None, payload.clone()),
                )
            })
            .collect::<Vec<_>>();

        self.send_messages(&messages)
    }
}

impl Node<Payload> for RaftNode<'_> {
    fn init(&mut self, _tx: std::sync::mpsc::Sender<Message<Payload>>) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        match &message.body().payload {
            Payload::Init { node_id, node_ids } => self.handle_init(&message, node_id, node_ids),
            Payload::InitOk => Ok(()),
            Payload::Read { key } => self.handle_read(&message, *key),
            Payload::Write { key, value } => self.handle_write(&message, *key, *value),
            Payload::Cas { key, from, to } => self.handle_cas(&message, *key, *from, *to),
            Payload::ReadOk { .. } => Ok(()),
            Payload::WriteOk => Ok(()),
            Payload::CasOk => Ok(()),
            Payload::Error { .. } => Ok(()),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdout = std::io::stdout().lock();
    let mut stdout_json_writter: Box<dyn MessageWritter<Message<Payload>>> =
        Box::new(StdoutJsonWritter::new(stdout));

    let mut node = RaftNode::new(&mut stdout_json_writter);
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

#[cfg(test)]
mod tests {
    use crate::Payload;
    use distributed_system_challenges::{Body, Message};

    #[test]
    fn test_read_deserialization() {
        let json_message = r#"{"src":"c0","dest":"n1","body":{"msg_id":3,"in_reply_to":null,"type":"read","key":1}}"#;
        let message = serde_json::from_str::<Message<Payload>>(json_message).unwrap();

        assert_eq!(message.msg_id().unwrap(), 3);

        match &message.body().payload {
            Payload::Read { key } => {
                assert_eq!(*key, 1);
            }
            _ => panic!("Invalid payload type found"),
        }
    }

    #[test]
    fn test_write_deserialijation() {
        let json_message = r#"{"src":"c0","dest":"n1","body":{"msg_id":3,"in_reply_to":null,"type":"write","key":1,"value":5}}"#;
        let message = serde_json::from_str::<Message<Payload>>(json_message).unwrap();

        assert_eq!(message.msg_id().unwrap(), 3);

        match &message.body().payload {
            Payload::Write { key, value } => {
                assert_eq!(*key, 1);
                assert_eq!(*value, 5);
            }
            _ => panic!("Invalid payload type found"),
        }
    }

    #[test]
    fn test_cas_deserialization() {
        let json_message = r#"{"src":"c0","dest":"n1","body":{"msg_id":3,"in_reply_to":null,"type":"cas","key":1,"from":0,"to":5}}"#;
        let message = serde_json::from_str::<Message<Payload>>(json_message).unwrap();

        assert_eq!(message.msg_id().unwrap(), 3);

        match &message.body().payload {
            Payload::Cas { key, from, to } => {
                assert_eq!(*key, 1);
                assert_eq!(*from, 0);
                assert_eq!(*to, 5);
            }
            _ => panic!("Invalid payload type found"),
        }
    }

    #[test]
    fn test_read_ok_serialization() {
        let json_message = r#"{"src":"c0","dest":"n1","body":{"msg_id":3,"in_reply_to":null,"type":"read_ok","value":1}}"#;
        let payload = Payload::ReadOk { value: 1 };
        let message = Message::new(
            "c0".to_owned(),
            "n1".to_owned(),
            Body::new(Some(3), None, payload),
        );

        let serialized_message = serde_json::to_string(&message).unwrap();

        assert_eq!(json_message, serialized_message);
    }

    #[test]
    fn test_write_ok_serialization() {
        let json_message =
            r#"{"src":"c0","dest":"n1","body":{"msg_id":3,"in_reply_to":null,"type":"write_ok"}}"#;
        let payload = Payload::WriteOk {};
        let message = Message::new(
            "c0".to_owned(),
            "n1".to_owned(),
            Body::new(Some(3), None, payload),
        );

        let serialized_message = serde_json::to_string(&message).unwrap();

        assert_eq!(json_message, serialized_message);
    }

    #[test]
    fn test_cas_ok_serialization() {
        let json_message =
            r#"{"src":"c0","dest":"n1","body":{"msg_id":3,"in_reply_to":null,"type":"cas_ok"}}"#;
        let payload = Payload::CasOk {};
        let message = Message::new(
            "c0".to_owned(),
            "n1".to_owned(),
            Body::new(Some(3), None, payload),
        );

        let serialized_message = serde_json::to_string(&message).unwrap();

        assert_eq!(json_message, serialized_message);
    }
}

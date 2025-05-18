use distributed_system_challenges::{
    Body, Message, Node, main_loop,
    writters::{MessageWritter, StdoutJsonWritter},
};
use serde::{
    self, Deserialize, Deserializer, Serialize, Serializer,
    de::{Error, SeqAccess, Visitor},
    ser::SerializeSeq,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};

type NodeId = String;
type KeyId = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init {
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },
    InitOk,
    Txn {
        txn: Vec<Operation>,
    },
    TxnOk {
        txn: Vec<Operation>,
    },
    InternalTxn {
        txn: Vec<Operation>,
    },
}

#[derive(Debug, Clone)]
enum Operation {
    Read { key: KeyId, value: Option<usize> },
    Write { key: KeyId, value: usize },
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(OperationVisitor)
    }
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        match self {
            Operation::Read { key, value } => {
                seq.serialize_element("r")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
            Operation::Write { key, value } => {
                seq.serialize_element("w")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
        }
        seq.end()
    }
}

struct OperationVisitor;

impl<'de> Visitor<'de> for OperationVisitor {
    type Value = Operation;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "Invalid operation format. Expected [\"r\" or \"w\", key, value]"
        )
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let op_type: String = seq
            .next_element()?
            .ok_or_else(|| Error::custom("missing operation type"))?;
        let key: KeyId = seq
            .next_element()?
            .ok_or_else(|| Error::custom("missing key"))?;

        match op_type.as_str() {
            "r" => {
                let value = seq.next_element::<usize>().unwrap_or_default();
                Ok(Operation::Read { key, value })
            }
            "w" => {
                let value: usize = seq
                    .next_element()?
                    .ok_or_else(|| Error::custom("missing value"))?;
                Ok(Operation::Write { key, value })
            }
            _ => Err(Error::unknown_variant(&op_type, &["r", "w"])),
        }
    }
}

struct TotallyAvailableTransactionsNode<'a> {
    writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>,
    node_id: NodeId,
    message_id: usize,
    cluster: HashSet<NodeId>,
    neighbors: HashSet<NodeId>,
    log_store: Arc<Mutex<HashMap<KeyId, usize>>>,
}

impl<'a> TotallyAvailableTransactionsNode<'a> {
    fn new(writter: &'a mut Box<dyn MessageWritter<Message<Payload>>>) -> Self {
        let node_id = "uninit";
        Self {
            node_id: node_id.to_owned(),
            message_id: 0,
            cluster: HashSet::new(),
            neighbors: HashSet::new(),
            writter,
            log_store: Arc::new(Mutex::new(HashMap::new())),
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
            Body::new(None, message.msg_id(), Payload::InitOk),
        );

        self.send_message(&reply)
    }

    fn handle_txn(
        &mut self,
        message: &Message<Payload>,
        txn: &Vec<Operation>,
    ) -> anyhow::Result<()> {
        let processed_txn = self.handle_internal_txn(message, txn)?;
        self.broadcast_txn(&processed_txn)?;

        let reply = Message::new(
            message.dest().to_owned(),
            message.src().to_owned(),
            Body::new(
                Some(self.message_id),
                message.msg_id(),
                Payload::TxnOk { txn: processed_txn },
            ),
        );

        self.send_message(&reply)
    }

    fn handle_internal_txn(
        &mut self,
        _message: &Message<Payload>,
        txn: &Vec<Operation>,
    ) -> anyhow::Result<Vec<Operation>> {
        let mut processed_txn = Vec::new();

        let cloned_log_store = self.log_store.clone();
        let mut log_store = cloned_log_store.lock().unwrap();

        for operation in txn {
            let tx = match operation {
                Operation::Read { key, .. } => self.process_read(*key, &mut log_store)?,
                Operation::Write { key, value } => {
                    self.process_write(*key, *value, &mut log_store)?
                }
            };

            processed_txn.push(tx);
        }

        Ok(processed_txn)
    }

    fn process_read(
        &mut self,
        key: KeyId,
        log_store: &mut MutexGuard<HashMap<usize, usize>>,
    ) -> anyhow::Result<Operation> {
        let value = log_store.get(&key).cloned();

        Ok(Operation::Read { key, value })
    }

    fn process_write(
        &mut self,
        key: KeyId,
        value: usize,

        log_store: &mut MutexGuard<HashMap<usize, usize>>,
    ) -> anyhow::Result<Operation> {
        log_store.insert(key, value);

        Ok(Operation::Write { key, value })
    }

    fn broadcast_txn(&mut self, txn: &[Operation]) -> anyhow::Result<()> {
        let messages = self
            .neighbors
            .iter()
            .map(|neighbor| {
                Message::new(
                    self.node_id.to_owned(),
                    neighbor.to_owned(),
                    Body::new(
                        Some(self.message_id),
                        None,
                        Payload::InternalTxn { txn: txn.to_vec() },
                    ),
                )
            })
            .collect::<Vec<_>>();

        self.send_messages(&messages)
    }
}

impl Node<Payload> for TotallyAvailableTransactionsNode<'_> {
    fn init(&mut self, _tx: std::sync::mpsc::Sender<Message<Payload>>) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        match &message.body().payload {
            Payload::Init { node_id, node_ids } => self.handle_init(&message, node_id, node_ids),
            Payload::InitOk => Ok(()),
            Payload::Txn { txn } => self.handle_txn(&message, txn),
            Payload::TxnOk { txn: _ } => Ok(()),
            Payload::InternalTxn { txn } => self.handle_internal_txn(&message, txn).map(|_| ()),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdout = std::io::stdout().lock();
    let mut stdout_json_writter: Box<dyn MessageWritter<Message<Payload>>> =
        Box::new(StdoutJsonWritter::new(stdout));

    let mut node = TotallyAvailableTransactionsNode::new(&mut stdout_json_writter);
    main_loop::<Message<Payload>, _, Payload>(&mut node)
}

#[cfg(test)]
mod tests {
    use crate::{Operation, Payload};
    use distributed_system_challenges::{Body, Message};

    const JSON_MESSAGE: &str = r#"{"src":"c0","dest":"n1","body":{"msg_id":3,"in_reply_to":null,"type":"txn","txn":[["r",1,null],["r",2,5],["w",3,6]]}}"#;

    #[test]
    fn test_deserialization() {
        let message = serde_json::from_str::<Message<Payload>>(JSON_MESSAGE).unwrap();

        assert_eq!(message.msg_id().unwrap(), 3);

        match &message.body().payload {
            Payload::Txn { txn } => {
                assert_eq!(txn.len(), 3);

                let tx_0 = txn[0].clone();
                assert!(matches!(
                    tx_0,
                    Operation::Read {
                        key: 1,
                        value: None,
                    }
                ));
                let tx_1 = txn[1].clone();
                assert!(matches!(
                    tx_1,
                    Operation::Read {
                        key: 2,
                        value: Some(5),
                    }
                ));
                let tx_2 = txn[2].clone();
                assert!(matches!(tx_2, Operation::Write { key: 3, value: 6 }));
            }
            _ => panic!("Invalid payload type found"),
        }
    }

    #[test]
    fn test_serialization() {
        let txn = [
            Operation::Read {
                key: 1,
                value: None,
            },
            Operation::Read {
                key: 2,
                value: Some(5),
            },
            Operation::Write { key: 3, value: 6 },
        ];
        let payload = Payload::Txn { txn: txn.to_vec() };
        let message = Message::new(
            "c0".to_owned(),
            "n1".to_owned(),
            Body::new(Some(3), None, payload),
        );

        let serialized_message = serde_json::to_string(&message).unwrap();

        assert_eq!(JSON_MESSAGE, serialized_message);
    }
}

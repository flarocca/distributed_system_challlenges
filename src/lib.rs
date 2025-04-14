use anyhow::{bail, Context};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{io::StdoutLock, sync::mpsc::Sender};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    src: String,
    dest: String,
    body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    pub fn new(src: String, dest: String, body: Body<Payload>) -> Self {
        Self { src, dest, body }
    }

    pub fn msg_id(&self) -> Option<usize> {
        self.body.msg_id
    }

    pub fn src(&self) -> &str {
        &self.src
    }

    pub fn dest(&self) -> &str {
        &self.dest
    }

    pub fn body(&self) -> &Body<Payload> {
        &self.body
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

impl<Payload> Body<Payload> {
    pub fn new(msg_id: Option<usize>, in_reply_to: Option<usize>, payload: Payload) -> Self {
        Self {
            msg_id,
            in_reply_to,
            payload,
        }
    }
}

pub trait Node<Payload> {
    fn init(&mut self, tx: Sender<Message<Payload>>) -> anyhow::Result<()>;

    fn handle_message(
        &mut self,
        message: Message<Payload>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}

pub fn main_loop<M, N, P>(node: &mut N) -> anyhow::Result<()>
where
    M: Serialize + Deserialize<'static>,
    N: Node<P>,
    P: std::fmt::Debug + Serialize + DeserializeOwned + Send + 'static,
{
    let mut stdout = std::io::stdout().lock();
    let (tx, rx) = std::sync::mpsc::channel();

    let tx_cloned = tx.clone();

    node.init(tx_cloned)?;

    let reciver_thread = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Value>();

        for message in inputs {
            let message = message
                .context("Failed to parse message as Value")
                .expect("Failed to parse message as Value");

            let message: Message<P> = serde_json::from_value(message)
                .context("Failed to parse stdin input message")
                .expect("Failed to parse stdin input message");

            if tx.send(message).is_err() {
                bail!("Failed to send message to main thread");
            }
        }

        Ok(())
    });

    for message in rx {
        node.handle_message(message, &mut stdout)?;
    }

    reciver_thread
        .join()
        .expect("Failed to join reciver thread")
        .context("Failed to join reciver thread")?;

    Ok(())
}

use anyhow::Context;
use serde::Serialize;
use std::io::{StdoutLock, Write};

pub trait MessageWritter<T> {
    fn send_message(&mut self, message: &T) -> anyhow::Result<()>;

    fn send_messages(&mut self, messages: &[T]) -> anyhow::Result<()>;
}

pub struct StdoutJsonWritter<'a> {
    stdout: StdoutLock<'a>,
}

impl<'a> StdoutJsonWritter<'a> {
    pub fn new(stdout: StdoutLock<'a>) -> Self {
        Self { stdout }
    }
}

impl<T> MessageWritter<T> for StdoutJsonWritter<'_>
where
    T: Sized + Serialize + std::fmt::Debug,
{
    fn send_message(&mut self, message: &T) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.stdout, message).context("Error serializing response")?;

        self.stdout
            .write_all(b"\n")
            .context("Error writing response to stdout")?;

        Ok(())
    }

    fn send_messages(&mut self, messages: &[T]) -> anyhow::Result<()> {
        for message in messages {
            self.send_message(message)?
        }

        Ok(())
    }
}

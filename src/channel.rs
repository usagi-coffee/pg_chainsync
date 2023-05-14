use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use pgx::prelude::warning;

use crate::types::Message;

pub const MESSAGES_CAPACITY: usize = 1024;

pub type MessageStream = ReceiverStream<Message>;

pub struct Channel {
    pub sender: mpsc::Sender<Message>,
}

impl Channel {
    pub fn new(sender: mpsc::Sender<Message>) -> Self {
        Self { sender }
    }

    pub fn send(&self, message: Message) -> bool {
        match self.sender.try_send(message) {
            Ok(()) => true,
            Err(_) => {
                warning!("sync: channel: failed to send message");
                false
            }
        }
    }
}

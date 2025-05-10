use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use pgrx::prelude::warning;

use crate::types::Message;

pub const MESSAGES_CAPACITY: usize = 10_000_000;

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
            Err(e) => {
                warning!("sync: channel: failed to send message, {}", e);
                false
            }
        }
    }

    pub async fn wait_for_messages(&self) {
        loop {
            if self.sender.capacity() >= MESSAGES_CAPACITY {
                break;
            }

            tokio::task::yield_now().await;
        }
    }
}

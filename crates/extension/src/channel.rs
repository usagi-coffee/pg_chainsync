use pgrx::warning;
use tokio::sync::mpsc;

use crate::types::Message;

pub use channel::bounded;
pub use channel::ordered_channel;
pub use channel::unbounded;
pub use channel::unbounded_ordered_channel;

pub const MESSAGES_CAPACITY: usize = 10_000_000;

pub type MessageStream = channel::Stream<Message>;

pub struct Channel {
    inner: channel::Channel<Message>,
}

impl Channel {
    pub fn new(sender: mpsc::Sender<Message>) -> Self {
        Self {
            inner: channel::Channel::new(sender),
        }
    }

    pub fn send(&self, message: Message) -> bool {
        match self.inner.send(message) {
            Ok(()) => true,
            Err(e) => {
                warning!("sync: channel: failed to send message, {}", e);
                false
            }
        }
    }

    pub async fn wait_for_messages(&self) {
        self.inner.wait_for_messages(MESSAGES_CAPACITY).await;
    }
}

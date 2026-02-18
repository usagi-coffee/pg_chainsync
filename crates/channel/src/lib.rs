use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub mod bounded;
pub mod unbounded;

pub type Stream<T> = ReceiverStream<T>;

pub struct Channel<T> {
    sender: mpsc::Sender<T>,
}

impl<T> Channel<T> {
    pub fn new(sender: mpsc::Sender<T>) -> Self {
        Self { sender }
    }

    pub fn send(&self, message: T) -> Result<(), mpsc::error::TrySendError<T>> {
        self.sender.try_send(message)
    }

    pub async fn wait_for_messages(&self, capacity: usize) {
        loop {
            if self.sender.capacity() >= capacity {
                break;
            }

            tokio::task::yield_now().await;
        }
    }
}

pub fn ordered_channel<T>(
    buffer: usize,
) -> (bounded::OrderedSender<T>, bounded::OrderedReceiver<T>) {
    bounded::OrderedSender::new(buffer)
}

pub fn unbounded_ordered_channel<T>(
) -> (unbounded::OrderedSender<T>, unbounded::OrderedReceiver<T>) {
    unbounded::OrderedSender::new()
}

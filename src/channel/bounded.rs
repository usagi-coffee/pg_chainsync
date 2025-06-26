use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use std::collections::BTreeMap;

pub struct OrderedSender<T> {
    inner: mpsc::Sender<(u64, T)>,
    max_index: Arc<AtomicU64>,
}

pub struct OrderedReceiver<T> {
    inner: mpsc::Receiver<(u64, T)>,
    buffer: BTreeMap<u64, T>,
    next_index: u64,
    max_index: Arc<AtomicU64>,
}

impl<T> OrderedSender<T> {
    pub fn new(buffer: usize) -> (Self, OrderedReceiver<T>) {
        let (tx, rx) = mpsc::channel(buffer);
        let max_index = Arc::new(AtomicU64::new(u64::MAX));
        let receiver = OrderedReceiver {
            inner: rx,
            buffer: BTreeMap::new(),
            next_index: 0,
            max_index: max_index.clone(),
        };
        (
            Self {
                inner: tx,
                max_index,
            },
            receiver,
        )
    }

    pub async fn send(
        &self,
        index: u64,
        item: T,
    ) -> Result<(), mpsc::error::SendError<(u64, T)>> {
        self.inner.send((index, item)).await
    }

    pub fn close(&self, at: u64) {
        self.max_index.fetch_min(at, Ordering::Release);
    }

    pub fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            max_index: self.max_index.clone(),
        }
    }
}

impl<T> OrderedReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            if self.next_index >= self.max_index.load(Ordering::Acquire) {
                return None;
            }

            if let Some(item) = self.buffer.remove(&self.next_index) {
                self.next_index += 1;
                return Some(item);
            }

            match self.inner.recv().await {
                Some((idx, item)) => {
                    self.buffer.insert(idx, item);
                }
                None => {
                    if self.next_index >= self.max_index.load(Ordering::Acquire)
                    {
                        return None;
                    }

                    if let Some(item) = self.buffer.remove(&self.next_index) {
                        self.next_index += 1;
                        return Some(item);
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ordered_channel() {
        let (tx, mut rx) = OrderedSender::new(10);

        tx.send(2, "third").await.unwrap();
        tx.send(1, "second").await.unwrap();
        tx.send(0, "first").await.unwrap();

        assert_eq!(rx.recv().await, Some("first"));
        assert_eq!(rx.recv().await, Some("second"));
        assert_eq!(rx.recv().await, Some("third"));

        drop(tx);

        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn test_ordered_channel_drop() {
        let (tx, mut rx) = OrderedSender::new(10);

        tx.send(0, "first").await.unwrap();
        tx.send(1, "second").await.unwrap();

        drop(tx);

        assert_eq!(rx.recv().await, Some("first"));
        assert_eq!(rx.recv().await, Some("second"));
    }

    #[tokio::test]
    async fn test_ordered_channel_close() {
        let (tx, mut rx) = OrderedSender::new(10);

        tx.send(0, "first").await.unwrap();
        tx.send(1, "second").await.unwrap();
        tx.send(2, "third").await.unwrap();

        tx.close(2);
        drop(tx);

        assert_eq!(rx.recv().await, Some("first"));
        assert_eq!(rx.recv().await, Some("second"));
        assert_eq!(rx.recv().await, None);
    }
}

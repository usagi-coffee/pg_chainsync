use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

pub struct OrderedSender<T> {
    inner: mpsc::UnboundedSender<(u64, T)>,
    max_index: Arc<AtomicU64>,
}

pub struct OrderedReceiver<T> {
    inner: mpsc::UnboundedReceiver<(u64, T)>,
    buffer: BTreeMap<u64, T>,
    next_index: u64,
    max_index: Arc<AtomicU64>,
}

impl<T> OrderedSender<T> {
    pub fn new() -> (Self, OrderedReceiver<T>) {
        let (tx, rx) = mpsc::unbounded_channel();
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

    pub fn send(
        &self,
        index: u64,
        item: T,
    ) -> Result<(), mpsc::error::SendError<(u64, T)>> {
        self.inner.send((index, item))
    }

    pub fn close(&self, at: u64) {
        self.max_index.fetch_min(at, Ordering::Release);
    }

    pub fn closed(&self) -> bool {
        self.max_index.load(Ordering::Acquire) < u64::MAX
    }
}

impl<T> Clone for OrderedSender<T> {
    fn clone(&self) -> Self {
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

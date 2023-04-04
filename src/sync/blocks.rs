use pgx::log;
use pgx::prelude::*;

use std::sync::Arc;

use tokio_stream::StreamMap;

use ethers::prelude::*;

use super::{wait_for_messages, MessageSender};
use crate::types::*;

pub async fn listen(jobs: Arc<Vec<Job>>, send_message: MessageSender) {
    let mut map = StreamMap::new();

    for job in jobs.iter() {
        if job.job_type != JobType::Blocks || map.contains_key(&job.chain) {
            continue;
        }

        let stream = job.ws.as_ref().unwrap().subscribe_blocks().await.unwrap();

        map.insert(job.chain, stream);
    }

    log!("sync: blocks: started listening");

    while let Some(tick) = map.next().await {
        let (chain, block) = tick;

        let number = block.number.unwrap_or(U64::from(0));
        log!("sync: blocks: {}: found {}", chain, number);

        match send_message.try_send((chain, Message::Block(block))) {
            Ok(()) => {}
            Err(_) => {
                warning!("sync: blocks: {}: failed to send {}", chain, number)
            }
        }
    }

    // Wait until queue gets processed before exiting
    log!("sync: blocks: waiting for consumer to finish");
    wait_for_messages(send_message).await;
}

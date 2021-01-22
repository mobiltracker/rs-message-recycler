use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    sync::Arc,
    thread::sleep,
    time::Duration,
};

use crossbeam_channel::{Receiver, Sender};
use redis::Commands;
use serde::Serialize;

#[derive(Debug, Eq, Clone, PartialEq, Serialize)]
pub struct ScheduledMessage<T: Serialize> {
    msg: T,
    time_to_send: i64,
}

impl<T: Serialize> ScheduledMessage<T> {
    pub fn new(msg: T, send_in_seconds: i64) -> Self {
        let now = chrono::Utc::now().timestamp_millis();

        Self {
            time_to_send: now + (1000 * send_in_seconds),
            msg,
        }
    }
}

impl<T: Serialize + Eq> Ord for ScheduledMessage<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time_to_send.cmp(&other.time_to_send)
    }
}

impl<T: Serialize + Eq> PartialOrd for ScheduledMessage<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MessageRecycler<T: Eq + Serialize> {
    queue: BinaryHeap<Reverse<ScheduledMessage<T>>>,
    list_key: String,
    channel_in: Arc<Sender<ScheduledMessage<T>>>,
    channel_out: Arc<Receiver<ScheduledMessage<T>>>,
    redis_conn: redis::Connection,
}

impl<T> MessageRecycler<T>
where
    T: Serialize + Eq + Send + 'static,
{
    pub fn new(
        list_key: &str,
        redis_conn_str: &str,
    ) -> Result<MessageRecycler<T>, redis::RedisError> {
        let channel = crossbeam_channel::unbounded();
        let client = redis::Client::open(redis_conn_str)?;
        let conn = client.get_connection_with_timeout(Duration::from_secs(10))?;

        Ok(Self {
            queue: BinaryHeap::new(),
            list_key: list_key.to_owned(),
            channel_in: Arc::new(channel.0),
            channel_out: Arc::new(channel.1),
            redis_conn: conn,
        })
    }

    pub fn spawn(mut self) -> Arc<Sender<ScheduledMessage<T>>> {
        let channel_in = self.channel_in.clone();

        std::thread::spawn(move || loop {
            match self.channel_out.try_recv() {
                Ok(msg) => {
                    println!(
                        "scheduling for recycle {}",
                        serde_json::to_string(&msg).unwrap()
                    );
                    self.queue.push(Reverse(msg));
                }
                Err(err) => match err {
                    crossbeam_channel::TryRecvError::Empty => {}
                    crossbeam_channel::TryRecvError::Disconnected => {
                        eprintln!("Recycler closed");
                        break;
                    }
                },
            }

            while let Ok(Some(msg)) = self.recycle() {
                println!("Recycled {}", msg);
            }

            sleep(Duration::from_secs(1));
        });

        channel_in
    }

    fn recycle(&mut self) -> Result<Option<String>, redis::RedisError> {
        if let Some(Reverse(earliest_msg)) = self.queue.pop() {
            let now = chrono::Utc::now().timestamp_millis();

            if earliest_msg.time_to_send < now {
                let msg = serde_json::to_string(&earliest_msg.msg)
                    .expect("failed to serialize for recycling");

                let _: () = self.redis_conn.lpush(&self.list_key, &msg)?;

                return Ok(Some(msg));
            } else {
                self.queue.push(Reverse(earliest_msg));
            }
        }

        Ok(None)
    }
}

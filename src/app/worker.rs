use std::sync::mpsc;
use std::thread;

use anyhow::Result;

use crate::kafka::{ClusterConnection, FetchedMessage, TopicInfo};

pub enum WorkerMsg {
    Topics(Result<Vec<TopicInfo>>),
    Messages {
        topic: String,
        result: Result<Vec<FetchedMessage>>,
    },
}

pub fn spawn_list_topics(conn: ClusterConnection, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = conn.list_topics();
        let _ = tx.send(WorkerMsg::Topics(result));
    });
}

pub fn spawn_fetch_messages(
    conn: ClusterConnection,
    topic: String,
    partition: Option<i32>,
    limit: usize,
    from_end: bool,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let result = conn.fetch_messages(&topic, partition, limit, from_end);
        let _ = tx.send(WorkerMsg::Messages { topic, result });
    });
}

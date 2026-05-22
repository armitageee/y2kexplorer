use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;

use anyhow::Result;

use crate::config::ClusterConfig;
use crate::kafka::{ClusterConnection, FetchedMessage, TopicInfo};

pub enum WorkerMsg {
    Topics(Result<Vec<TopicInfo>>),
    Messages {
        topic: String,
        result: Result<Vec<FetchedMessage>>,
    },
    LiveMessages {
        topic: String,
        result: Result<Vec<FetchedMessage>>,
    },
    Op(Result<String>),
}

pub fn spawn_list_topics(conn: ClusterConnection, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = conn.list_topics();
        let _ = tx.send(WorkerMsg::Topics(result));
    });
}

pub fn spawn_poll_live_messages(
    cluster: ClusterConfig,
    topic: String,
    partition: Option<i32>,
    after_offsets: HashMap<i32, i64>,
    max_messages: usize,
    sort_by_time: bool,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let result = ClusterConnection::connect(&cluster).and_then(|conn| {
            conn.poll_new_messages(
                &topic,
                partition,
                &after_offsets,
                max_messages,
                sort_by_time,
            )
        });
        let _ = tx.send(WorkerMsg::LiveMessages { topic, result });
    });
}

pub fn spawn_fetch_messages(
    conn: ClusterConnection,
    topic: String,
    partition: Option<i32>,
    limit: usize,
    from_end: bool,
    sort_by_time: bool,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let result = conn.fetch_messages(&topic, partition, limit, from_end, sort_by_time);
        let _ = tx.send(WorkerMsg::Messages { topic, result });
    });
}

pub fn spawn_create_topic(
    conn: ClusterConnection,
    name: String,
    partitions: i32,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let result = conn
            .create_topic(&name, partitions)
            .map(|_| format!("created topic '{name}' ({partitions} partitions)"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_delete_topic(conn: ClusterConnection, name: String, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = conn
            .delete_topic(&name)
            .map(|_| format!("deleted topic '{name}'"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_produce(
    conn: ClusterConnection,
    topic: String,
    key: Option<String>,
    payload: String,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let key_ref = key.as_deref();
        let result = conn
            .produce_message(&topic, key_ref, &payload)
            .map(|_| format!("produced to '{topic}' ({} bytes)", payload.len()));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

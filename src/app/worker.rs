use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;

use anyhow::Result;

use crate::config::{ClusterConfig, KafkaConnectConfig, SchemaRegistryConfig};
use crate::kafka::{
    AclEntry, AclSpec, ClusterConnection, ConsumerGroupInfo, FetchedMessage, GroupOffset,
    ResetStrategy, TopicInfo,
};
use y2kexplorer::kafka_connect::{ConnectorDetail, ConnectorSummary, KafkaConnectClient};
use y2kexplorer::schema_registry::{
    SchemaRegistryClient, SchemaSubjectSummary, SchemaVersionDetail,
};

pub enum WorkerMsg {
    Topics(Result<Vec<TopicInfo>>),
    Messages {
        topic: String,
        result: Result<Vec<FetchedMessage>>,
    },
    MessagesProgress {
        topic: String,
        done: usize,
        total: usize,
    },
    LiveMessages {
        topic: String,
        result: Result<Vec<FetchedMessage>>,
    },
    Groups(Result<Vec<ConsumerGroupInfo>>),
    GroupOffsets {
        group: String,
        result: Result<(ConsumerGroupInfo, Vec<GroupOffset>)>,
    },
    Acls(Result<Vec<AclEntry>>),
    Schemas(Result<Vec<SchemaSubjectSummary>>),
    Connectors(Result<Vec<ConnectorSummary>>),
    ConnectorDetail {
        name: String,
        result: Result<ConnectorDetail>,
    },
    SchemaVersions {
        subject: String,
        result: Result<Vec<i32>>,
    },
    SchemaVersion {
        subject: String,
        version: i32,
        result: Result<SchemaVersionDetail>,
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
        let progress_topic = topic.clone();
        let progress_tx = tx.clone();
        let result = conn.fetch_messages_with_progress(
            &topic,
            partition,
            limit,
            from_end,
            sort_by_time,
            move |done, total| {
                let _ = progress_tx.send(WorkerMsg::MessagesProgress {
                    topic: progress_topic.clone(),
                    done,
                    total,
                });
            },
        );
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

pub fn spawn_list_groups(conn: ClusterConnection, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let _ = tx.send(WorkerMsg::Groups(conn.list_consumer_groups()));
    });
}

pub fn spawn_group_offsets(conn: ClusterConnection, group: String, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = conn
            .describe_group(&group)
            .and_then(|info| conn.group_offsets(&group).map(|offsets| (info, offsets)));
        let _ = tx.send(WorkerMsg::GroupOffsets { group, result });
    });
}

pub fn spawn_reset_group_offsets(
    cluster: ClusterConfig,
    group: String,
    strategy: ResetStrategy,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let result = ClusterConnection::connect(&cluster)
            .and_then(|conn| conn.reset_group_offsets(&group, &strategy))
            .map(|n| format!("reset offsets for '{group}' ({n} partitions)"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_delete_group(conn: ClusterConnection, group: String, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = conn
            .delete_consumer_group(&group)
            .map(|_| format!("deleted group '{group}'"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_list_acls(conn: ClusterConnection, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let _ = tx.send(WorkerMsg::Acls(conn.list_acls()));
    });
}

pub fn spawn_create_acl(conn: ClusterConnection, spec: AclSpec, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = conn
            .create_acl(&spec)
            .map(|_| format!("created ACL for {}", spec.principal));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_delete_acl(conn: ClusterConnection, spec: AclSpec, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = conn
            .delete_acl(&spec)
            .map(|n| format!("deleted {n} ACL(s)"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_replace_acl(
    conn: ClusterConnection,
    old: AclSpec,
    new: AclSpec,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let result = conn
            .replace_acl(&old, &new)
            .map(|_| format!("updated ACL for {}", new.principal));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_list_connectors(cfg: KafkaConnectConfig, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = KafkaConnectClient::new(&cfg).and_then(|c| c.list_summaries());
        let _ = tx.send(WorkerMsg::Connectors(result));
    });
}

pub fn spawn_connector_detail(cfg: KafkaConnectConfig, name: String, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = KafkaConnectClient::new(&cfg).and_then(|c| c.get_detail(&name));
        let _ = tx.send(WorkerMsg::ConnectorDetail { name, result });
    });
}

pub fn spawn_connect_restart(cfg: KafkaConnectConfig, name: String, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = KafkaConnectClient::new(&cfg)
            .and_then(|c| c.restart(&name))
            .map(|_| format!("restarted connector '{name}'"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_connect_pause(cfg: KafkaConnectConfig, name: String, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = KafkaConnectClient::new(&cfg)
            .and_then(|c| c.pause(&name))
            .map(|_| format!("paused connector '{name}'"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_connect_resume(cfg: KafkaConnectConfig, name: String, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = KafkaConnectClient::new(&cfg)
            .and_then(|c| c.resume(&name))
            .map(|_| format!("resumed connector '{name}'"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_connect_delete(cfg: KafkaConnectConfig, name: String, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = KafkaConnectClient::new(&cfg)
            .and_then(|c| c.delete(&name))
            .map(|_| format!("deleted connector '{name}'"));
        let _ = tx.send(WorkerMsg::Op(result));
    });
}

pub fn spawn_list_schemas(cfg: SchemaRegistryConfig, tx: mpsc::Sender<WorkerMsg>) {
    thread::spawn(move || {
        let result = SchemaRegistryClient::new(&cfg).and_then(|c| c.list_summaries());
        let _ = tx.send(WorkerMsg::Schemas(result));
    });
}

pub fn spawn_schema_versions(
    cfg: SchemaRegistryConfig,
    subject: String,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let result = SchemaRegistryClient::new(&cfg)
            .and_then(|c| c.list_versions(&subject))
            .map(|mut v| {
                v.sort_unstable();
                v
            });
        let _ = tx.send(WorkerMsg::SchemaVersions { subject, result });
    });
}

pub fn spawn_schema_version(
    cfg: SchemaRegistryConfig,
    subject: String,
    version: i32,
    tx: mpsc::Sender<WorkerMsg>,
) {
    thread::spawn(move || {
        let result = SchemaRegistryClient::new(&cfg).and_then(|c| c.get_version(&subject, version));
        let _ = tx.send(WorkerMsg::SchemaVersion {
            subject,
            version,
            result,
        });
    });
}

use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};

use crate::config::{AuthConfig, ClusterConfig, ScramMechanism};

#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: usize,
    pub replication: usize,
    pub internal: bool,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupInfo {
    pub id: String,
    pub state: String,
    pub members: usize,
}

#[derive(Debug, Clone)]
pub struct FetchedMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<String>,
    pub payload: Option<String>,
    pub headers: Vec<(String, String)>,
}

pub struct ClusterConnection {
    admin: AdminClient<DefaultClientContext>,
    cluster: ClusterConfig,
}

impl ClusterConnection {
    pub fn connect(cluster: &ClusterConfig) -> Result<Self> {
        let admin: AdminClient<DefaultClientContext> = base_config(cluster)
            .create()
            .context("create kafka admin client")?;
        Ok(Self {
            admin,
            cluster: cluster.clone(),
        })
    }

    pub fn reconnect(&self) -> Result<Self> {
        Self::connect(&self.cluster)
    }

    pub fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let metadata = self
            .admin
            .inner()
            .fetch_metadata(None, Timeout::After(Duration::from_secs(10)))
            .context("fetch cluster metadata")?;

        let mut topics: Vec<TopicInfo> = metadata
            .topics()
            .iter()
            .filter(|t| !t.name().is_empty())
            .map(|t| {
                let partitions = t.partitions();
                let replication = partitions
                    .first()
                    .map(|p| p.replicas().len())
                    .unwrap_or(0);
                TopicInfo {
                    name: t.name().to_string(),
                    partitions: partitions.len(),
                    replication,
                    internal: t.name().starts_with('_'),
                }
            })
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    pub fn topic_partitions(&self, topic: &str) -> Result<Vec<PartitionInfo>> {
        let metadata = self.admin.inner().fetch_metadata(
            Some(topic),
            Timeout::After(Duration::from_secs(10)),
        )?;

        let meta_topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .with_context(|| format!("topic '{topic}' not found"))?;

        Ok(meta_topic
            .partitions()
            .iter()
            .map(|p| PartitionInfo {
                id: p.id(),
                leader: p.leader(),
                replicas: p.replicas().to_vec(),
                isr: p.isr().to_vec(),
            })
            .collect())
    }

    pub fn fetch_messages(
        &self,
        topic: &str,
        partition: Option<i32>,
        limit: usize,
        from_end: bool,
    ) -> Result<Vec<FetchedMessage>> {
        let consumer: BaseConsumer = base_config(&self.cluster)
            .set("group.id", format!("y2kexplorer-{}", std::process::id()))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .create()
            .context("create kafka consumer")?;

        let metadata = consumer
            .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(10)))?;
        let meta_topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .with_context(|| format!("topic '{topic}' not found"))?;

        let mut tpl = TopicPartitionList::new();
        for p in meta_topic.partitions() {
            if partition.is_some_and(|sel| sel != p.id()) {
                continue;
            }
            let (low, high) = consumer
                .fetch_watermarks(topic, p.id(), Timeout::After(Duration::from_secs(5)))?;
            let offset = if from_end {
                Offset::Offset(high.saturating_sub(limit as i64))
            } else {
                Offset::Offset(low)
            };
            tpl.add_partition_offset(topic, p.id(), offset)?;
        }

        consumer.assign(&tpl)?;

        let mut out = Vec::with_capacity(limit);
        let deadline = std::time::Instant::now() + Duration::from_secs(15);

        while out.len() < limit && std::time::Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(200)) {
                None => continue,
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(m)) => {
                    if m.topic().is_empty() {
                        continue;
                    }
                    let headers = message_headers(&m);
                    out.push(FetchedMessage {
                        partition: m.partition(),
                        offset: m.offset(),
                        timestamp_ms: m.timestamp().to_millis(),
                        key: m
                            .key()
                            .map(|k| String::from_utf8_lossy(k).into_owned()),
                        payload: m
                            .payload()
                            .map(|p| String::from_utf8_lossy(p).into_owned()),
                        headers,
                    });
                }
            }
        }

        Ok(out)
    }
}

fn message_headers<M: Message>(m: &M) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(hdrs) = m.headers() {
        for i in 0..hdrs.count() {
            let h = hdrs.get(i);
            let key = h.key.to_string();
            let val = h
                .value
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default();
            out.push((key, val));
        }
    }
    out
}

fn base_config(cluster: &ClusterConfig) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", cluster.brokers.join(","));
    if let Some(id) = &cluster.client_id {
        cfg.set("client.id", id);
    }
    cfg.set("socket.timeout.ms", "10000");
    cfg.set("api.version.request", "true");

    match &cluster.auth {
        AuthConfig::None => {}
        AuthConfig::SaslPlain { username, password, tls } => {
            apply_tls(&mut cfg, *tls);
            cfg.set("security.protocol", if *tls { "SASL_SSL" } else { "SASL_PLAINTEXT" });
            cfg.set("sasl.mechanism", "PLAIN");
            cfg.set("sasl.username", username);
            cfg.set("sasl.password", password);
        }
        AuthConfig::SaslScram {
            username,
            password,
            mechanism,
            tls,
        } => {
            apply_tls(&mut cfg, *tls);
            cfg.set("security.protocol", if *tls { "SASL_SSL" } else { "SASL_PLAINTEXT" });
            let mech = match mechanism {
                ScramMechanism::ScramSha256 => "SCRAM-SHA-256",
                ScramMechanism::ScramSha512 => "SCRAM-SHA-512",
            };
            cfg.set("sasl.mechanism", mech);
            cfg.set("sasl.username", username);
            cfg.set("sasl.password", password);
        }
        AuthConfig::Ssl {
            ca_location,
            certificate_location,
            key_location,
            key_password,
        } => {
            cfg.set("security.protocol", "SSL");
            if let Some(ca) = ca_location {
                cfg.set("ssl.ca.location", ca);
            }
            if let Some(cert) = certificate_location {
                cfg.set("ssl.certificate.location", cert);
            }
            if let Some(key) = key_location {
                cfg.set("ssl.key.location", key);
            }
            if let Some(pw) = key_password {
                cfg.set("ssl.key.password", pw);
            }
        }
        AuthConfig::Kerberos {
            keytab,
            principal,
            service_name,
            tls,
        } => {
            apply_tls(&mut cfg, *tls);
            cfg.set("security.protocol", if *tls { "SASL_SSL" } else { "SASL_PLAINTEXT" });
            cfg.set("sasl.mechanism", "GSSAPI");
            cfg.set("sasl.kerberos.keytab", keytab.display().to_string());
            cfg.set("sasl.kerberos.principal", principal);
            cfg.set("sasl.kerberos.service.name", service_name);
        }
    }

    cfg
}

fn apply_tls(cfg: &mut ClientConfig, tls: bool) {
    if tls {
        cfg.set("ssl.endpoint.identification.algorithm", "https");
    }
}

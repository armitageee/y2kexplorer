use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};

use crate::config::{
    apply_kerberos_env, resolve_kerberos_ssl_ca, AuthConfig, ClusterConfig, ScramMechanism,
};

#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: usize,
    pub replication: usize,
    pub message_count: u64,
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
        apply_kerberos_env(&cluster.auth);
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

        let consumer: BaseConsumer = base_config(&self.cluster)
            .set("group.id", format!("y2kexplorer-meta-{}", std::process::id()))
            .create()
            .context("create metadata consumer")?;

        let timeout = Timeout::After(Duration::from_secs(5));
        let mut topics: Vec<TopicInfo> = metadata
            .topics()
            .iter()
            .filter(|t| !t.name().is_empty())
            .map(|t| {
                let name = t.name();
                let parts = t.partitions();
                let replication = parts.first().map(|p| p.replicas().len()).unwrap_or(0);
                let message_count = parts
                    .iter()
                    .filter_map(|p| {
                        consumer
                            .fetch_watermarks(name, p.id(), timeout)
                            .ok()
                            .map(|(low, high)| (high - low).max(0) as u64)
                    })
                    .sum();
                TopicInfo {
                    name: name.to_string(),
                    partitions: parts.len(),
                    replication,
                    message_count,
                    internal: name.starts_with('_'),
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
            .set("log_level", "4") // warning+ — меньше шума от PartitionEOF
            .create()
            .context("create kafka consumer")?;

        let metadata = consumer
            .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(10)))?;
        let meta_topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .with_context(|| format!("topic '{topic}' not found"))?;

        let partition_ids: Vec<i32> = meta_topic
            .partitions()
            .iter()
            .map(|p| p.id())
            .filter(|id| partition.is_none_or(|sel| sel == *id))
            .collect();

        if partition_ids.is_empty() {
            return Ok(Vec::new());
        }

        let per_partition = (limit / partition_ids.len()).max(1);
        let timeout = Timeout::After(Duration::from_secs(5));
        let mut out = Vec::with_capacity(limit);

        for part_id in partition_ids {
            if out.len() >= limit {
                break;
            }
            let (low, high) = consumer.fetch_watermarks(topic, part_id, timeout)?;
            // high — следующий offset для записи; сообщения лежат в [low, high)
            if high <= low {
                continue;
            }

            let available = (high - low) as usize;
            let want = per_partition.min(available).min(limit - out.len());
            let start = if from_end {
                (high - want as i64).max(low)
            } else {
                low
            };

            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(topic, part_id, Offset::Offset(start))?;
            consumer.assign(&tpl)?;

            let mut got = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while got < want && out.len() < limit && std::time::Instant::now() < deadline {
                match consumer.poll(Duration::from_millis(300)) {
                    None => continue,
                    Some(Err(e)) => {
                        if e.rdkafka_error_code() == Some(RDKafkaErrorCode::PartitionEOF) {
                            break;
                        }
                        return Err(e.into());
                    }
                    Some(Ok(m)) => {
                        if m.offset() < low || m.offset() >= high {
                            break;
                        }
                        got += 1;
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
        }

        out.sort_by(|a, b| (a.partition, a.offset).cmp(&(b.partition, b.offset)));
        out.truncate(limit);
        Ok(out)
    }

    pub fn create_topic(&self, name: &str, partitions: i32) -> Result<()> {
        let topic = NewTopic::new(name, partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30))));
        let results = block_on(self.admin.create_topics(&[topic], &opts))?;
        for r in results {
            r.map_err(|(name, code)| anyhow::anyhow!("create topic '{name}': {code:?}"))?;
        }
        Ok(())
    }

    pub fn delete_topic(&self, name: &str) -> Result<()> {
        let opts = AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30))));
        let results = block_on(self.admin.delete_topics(&[name], &opts))?;
        for r in results {
            r.map_err(|(name, code)| anyhow::anyhow!("delete topic '{name}': {code:?}"))?;
        }
        Ok(())
    }

    pub fn produce_message(&self, topic: &str, key: Option<&str>, payload: &str) -> Result<()> {
        let producer: BaseProducer = base_config(&self.cluster)
            .set("message.timeout.ms", "10000")
            .create()
            .context("create kafka producer")?;

        let mut record = BaseRecord::to(topic).payload(payload);
        if let Some(k) = key.filter(|s| !s.is_empty()) {
            record = record.key(k);
        }
        producer
            .send(record)
            .map_err(|(e, _)| e)
            .context("produce message")?;
        producer
            .flush(Timeout::After(Duration::from_secs(10)))
            .context("flush producer")?;
        Ok(())
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
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
    cfg.set("socket.timeout.ms", "30000");
    cfg.set("socket.connection.setup.timeout.ms", "30000");
    cfg.set("api.version.request", "true");

    match &cluster.auth {
        AuthConfig::None => {}
        AuthConfig::SaslPlain { username, password, tls } => {
            apply_tls(&mut cfg, *tls, None, true);
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
            apply_tls(&mut cfg, *tls, None, true);
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
            tls_verify_hostname,
            krb5_conf: _,
            ..
        } => {
            let ca = resolve_kerberos_ssl_ca(&cluster.auth);
            apply_tls(
                &mut cfg,
                *tls,
                ca.as_deref(),
                *tls_verify_hostname,
            );
            cfg.set("security.protocol", if *tls { "SASL_SSL" } else { "SASL_PLAINTEXT" });
            cfg.set("sasl.mechanism", "GSSAPI");
            cfg.set("sasl.kerberos.keytab", keytab.display().to_string());
            cfg.set("sasl.kerberos.principal", principal);
            cfg.set("sasl.kerberos.service.name", service_name);
            // Ticket из keytab (без -R renew из чужого ccache, как в дефолте librdkafka)
            cfg.set(
                "sasl.kerberos.kinit.cmd",
                "kinit -t \"%{sasl.kerberos.keytab}\" -k %{sasl.kerberos.principal}",
            );
            // 0..86400000 с; раз в сутки обновлять ticket из keytab
            cfg.set("sasl.kerberos.min.time.before.relogin", "86400");
        }
    }

    cfg
}

fn apply_tls(
    cfg: &mut ClientConfig,
    tls: bool,
    ca_location: Option<&str>,
    verify_hostname: bool,
) {
    if !tls {
        return;
    }
    if verify_hostname {
        cfg.set("ssl.endpoint.identification.algorithm", "https");
    } else {
        cfg.set("ssl.endpoint.identification.algorithm", "none");
    }
    if let Some(ca) = ca_location {
        if std::path::Path::new(ca).exists() {
            cfg.set("ssl.ca.location", ca);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthConfig;

    #[test]
    fn fetch_messages_from_local_orders() {
        let cluster = ClusterConfig {
            brokers: vec!["localhost:9092".into()],
            auth: AuthConfig::None,
            client_id: Some("y2kexplorer-test".into()),
        };
        let conn = ClusterConnection::connect(&cluster).expect("connect");
        let msgs = conn
            .fetch_messages("orders", None, 10, true)
            .expect("fetch tail");
        assert!(
            !msgs.is_empty(),
            "expected messages in orders, got none"
        );
    }
}

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
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
    pub protocol: String,
    pub protocol_type: String,
}

impl ConsumerGroupInfo {
    /// Reset/delete операции допустимы только когда у группы нет активных членов:
    /// иначе брокер вернёт REBALANCE_IN_PROGRESS / UNKNOWN_MEMBER_ID.
    pub fn is_empty_or_dead(&self) -> bool {
        let s = self.state.as_str();
        s == "Empty" || s == "Dead"
    }
}

/// Один (topic, partition) commit-оффсет для consumer-group + LEO/lag.
#[derive(Debug, Clone)]
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    /// `None` — у группы нет коммита для этой партиции.
    pub current_offset: Option<i64>,
    pub log_end_offset: i64,
    pub lag: i64,
}

/// Куда сдвинуть оффсеты при reset.
#[derive(Debug, Clone)]
pub enum ResetStrategy {
    Earliest,
    Latest,
    /// Абсолютный оффсет (применяется ко всем партициям всех топиков, на которые
    /// есть коммит у группы). Если значение вне `[low, high]` — клампится.
    ToOffset(i64),
    /// Unix-миллисекунды. Преобразуется в оффсеты через `offsets_for_times`.
    ToTimestamp(i64),
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

/// Максимум новых сообщений за один live-tick (защита от всплеска).
pub const LIVE_MAX_PER_POLL: usize = 150;

/// Опции загрузки списка топиков. По умолчанию — fetch включён, параллелизм 16.
#[derive(Debug, Clone, Copy)]
pub struct ListTopicsOptions {
    pub fetch_watermarks: bool,
    pub parallelism: usize,
}

impl Default for ListTopicsOptions {
    fn default() -> Self {
        Self {
            fetch_watermarks: true,
            parallelism: 16,
        }
    }
}

pub struct ClusterConnection {
    admin: AdminClient<DefaultClientContext>,
    cluster: ClusterConfig,
    list_topics_opts: ListTopicsOptions,
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
            list_topics_opts: ListTopicsOptions::default(),
        })
    }

    pub fn with_list_topics_options(mut self, opts: ListTopicsOptions) -> Self {
        self.list_topics_opts = opts;
        self
    }

    pub fn reconnect(&self) -> Result<Self> {
        Ok(Self::connect(&self.cluster)?.with_list_topics_options(self.list_topics_opts))
    }

    /// Список топиков с метаданными. Самая дорогая часть — высчитывание `message_count`
    /// (high–low watermark per partition). Делается параллельно: `parallelism`
    /// потоков делят все (topic, partition) пары между собой и атомарно агрегируют
    /// per-topic. Если `fetch_watermarks=false` — message_count=0, но загрузка
    /// мгновенная (только metadata-запрос).
    ///
    /// Бенч на реальном кластере (84 топика, 720 партиций, Kerberos+TLS):
    /// sequential → 103s, parallel(16) → 6.4s = 16× speedup.
    pub fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let metadata = self
            .admin
            .inner()
            .fetch_metadata(None, Timeout::After(Duration::from_secs(10)))
            .context("fetch cluster metadata")?;

        // Снимаем нужные данные из metadata в Vec, чтобы не держать borrow на metadata
        // в spawned-потоках.
        struct TopicMeta {
            name: String,
            partitions: Vec<i32>,
            replication: usize,
        }
        let topics_meta: Vec<TopicMeta> = metadata
            .topics()
            .iter()
            .filter(|t| !t.name().is_empty())
            .map(|t| {
                let parts = t.partitions();
                let replication = parts.first().map(|p| p.replicas().len()).unwrap_or(0);
                TopicMeta {
                    name: t.name().to_string(),
                    partitions: parts.iter().map(|p| p.id()).collect(),
                    replication,
                }
            })
            .collect();

        // Если watermarks не нужны — выходим сразу с message_count=0.
        if !self.list_topics_opts.fetch_watermarks || topics_meta.is_empty() {
            let mut topics: Vec<TopicInfo> = topics_meta
                .into_iter()
                .map(|t| TopicInfo {
                    internal: t.name.starts_with('_'),
                    partitions: t.partitions.len(),
                    replication: t.replication,
                    message_count: 0,
                    name: t.name,
                })
                .collect();
            topics.sort_by(|a, b| a.name.cmp(&b.name));
            return Ok(topics);
        }

        // Один consumer шарится между потоками — BaseConsumer: Send + Sync,
        // librdkafka сериализует API-вызовы внутри C-уровня, но запросы на брокер
        // пайплайнятся (так что параллелизм даёт реальный speedup).
        let consumer: BaseConsumer = base_config(&self.cluster)
            .set(
                "group.id",
                format!("y2kexplorer-meta-{}", std::process::id()),
            )
            .create()
            .context("create metadata consumer")?;
        let timeout = Timeout::After(Duration::from_secs(5));

        // Per-topic atomic счётчик — потоки fetch_add'ят сюда без mutex contention.
        let counts: Vec<AtomicU64> = (0..topics_meta.len()).map(|_| AtomicU64::new(0)).collect();

        // Плоский job-list: (topic_idx, &name, partition_id). &str живёт ровно столько
        // же, сколько topics_meta, что охватывается thread::scope.
        let jobs: Vec<(usize, &str, i32)> = topics_meta
            .iter()
            .enumerate()
            .flat_map(|(i, t)| t.partitions.iter().map(move |&p| (i, t.name.as_str(), p)))
            .collect();

        let par = self.list_topics_opts.parallelism.clamp(1, 64);
        let chunk_size = jobs.len().div_ceil(par).max(1);

        thread::scope(|s| {
            for chunk in jobs.chunks(chunk_size) {
                let consumer = &consumer;
                let counts = &counts;
                s.spawn(move || {
                    for &(idx, name, pid) in chunk {
                        if let Ok((low, high)) = consumer.fetch_watermarks(name, pid, timeout) {
                            let v = (high - low).max(0) as u64;
                            counts[idx].fetch_add(v, Ordering::Relaxed);
                        }
                    }
                });
            }
        });

        let mut topics: Vec<TopicInfo> = topics_meta
            .into_iter()
            .enumerate()
            .map(|(i, t)| TopicInfo {
                internal: t.name.starts_with('_'),
                partitions: t.partitions.len(),
                replication: t.replication,
                message_count: counts[i].load(Ordering::Relaxed),
                name: t.name,
            })
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    pub fn topic_partitions(&self, topic: &str) -> Result<Vec<PartitionInfo>> {
        let metadata = self
            .admin
            .inner()
            .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(10)))?;

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
        sort_by_time: bool,
    ) -> Result<Vec<FetchedMessage>> {
        self.fetch_messages_with_progress(
            topic,
            partition,
            limit,
            from_end,
            sort_by_time,
            |_d, _t| {},
        )
    }

    pub fn fetch_messages_with_progress<F>(
        &self,
        topic: &str,
        partition: Option<i32>,
        limit: usize,
        from_end: bool,
        sort_by_time: bool,
        mut on_progress: F,
    ) -> Result<Vec<FetchedMessage>>
    where
        F: FnMut(usize, usize),
    {
        let consumer: BaseConsumer = consumer_config(&self.cluster, "y2kexplorer")
            .create()
            .context("create kafka consumer")?;

        let metadata =
            consumer.fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(10)))?;
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

        let multi_partition = partition_ids.len() > 1;
        let per_partition = (limit / partition_ids.len()).max(1);
        let total_parts = partition_ids.len();
        let workers = total_parts.min(8);
        let chunk_size = total_parts.div_ceil(workers).max(1);
        let topic = topic.to_string();
        let cluster = self.cluster.clone();
        let (tx, rx) = mpsc::channel();

        thread::scope(|scope| {
            for chunk in partition_ids.chunks(chunk_size) {
                let tx = tx.clone();
                let cluster = cluster.clone();
                let topic = topic.clone();
                let chunk = chunk.to_vec();
                scope.spawn(move || {
                    let consumer: BaseConsumer =
                        match consumer_config(&cluster, "y2kexplorer-fetch").create() {
                            Ok(c) => c,
                            Err(e) => {
                                let err = anyhow::anyhow!("create kafka consumer: {e}");
                                for part in chunk {
                                    let _ = tx.send((part, Err(anyhow::anyhow!("{err}"))));
                                }
                                return;
                            }
                        };
                    for part_id in chunk {
                        let res = fetch_partition_messages(
                            &consumer,
                            &topic,
                            part_id,
                            per_partition,
                            from_end,
                        );
                        let _ = tx.send((part_id, res));
                    }
                });
            }
        });
        drop(tx);

        let mut out = Vec::with_capacity(limit);
        for done in 1..=total_parts {
            let (_, part_result) = rx.recv().context("messages worker channel closed")?;
            on_progress(done, total_parts);
            match part_result {
                Ok(mut part_msgs) => out.append(&mut part_msgs),
                Err(e) => return Err(e),
            }
        }

        sort_fetched(
            &mut out,
            partition.is_some() || !multi_partition,
            sort_by_time,
        );
        out.truncate(limit);
        Ok(out)
    }

    /// Только сообщения с offset >= `after_offsets[partition]` (инкрементальный live-poll).
    pub fn poll_new_messages(
        &self,
        topic: &str,
        partition: Option<i32>,
        after_offsets: &HashMap<i32, i64>,
        max_messages: usize,
        sort_by_time: bool,
    ) -> Result<Vec<FetchedMessage>> {
        let cap = max_messages.min(LIVE_MAX_PER_POLL);
        if cap == 0 || after_offsets.is_empty() {
            return Ok(Vec::new());
        }

        let consumer: BaseConsumer = consumer_config(&self.cluster, "y2kexplorer-live")
            .create()
            .context("create live consumer")?;

        let metadata =
            consumer.fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(5)))?;
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
            .filter(|id| after_offsets.contains_key(id))
            .collect();

        if partition_ids.is_empty() {
            return Ok(Vec::new());
        }

        let multi_partition = partition_ids.len() > 1;
        let per_part = (cap / partition_ids.len()).max(1);
        let wm_timeout = Timeout::After(Duration::from_secs(3));
        let mut out = Vec::with_capacity(cap);

        for part_id in partition_ids {
            if out.len() >= cap {
                break;
            }
            let Some(&start) = after_offsets.get(&part_id) else {
                continue;
            };
            let (_low, high) = consumer.fetch_watermarks(topic, part_id, wm_timeout)?;
            if start >= high {
                continue;
            }

            let available = (high - start) as usize;
            let want = per_part.min(available).min(cap - out.len());
            if want == 0 {
                continue;
            }

            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(topic, part_id, Offset::Offset(start))?;
            consumer.assign(&tpl)?;

            let mut got = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while got < want && out.len() < cap && std::time::Instant::now() < deadline {
                match consumer.poll(Duration::from_millis(200)) {
                    None => continue,
                    Some(Err(e)) => {
                        if e.rdkafka_error_code() == Some(RDKafkaErrorCode::PartitionEOF) {
                            break;
                        }
                        return Err(e.into());
                    }
                    Some(Ok(m)) => {
                        if m.offset() < start || m.offset() >= high {
                            break;
                        }
                        got += 1;
                        out.push(message_to_fetched(&m));
                    }
                }
            }
        }

        sort_fetched(
            &mut out,
            partition.is_some() || !multi_partition,
            sort_by_time,
        );
        Ok(out)
    }

    pub fn create_topic(&self, name: &str, partitions: i32) -> Result<()> {
        let topic = NewTopic::new(name, partitions, TopicReplication::Fixed(1));
        let opts =
            AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30))));
        let results = block_on(self.admin.create_topics(&[topic], &opts))?;
        for r in results {
            r.map_err(|(name, code)| anyhow::anyhow!("create topic '{name}': {code:?}"))?;
        }
        Ok(())
    }

    pub fn delete_topic(&self, name: &str) -> Result<()> {
        let opts =
            AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30))));
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

    /// Список всех consumer-групп кластера.
    pub fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>> {
        let consumer: BaseConsumer = base_config(&self.cluster)
            .set(
                "group.id",
                format!("y2kexplorer-grouplist-{}", std::process::id()),
            )
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("log_level", "0")
            .create()
            .context("create groups-meta consumer")?;
        let gl = consumer
            .fetch_group_list(None, Timeout::After(Duration::from_secs(15)))
            .context("fetch group list")?;
        let mut out: Vec<ConsumerGroupInfo> = gl
            .groups()
            .iter()
            .map(|g| ConsumerGroupInfo {
                id: g.name().to_string(),
                state: g.state().to_string(),
                members: g.members().len(),
                protocol: g.protocol().to_string(),
                protocol_type: g.protocol_type().to_string(),
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    /// Метаданные одной группы (state, members, protocol).
    pub fn describe_group(&self, group: &str) -> Result<ConsumerGroupInfo> {
        let consumer: BaseConsumer = base_config(&self.cluster)
            .set(
                "group.id",
                format!("y2kexplorer-describe-{}", std::process::id()),
            )
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("log_level", "0")
            .create()
            .context("create describe consumer")?;
        let gl = consumer
            .fetch_group_list(Some(group), Timeout::After(Duration::from_secs(10)))
            .context("describe group")?;
        let g = gl
            .groups()
            .iter()
            .find(|g| g.name() == group)
            .with_context(|| format!("group '{group}' not found"))?;
        Ok(ConsumerGroupInfo {
            id: g.name().to_string(),
            state: g.state().to_string(),
            members: g.members().len(),
            protocol: g.protocol().to_string(),
            protocol_type: g.protocol_type().to_string(),
        })
    }

    /// Коммит-оффсеты группы по всем (topic, partition) с lag = LEO − committed.
    /// Партиции без коммита у этой группы (`Offset::Invalid`) отбрасываются.
    pub fn group_offsets(&self, group: &str) -> Result<Vec<GroupOffset>> {
        let metadata = self
            .admin
            .inner()
            .fetch_metadata(None, Timeout::After(Duration::from_secs(10)))
            .context("fetch cluster metadata")?;

        let mut tpl = TopicPartitionList::new();
        for t in metadata.topics() {
            let name = t.name();
            if name.is_empty() {
                continue;
            }
            for p in t.partitions() {
                tpl.add_partition_offset(name, p.id(), Offset::Invalid)?;
            }
        }
        if tpl.count() == 0 {
            return Ok(Vec::new());
        }

        let consumer: BaseConsumer = base_config(&self.cluster)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("log_level", "0")
            .create()
            .context("create offsets consumer")?;
        let committed = consumer
            .committed_offsets(tpl, Timeout::After(Duration::from_secs(15)))
            .context("fetch committed offsets")?;

        let timeout = Timeout::After(Duration::from_secs(5));
        let mut out: Vec<GroupOffset> = Vec::new();
        for elem in committed.elements() {
            let off = match elem.offset() {
                Offset::Offset(n) => Some(n),
                _ => None,
            };
            if off.is_none() {
                continue;
            }
            let topic = elem.topic().to_string();
            let part = elem.partition();
            let (_low, high) = consumer
                .fetch_watermarks(&topic, part, timeout)
                .with_context(|| format!("watermarks {topic}-{part}"))?;
            let current = off.unwrap_or(0);
            let lag = (high - current).max(0);
            out.push(GroupOffset {
                topic,
                partition: part,
                current_offset: off,
                log_end_offset: high,
                lag,
            });
        }
        out.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
        Ok(out)
    }

    /// Сброс / сдвиг оффсетов всех (topic, partition), на которые группа коммитила.
    /// Группа должна быть в состоянии Empty/Dead — иначе брокер откажет.
    pub fn reset_group_offsets(&self, group: &str, strategy: &ResetStrategy) -> Result<usize> {
        // Pre-flight: лучше дать понятное сообщение, чем брокерскую ошибку
        // вида "REBALANCE_IN_PROGRESS".
        let info = self.describe_group(group)?;
        if !info.is_empty_or_dead() {
            anyhow::bail!(
                "group '{group}' is in state '{state}'; stop all consumers (group must be Empty or Dead)",
                state = info.state
            );
        }

        let current = self.group_offsets(group)?;
        if current.is_empty() {
            anyhow::bail!("group '{group}' has no committed offsets to reset");
        }

        let consumer: BaseConsumer = base_config(&self.cluster)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("log_level", "0")
            .create()
            .context("create reset consumer")?;

        let wm_timeout = Timeout::After(Duration::from_secs(5));
        let mut tpl = TopicPartitionList::new();
        let count = current.len();

        match strategy {
            ResetStrategy::Earliest => {
                for o in &current {
                    let (low, _high) =
                        consumer.fetch_watermarks(&o.topic, o.partition, wm_timeout)?;
                    tpl.add_partition_offset(&o.topic, o.partition, Offset::Offset(low))?;
                }
            }
            ResetStrategy::Latest => {
                for o in &current {
                    let (_low, high) =
                        consumer.fetch_watermarks(&o.topic, o.partition, wm_timeout)?;
                    tpl.add_partition_offset(&o.topic, o.partition, Offset::Offset(high))?;
                }
            }
            ResetStrategy::ToOffset(target) => {
                for o in &current {
                    let (low, high) =
                        consumer.fetch_watermarks(&o.topic, o.partition, wm_timeout)?;
                    let clamped = (*target).clamp(low, high);
                    tpl.add_partition_offset(&o.topic, o.partition, Offset::Offset(clamped))?;
                }
            }
            ResetStrategy::ToTimestamp(ts_ms) => {
                let mut request = TopicPartitionList::new();
                for o in &current {
                    request.add_partition_offset(&o.topic, o.partition, Offset::Offset(*ts_ms))?;
                }
                let resolved = consumer
                    .offsets_for_times(request, Timeout::After(Duration::from_secs(15)))
                    .context("offsets_for_times")?;
                for elem in resolved.elements() {
                    let off = match elem.offset() {
                        Offset::Offset(n) => n,
                        // нет сообщений после ts — ставим LEO (новые consumer-ы начнут с конца)
                        _ => {
                            let (_low, high) = consumer.fetch_watermarks(
                                elem.topic(),
                                elem.partition(),
                                wm_timeout,
                            )?;
                            high
                        }
                    };
                    tpl.add_partition_offset(elem.topic(), elem.partition(), Offset::Offset(off))?;
                }
            }
        }

        consumer
            .commit(&tpl, CommitMode::Sync)
            .context("commit reset offsets")?;
        Ok(count)
    }

    pub fn delete_consumer_group(&self, group: &str) -> Result<()> {
        let opts =
            AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30))));
        let results = block_on(self.admin.delete_groups(&[group], &opts))?;
        for r in results {
            r.map_err(|(name, code)| anyhow::anyhow!("delete group '{name}': {code:?}"))?;
        }
        Ok(())
    }

    fn admin_native(&self) -> *mut rdkafka::bindings::rd_kafka_t {
        self.admin.inner().native_ptr()
    }

    pub fn list_acls(&self) -> Result<Vec<crate::kafka::AclEntry>> {
        crate::kafka::acl::list_acls(self.admin_native())
    }

    pub fn create_acl(&self, spec: &crate::kafka::AclSpec) -> Result<()> {
        crate::kafka::acl::create_acl(self.admin_native(), spec)
    }

    pub fn delete_acl(&self, spec: &crate::kafka::AclSpec) -> Result<usize> {
        crate::kafka::acl::delete_acl(self.admin_native(), spec)
    }

    pub fn replace_acl(
        &self,
        old: &crate::kafka::AclSpec,
        new: &crate::kafka::AclSpec,
    ) -> Result<()> {
        crate::kafka::acl::replace_acl(self.admin_native(), old, new)
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn message_to_fetched<M: Message>(m: &M) -> FetchedMessage {
    FetchedMessage {
        partition: m.partition(),
        offset: m.offset(),
        timestamp_ms: m.timestamp().to_millis(),
        key: m.key().map(|k| String::from_utf8_lossy(k).into_owned()),
        payload: m.payload().map(|p| String::from_utf8_lossy(p).into_owned()),
        headers: message_headers(m),
    }
}

fn fetch_partition_messages(
    consumer: &BaseConsumer,
    topic: &str,
    part_id: i32,
    per_partition: usize,
    from_end: bool,
) -> Result<Vec<FetchedMessage>> {
    let timeout = Timeout::After(Duration::from_secs(5));
    let (low, high) = consumer.fetch_watermarks(topic, part_id, timeout)?;
    // high — следующий offset для записи; сообщения лежат в [low, high)
    if high <= low {
        return Ok(Vec::new());
    }

    let available = (high - low) as usize;
    let want = per_partition.min(available);
    if want == 0 {
        return Ok(Vec::new());
    }
    let start = if from_end {
        (high - want as i64).max(low)
    } else {
        low
    };

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, part_id, Offset::Offset(start))?;
    consumer.assign(&tpl)?;

    let mut out = Vec::with_capacity(want);
    let mut got = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while got < want && std::time::Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(250)) {
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
                out.push(message_to_fetched(&m));
            }
        }
    }

    Ok(out)
}

fn sort_fetched(out: &mut [FetchedMessage], single_partition: bool, sort_by_time: bool) {
    if single_partition {
        out.sort_by_key(|m| std::cmp::Reverse(m.offset));
    } else if sort_by_time {
        out.sort_by(|a, b| match (a.timestamp_ms, b.timestamp_ms) {
            (Some(ta), Some(tb)) => tb.cmp(&ta),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => (b.partition, b.offset).cmp(&(a.partition, a.offset)),
        });
    } else {
        out.sort_by_key(|m| std::cmp::Reverse((m.partition, m.offset)));
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

/// Consumer для чтения сообщений: без PartitionEOF-событий (они не ошибка, а конец очереди).
fn consumer_config(cluster: &ClusterConfig, group_prefix: &str) -> ClientConfig {
    let mut cfg = base_config(cluster);
    cfg.set("group.id", format!("{group_prefix}-{}", std::process::id()));
    cfg.set("enable.auto.commit", "false");
    // false — иначе librdkafka шлёт global ERROR PartitionEOF в tracing (портит TUI в live).
    cfg.set("enable.partition.eof", "false");
    cfg.set("log.connection.close", "false");
    cfg.set("log_level", "0");
    cfg
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
        AuthConfig::SaslPlain {
            username,
            password,
            tls,
        } => {
            apply_tls(&mut cfg, *tls, None, true);
            cfg.set(
                "security.protocol",
                if *tls { "SASL_SSL" } else { "SASL_PLAINTEXT" },
            );
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
            cfg.set(
                "security.protocol",
                if *tls { "SASL_SSL" } else { "SASL_PLAINTEXT" },
            );
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
            apply_tls(&mut cfg, *tls, ca.as_deref(), *tls_verify_hostname);
            cfg.set(
                "security.protocol",
                if *tls { "SASL_SSL" } else { "SASL_PLAINTEXT" },
            );
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

fn apply_tls(cfg: &mut ClientConfig, tls: bool, ca_location: Option<&str>, verify_hostname: bool) {
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
    #[ignore = "requires local Kafka: docker compose up -d"]
    fn list_acls_from_local_docker() {
        let cluster = ClusterConfig {
            brokers: vec!["localhost:9092".into()],
            auth: AuthConfig::SaslPlain {
                username: "admin".into(),
                password: "admin-secret".into(),
                tls: false,
            },
            client_id: Some("y2kexplorer-test".into()),
            schema_registry: None,
            kafka_connect: None,
        };
        let conn = ClusterConnection::connect(&cluster).expect("connect");
        let acls = conn.list_acls().expect("list acls");
        eprintln!("acls ({})", acls.len());
        for a in &acls {
            eprintln!(
                "{} {} {:?} {} {} {} {}",
                a.resource_type,
                a.resource_name,
                a.pattern_type,
                a.principal,
                a.host,
                a.operation,
                a.permission
            );
        }
        assert!(
            acls.iter()
                .any(|a| a.principal == "User:admin" && a.resource_name == "users.events"),
            "expected DENY ACL on users.events for User:admin"
        );
    }

    #[test]
    #[ignore = "requires local Kafka: docker compose up -d"]
    fn fetch_messages_from_local_orders() {
        let cluster = ClusterConfig {
            brokers: vec!["localhost:9092".into()],
            auth: AuthConfig::SaslPlain {
                username: "admin".into(),
                password: "admin-secret".into(),
                tls: false,
            },
            client_id: Some("y2kexplorer-test".into()),
            schema_registry: None,
            kafka_connect: None,
        };
        let conn = ClusterConnection::connect(&cluster).expect("connect");
        let msgs = conn
            .fetch_messages("orders", None, 10, true, true)
            .expect("fetch tail");
        assert!(!msgs.is_empty(), "expected messages in orders, got none");
    }
}

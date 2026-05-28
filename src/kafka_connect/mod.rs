//! Kafka Connect REST API client (Connect 2.x+).

use std::collections::BTreeMap;
use std::io::Read;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use crate::config::KafkaConnectConfig;

const TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct ConnectorSummary {
    pub name: String,
    pub kind: String,
    pub state: String,
    pub tasks_running: usize,
    pub tasks_failed: usize,
    pub tasks_total: usize,
}

#[derive(Debug, Clone)]
pub struct ConnectorDetail {
    pub name: String,
    pub kind: String,
    pub state: String,
    pub worker_id: Option<String>,
    pub tasks: Vec<TaskStatus>,
    pub config: BTreeMap<String, String>,
    pub config_json: String,
}

#[derive(Debug, Clone)]
pub struct TaskStatus {
    pub id: i32,
    pub state: String,
    pub worker_id: Option<String>,
    pub trace: Option<String>,
}

pub struct KafkaConnectClient {
    base: String,
    auth: Option<(String, String)>,
}

impl KafkaConnectClient {
    pub fn new(cfg: &KafkaConnectConfig) -> Result<Self> {
        let base = cfg.url.trim().trim_end_matches('/').to_string();
        if base.is_empty() {
            return Err(anyhow!("kafka_connect.url is empty"));
        }
        let auth = match (&cfg.username, &cfg.password) {
            (Some(u), Some(p)) if !u.is_empty() => Some((u.clone(), p.clone())),
            _ => None,
        };
        Ok(Self { base, auth })
    }

    pub fn list_summaries(&self) -> Result<Vec<ConnectorSummary>> {
        let url = format!("{}/connectors?expand=status&expand=info", self.base);
        let expanded: BTreeMap<String, ExpandEntry> = self.get_json(&url)?;
        let mut out = Vec::with_capacity(expanded.len());
        for (name, wrap) in expanded {
            out.push(summary_from_entry(&name, wrap)?);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    pub fn get_detail(&self, name: &str) -> Result<ConnectorDetail> {
        let status_url = format!("{}/connectors/{}/status", self.base, percent_encode(name));
        let config_url = format!("{}/connectors/{}/config", self.base, percent_encode(name));
        let status: StatusResponse = self.get_json(&status_url)?;
        let config: BTreeMap<String, String> = self.get_json(&config_url)?;
        let config_json = serde_json::to_string_pretty(&config).unwrap_or_default();
        let class = config
            .get("connector.class")
            .map(String::as_str)
            .unwrap_or("");
        let kind = connector_kind(class);
        let tasks = status
            .tasks
            .into_iter()
            .map(|t| TaskStatus {
                id: t.id,
                state: t.state,
                worker_id: t.worker_id,
                trace: t.trace,
            })
            .collect();
        Ok(ConnectorDetail {
            name: name.to_string(),
            kind,
            state: status.connector.state,
            worker_id: status.connector.worker_id,
            tasks,
            config,
            config_json,
        })
    }

    pub fn restart(&self, name: &str) -> Result<()> {
        let url = format!("{}/connectors/{}/restart", self.base, percent_encode(name));
        self.post_empty(&url)
    }

    pub fn pause(&self, name: &str) -> Result<()> {
        let url = format!("{}/connectors/{}/pause", self.base, percent_encode(name));
        self.put_empty(&url)
    }

    pub fn resume(&self, name: &str) -> Result<()> {
        let url = format!("{}/connectors/{}/resume", self.base, percent_encode(name));
        self.put_empty(&url)
    }

    pub fn delete(&self, name: &str) -> Result<()> {
        let url = format!("{}/connectors/{}", self.base, percent_encode(name));
        self.delete_req(&url)
    }

    fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T> {
        let mut req = ureq::get(url).timeout(TIMEOUT);
        if let Some((user, pass)) = &self.auth {
            req = req.set("Authorization", &basic_auth(user, pass));
        }
        let response = req.call().with_context(|| format!("GET {url}"))?;
        read_json(response, url)
    }

    fn post_empty(&self, url: &str) -> Result<()> {
        let mut req = ureq::post(url).timeout(TIMEOUT);
        if let Some((user, pass)) = &self.auth {
            req = req.set("Authorization", &basic_auth(user, pass));
        }
        let response = req.send_bytes(&[]).with_context(|| format!("POST {url}"))?;
        if response.status() >= 400 {
            return Err(http_error(response, url));
        }
        Ok(())
    }

    fn put_empty(&self, url: &str) -> Result<()> {
        let mut req = ureq::put(url).timeout(TIMEOUT);
        if let Some((user, pass)) = &self.auth {
            req = req.set("Authorization", &basic_auth(user, pass));
        }
        let response = req.send_bytes(&[]).with_context(|| format!("PUT {url}"))?;
        if response.status() >= 400 {
            return Err(http_error(response, url));
        }
        Ok(())
    }

    fn delete_req(&self, url: &str) -> Result<()> {
        let mut req = ureq::delete(url).timeout(TIMEOUT);
        if let Some((user, pass)) = &self.auth {
            req = req.set("Authorization", &basic_auth(user, pass));
        }
        let response = req.call().with_context(|| format!("DELETE {url}"))?;
        if response.status() >= 400 {
            return Err(http_error(response, url));
        }
        Ok(())
    }
}

fn summary_from_entry(name: &str, entry: ExpandEntry) -> Result<ConnectorSummary> {
    let status = entry.status.unwrap_or_default();
    let tasks_total = status.tasks.len();
    let tasks_running = status.tasks.iter().filter(|t| t.state == "RUNNING").count();
    let tasks_failed = status.tasks.iter().filter(|t| t.state == "FAILED").count();
    let kind = entry
        .info
        .as_ref()
        .and_then(|i| i.config.get("connector.class"))
        .map(|c| connector_kind(c))
        .or_else(|| entry.info.as_ref().map(|i| i.r#type.clone()))
        .unwrap_or_else(|| "—".into());
    Ok(ConnectorSummary {
        name: name.to_string(),
        kind,
        state: status.connector.state,
        tasks_running,
        tasks_failed,
        tasks_total,
    })
}

fn connector_kind(class: &str) -> String {
    if class.contains("Source") {
        "source".into()
    } else if class.contains("Sink") {
        "sink".into()
    } else {
        "connector".into()
    }
}

#[derive(Debug, Deserialize)]
struct ExpandEntry {
    #[serde(default)]
    status: Option<StatusResponse>,
    #[serde(default)]
    info: Option<InfoResponse>,
}

#[derive(Debug, Deserialize)]
struct InfoResponse {
    #[serde(rename = "type", default)]
    r#type: String,
    #[serde(default)]
    config: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
struct StatusResponse {
    #[serde(default)]
    connector: ConnectorState,
    #[serde(default)]
    tasks: Vec<TaskEntry>,
}

#[derive(Debug, Default, Deserialize)]
struct ConnectorState {
    #[serde(default)]
    state: String,
    #[serde(default)]
    worker_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaskEntry {
    id: i32,
    state: String,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    trace: Option<String>,
}

fn read_json<T: for<'de> Deserialize<'de>>(response: ureq::Response, url: &str) -> Result<T> {
    let status = response.status();
    let mut body = String::new();
    response
        .into_reader()
        .read_to_string(&mut body)
        .context("read connect response")?;
    if status >= 400 {
        return Err(anyhow!("kafka connect HTTP {status} on {url}: {body}"));
    }
    serde_json::from_str(&body).with_context(|| format!("parse JSON from {url}"))
}

fn http_error(response: ureq::Response, url: &str) -> anyhow::Error {
    let status = response.status();
    let mut body = String::new();
    let _ = response.into_reader().read_to_string(&mut body);
    anyhow!("kafka connect HTTP {status} on {url}: {body}")
}

fn percent_encode(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

fn basic_auth(user: &str, pass: &str) -> String {
    let raw = format!("{user}:{pass}");
    format!("Basic {}", base64_encode(raw.as_bytes()))
}

fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KafkaConnectConfig;

    #[test]
    #[ignore = "requires docker compose: kafka-connect on :8083"]
    fn list_connectors_local() {
        let cfg = KafkaConnectConfig {
            url: "http://localhost:8083".into(),
            username: None,
            password: None,
            tls: false,
        };
        let client = KafkaConnectClient::new(&cfg).expect("client");
        let list = client.list_summaries().expect("list");
        assert!(
            list.iter().any(|c| c.name == "file-source"),
            "expected file-source connector"
        );
        assert!(
            list.iter().any(|c| c.name == "file-sink"),
            "expected file-sink connector"
        );
    }
}

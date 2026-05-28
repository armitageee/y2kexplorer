//! Confluent-compatible Schema Registry REST client.

use std::io::Read;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use crate::config::SchemaRegistryConfig;

const TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct SchemaSubjectSummary {
    pub subject: String,
    pub latest_version: i32,
    pub schema_type: String,
    pub version_count: usize,
}

#[derive(Debug, Clone)]
pub struct SchemaVersionDetail {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    pub schema_type: String,
    pub schema: String,
}

pub struct SchemaRegistryClient {
    base: String,
    auth: Option<(String, String)>,
}

impl SchemaRegistryClient {
    pub fn new(cfg: &SchemaRegistryConfig) -> Result<Self> {
        let base = cfg.url.trim().trim_end_matches('/').to_string();
        if base.is_empty() {
            return Err(anyhow!("schema_registry.url is empty"));
        }
        let auth = match (&cfg.username, &cfg.password) {
            (Some(u), Some(p)) if !u.is_empty() => Some((u.clone(), p.clone())),
            _ => None,
        };
        Ok(Self { base, auth })
    }

    pub fn list_subjects(&self) -> Result<Vec<String>> {
        let url = format!("{}/subjects", self.base);
        let subjects: Vec<String> = self.get_json(&url)?;
        let mut out = subjects;
        out.sort();
        Ok(out)
    }

    pub fn list_summaries(&self) -> Result<Vec<SchemaSubjectSummary>> {
        let subjects = self.list_subjects()?;
        let mut out = Vec::with_capacity(subjects.len());
        for subject in subjects {
            out.push(self.summarize_subject(&subject)?);
        }
        Ok(out)
    }

    fn summarize_subject(&self, subject: &str) -> Result<SchemaSubjectSummary> {
        let versions = self.list_versions(subject)?;
        let latest = versions.iter().copied().max().unwrap_or(0);
        let latest_detail = if latest > 0 {
            self.get_version(subject, latest)?
        } else {
            return Err(anyhow!("subject {subject} has no versions"));
        };
        Ok(SchemaSubjectSummary {
            subject: subject.to_string(),
            latest_version: latest,
            schema_type: latest_detail.schema_type,
            version_count: versions.len(),
        })
    }

    pub fn list_versions(&self, subject: &str) -> Result<Vec<i32>> {
        let url = format!(
            "{}/subjects/{}/versions",
            self.base,
            percent_encode(subject)
        );
        let versions: Vec<i32> = self.get_json(&url)?;
        Ok(versions)
    }

    pub fn get_latest(&self, subject: &str) -> Result<SchemaVersionDetail> {
        let url = format!(
            "{}/subjects/{}/versions/latest",
            self.base,
            percent_encode(subject)
        );
        self.parse_version(subject, self.get_json::<VersionResponse>(&url)?)
    }

    pub fn get_version(&self, subject: &str, version: i32) -> Result<SchemaVersionDetail> {
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base,
            percent_encode(subject),
            version
        );
        self.parse_version(subject, self.get_json::<VersionResponse>(&url)?)
    }

    fn parse_version(&self, subject: &str, v: VersionResponse) -> Result<SchemaVersionDetail> {
        Ok(SchemaVersionDetail {
            subject: v.subject.unwrap_or_else(|| subject.to_string()),
            version: v.version,
            id: v.id,
            schema_type: v.schema_type.unwrap_or_else(|| "AVRO".into()),
            schema: v.schema,
        })
    }

    fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T> {
        let mut req = ureq::get(url)
            .set("Accept", "application/vnd.schemaregistry.v1+json")
            .timeout(TIMEOUT);
        if let Some((user, pass)) = &self.auth {
            req = req.set("Authorization", &basic_auth(user, pass));
        }
        let response = req.call().with_context(|| format!("GET {url}"))?;
        let status = response.status();
        let mut body = String::new();
        response
            .into_reader()
            .read_to_string(&mut body)
            .context("read schema registry response")?;
        if !(200..300).contains(&status) {
            return Err(anyhow!("schema registry HTTP {status}: {body}"));
        }
        serde_json::from_str(&body).with_context(|| format!("parse JSON from {url}"))
    }
}

#[derive(Debug, Deserialize)]
struct VersionResponse {
    #[serde(default)]
    subject: Option<String>,
    version: i32,
    id: i32,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    schema: String,
}

fn percent_encode(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

fn basic_auth(user: &str, pass: &str) -> String {
    let raw = format!("{user}:{pass}");
    let encoded = base64_encode(raw.as_bytes());
    format!("Basic {encoded}")
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
    use crate::config::SchemaRegistryConfig;

    #[test]
    #[ignore = "requires docker compose: schema-registry on :8081"]
    fn list_summaries_local() {
        let cfg = SchemaRegistryConfig {
            url: "http://localhost:8081".into(),
            username: None,
            password: None,
            tls: false,
        };
        let client = SchemaRegistryClient::new(&cfg).expect("client");
        let summaries = client.list_summaries().expect("list");
        assert!(!summaries.is_empty(), "expected demo schemas");
        assert!(
            summaries.iter().any(|s| s.subject == "orders-value"),
            "expected orders-value"
        );
    }
}

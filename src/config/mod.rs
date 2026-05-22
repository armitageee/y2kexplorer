use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub clusters: HashMap<String, ClusterConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Defaults {
    pub cluster: Option<String>,
    #[serde(default = "default_message_limit")]
    pub message_limit: usize,
}

fn default_message_limit() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub brokers: Vec<String>,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    #[default]
    None,
    SaslPlain {
        username: String,
        password: String,
        #[serde(default)]
        tls: bool,
    },
    SaslScram {
        username: String,
        password: String,
        mechanism: ScramMechanism,
        #[serde(default)]
        tls: bool,
    },
    Ssl {
        ca_location: Option<String>,
        certificate_location: Option<String>,
        key_location: Option<String>,
        key_password: Option<String>,
    },
    Kerberos {
        /// Path to client keytab file.
        keytab: PathBuf,
        /// e.g. kafka-client/host.example.com@REALM
        principal: String,
        /// Broker service name (primary in broker principal), usually "kafka".
        #[serde(default = "default_kerberos_service")]
        service_name: String,
        #[serde(default)]
        tls: bool,
    },
}

fn default_kerberos_service() -> String {
    "kafka".into()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum ScramMechanism {
    ScramSha256,
    ScramSha512,
}

impl AppConfig {
    pub fn config_dir() -> Result<PathBuf> {
        let dirs = ProjectDirs::from("com", "crpt", "y2kexplorer")
            .context("could not resolve config directory")?;
        Ok(dirs.config_dir().to_path_buf())
    }

    pub fn config_path() -> Result<PathBuf> {
        Ok(Self::config_dir()?.join("config.toml"))
    }

    pub fn load(path: Option<&Path>) -> Result<Self> {
        let path = path
            .map(Path::to_path_buf)
            .unwrap_or_else(|| Self::config_path().unwrap_or_default());

        if !path.exists() {
            return Ok(Self::default_with_example());
        }

        let raw = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parse {}", path.display()))
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::config_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let raw = toml::to_string_pretty(self)?;
        fs::write(&path, raw)?;
        Ok(())
    }

    pub fn active_cluster(&self) -> Result<(&str, &ClusterConfig)> {
        let name = self
            .defaults
            .cluster
            .as_deref()
            .or_else(|| self.clusters.keys().next().map(String::as_str))
            .context("no cluster configured; edit config.toml")?;
        let cluster = self
            .clusters
            .get(name)
            .with_context(|| format!("cluster '{name}' not found"))?;
        Ok((name, cluster))
    }

    fn default_with_example() -> Self {
        let mut clusters = HashMap::new();
        clusters.insert(
            "local".into(),
            ClusterConfig {
                brokers: vec!["localhost:9092".into()],
                auth: AuthConfig::None,
                client_id: Some("y2kexplorer".into()),
            },
        );
        Self {
            defaults: Defaults {
                cluster: Some("local".into()),
                message_limit: default_message_limit(),
            },
            clusters,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::default_with_example()
    }
}

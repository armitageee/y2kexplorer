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
    /// Интервал live-poll (секунды), 1–30.
    #[serde(default = "default_live_poll_secs")]
    pub live_poll_secs: u64,
    /// UI-тема: `"dark"` (по умолчанию) или `"light"`. Можно переопределить флагом `--theme`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub theme: Option<String>,
}

fn default_message_limit() -> usize {
    100
}

fn default_live_poll_secs() -> u64 {
    3
}

pub const LIVE_POLL_MIN_SECS: u64 = 1;
pub const LIVE_POLL_MAX_SECS: u64 = 30;

pub fn clamp_live_poll_secs(secs: u64) -> u64 {
    secs.clamp(LIVE_POLL_MIN_SECS, LIVE_POLL_MAX_SECS)
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
        /// Путь к krb5.conf → `KRB5_CONFIG` (опционально).
        krb5_conf: Option<PathBuf>,
        /// CA для TLS (ssl.ca.location), если брокер на корпоративном CA.
        ssl_ca: Option<String>,
        /// Проверка имени хоста в TLS-сертификате брокера (false → identification none).
        #[serde(default = "default_true")]
        tls_verify_hostname: bool,
    },
}

fn default_true() -> bool {
    true
}

/// CA для Kerberos+TLS: явный `ssl_ca`, иначе первый существующий путь из `krb5.conf` (pkinit_*).
pub fn resolve_kerberos_ssl_ca(auth: &AuthConfig) -> Option<String> {
    let AuthConfig::Kerberos {
        ssl_ca, krb5_conf, ..
    } = auth
    else {
        return None;
    };

    if let Some(ca) = ssl_ca {
        if Path::new(ca).exists() {
            return Some(ca.clone());
        }
    }
    if let Some(krb5) = krb5_conf {
        for ca in krb5_ca_candidates(krb5) {
            if Path::new(&ca).exists() {
                return Some(ca);
            }
        }
    }
    None
}

/// Пути `FILE:...` из krb5.conf (pkinit_pool, pkinit_anchors и т.п.).
pub fn krb5_ca_candidates(krb5_conf: &Path) -> Vec<String> {
    let Ok(raw) = fs::read_to_string(krb5_conf) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some(i) = trimmed.find("FILE:") {
            let p = trimmed[i + 5..]
                .trim()
                .trim_end_matches(['}', ',', '"', ';']);
            if !p.is_empty() {
                out.push(p.to_string());
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Kerberos через keytab: krb5.conf + отдельный FILE ccache (не macOS UUID cache).
pub fn apply_kerberos_env(auth: &AuthConfig) {
    let AuthConfig::Kerberos {
        keytab, krb5_conf, ..
    } = auth
    else {
        return;
    };

    if let Some(path) = krb5_conf {
        set_env_var("KRB5_CONFIG", path.display().to_string());
    }

    set_env_var("KRB5_CLIENT_KTNAME", keytab.display().to_string());

    // Свой ccache в /tmp: kinit возьмёт ticket из keytab, не из login session
    let ccache = format!("FILE:/tmp/y2kexplorer-{}.ccache", std::process::id());
    set_env_var("KRB5CCNAME", ccache);
}

fn set_env_var(key: &str, value: String) {
    // SAFETY: перед созданием rdkafka-клиента в том же потоке.
    unsafe { std::env::set_var(key, value) };
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
    /// Основной путь: `~/.config/y2kexplorer/config.toml` (как в README).
    pub fn config_dir() -> Result<PathBuf> {
        Ok(Self::home_config_dir())
    }

    pub fn config_path() -> Result<PathBuf> {
        Ok(Self::resolve_config_path(None))
    }

    /// Ищет конфиг: явный путь → `$Y2K_CONFIG` → `~/.config/...` → legacy macOS path.
    pub fn resolve_config_path(explicit: Option<&Path>) -> PathBuf {
        if let Some(p) = explicit {
            return p.to_path_buf();
        }
        if let Ok(p) = std::env::var("Y2K_CONFIG") {
            return PathBuf::from(p);
        }
        let home_cfg = Self::home_config_dir().join("config.toml");
        if home_cfg.exists() {
            return home_cfg;
        }
        if let Ok(legacy) = Self::legacy_config_path() {
            if legacy.exists() {
                return legacy;
            }
        }
        home_cfg
    }

    fn home_config_dir() -> PathBuf {
        directories::BaseDirs::new()
            .map(|d| d.home_dir().join(".config").join("y2kexplorer"))
            .unwrap_or_else(|| PathBuf::from(".config/y2kexplorer"))
    }

    fn legacy_config_path() -> Result<PathBuf> {
        let dirs = ProjectDirs::from("com", "crpt", "y2kexplorer")
            .context("could not resolve legacy config directory")?;
        Ok(dirs.config_dir().join("config.toml"))
    }

    pub fn load(path: Option<&Path>) -> Result<Self> {
        let path = Self::resolve_config_path(path);

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

    pub fn cluster_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.clusters.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn set_active_cluster(&mut self, name: &str) -> Result<()> {
        if !self.clusters.contains_key(name) {
            let available = self.cluster_names().join(", ");
            anyhow::bail!("cluster '{name}' not found (available: {available})");
        }
        self.defaults.cluster = Some(name.to_string());
        Ok(())
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
                live_poll_secs: default_live_poll_secs(),
                theme: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_prefers_dot_config() {
        let path = AppConfig::resolve_config_path(None);
        assert!(
            path.ends_with(".config/y2kexplorer/config.toml"),
            "unexpected path: {}",
            path.display()
        );
    }

    #[test]
    fn load_user_config_includes_secure() {
        let path = AppConfig::resolve_config_path(None);
        if !path.exists() {
            return;
        }
        let cfg = AppConfig::load(Some(&path)).expect("parse");
        assert!(cfg.clusters.contains_key("local"));
        assert!(
            cfg.clusters.contains_key("secure"),
            "clusters: {:?}",
            cfg.cluster_names()
        );
    }
}

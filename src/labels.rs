//! Локальные лейблы топиков (не в Kafka — только в config.toml).
//!
//! Формат в конфиге:
//! ```toml
//! [topic_labels.lt01]
//! "orders" = ["order-service", "prod"]
//! ```

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// `cluster_name` → `topic_name` → список лейблов.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicLabelStore {
    #[serde(default)]
    pub clusters: HashMap<String, HashMap<String, Vec<String>>>,
}

impl TopicLabelStore {
    pub fn labels_for(&self, cluster: &str, topic: &str) -> Vec<String> {
        self.clusters
            .get(cluster)
            .and_then(|m| m.get(topic))
            .cloned()
            .unwrap_or_default()
    }

    pub fn all_labels(&self, cluster: &str) -> Vec<(String, usize)> {
        let Some(topics) = self.clusters.get(cluster) else {
            return Vec::new();
        };
        let mut counts: HashMap<String, usize> = HashMap::new();
        for labels in topics.values() {
            for l in labels {
                *counts.entry(l.clone()).or_default() += 1;
            }
        }
        let mut out: Vec<_> = counts.into_iter().collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    pub fn topics_with_label(&self, cluster: &str, label: &str) -> HashSet<String> {
        let Some(topics) = self.clusters.get(cluster) else {
            return HashSet::new();
        };
        let needle = label.to_lowercase();
        topics
            .iter()
            .filter(|(_, labels)| labels.iter().any(|l| l.to_lowercase() == needle))
            .map(|(name, _)| name.clone())
            .collect()
    }

    pub fn add_label(&mut self, cluster: &str, topic: &str, label: &str) {
        let label = normalize_label(label);
        if label.is_empty() {
            return;
        }
        let entry = self
            .clusters
            .entry(cluster.to_string())
            .or_default()
            .entry(topic.to_string())
            .or_default();
        if !entry.iter().any(|l| l == &label) {
            entry.push(label);
            entry.sort();
        }
    }

    pub fn remove_label(&mut self, cluster: &str, topic: &str, label: &str) {
        let Some(topics) = self.clusters.get_mut(cluster) else {
            return;
        };
        let Some(entry) = topics.get_mut(topic) else {
            return;
        };
        let needle = label.to_lowercase();
        entry.retain(|l| l.to_lowercase() != needle);
        if entry.is_empty() {
            topics.remove(topic);
        }
    }

    pub fn remove_label_from_many(&mut self, cluster: &str, topic_names: &[String], label: &str) {
        for t in topic_names {
            self.remove_label(cluster, t, label);
        }
    }

    pub fn add_label_to_many(&mut self, cluster: &str, topic_names: &[String], label: &str) {
        for t in topic_names {
            self.add_label(cluster, t, label);
        }
    }

    /// Удалить лейбл со всех топиков кластера. Возвращает число топиков, где лейбл был снят.
    pub fn delete_label(&mut self, cluster: &str, label: &str) -> usize {
        let needle = label.trim().to_lowercase();
        if needle.is_empty() {
            return 0;
        }
        let Some(topics) = self.clusters.get_mut(cluster) else {
            return 0;
        };
        let mut affected = 0usize;
        let empty_topics: Vec<String> = topics
            .iter_mut()
            .filter_map(|(topic, labels)| {
                let before = labels.len();
                labels.retain(|l| l.to_lowercase() != needle);
                if before != labels.len() {
                    affected += 1;
                }
                if labels.is_empty() {
                    Some(topic.clone())
                } else {
                    None
                }
            })
            .collect();
        for t in empty_topics {
            topics.remove(&t);
        }
        if topics.is_empty() {
            self.clusters.remove(cluster);
        }
        affected
    }

    pub fn label_topic_count(&self, cluster: &str, label: &str) -> usize {
        self.topics_with_label(cluster, label).len()
    }
}

fn normalize_label(s: &str) -> String {
    s.trim().to_lowercase()
}

pub fn format_labels(labels: &[String]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    labels.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_list_labels() {
        let mut store = TopicLabelStore::default();
        store.add_label("c1", "orders", "billing");
        store.add_label("c1", "orders", "billing"); // dup
        assert_eq!(store.labels_for("c1", "orders"), vec!["billing"]);
        store.add_label("c1", "payments", "billing");
        let all = store.all_labels("c1");
        assert_eq!(all, vec![("billing".into(), 2)]);

        let n = store.delete_label("c1", "billing");
        assert_eq!(n, 2);
        assert!(store.all_labels("c1").is_empty());
        assert!(store.labels_for("c1", "orders").is_empty());
    }
}

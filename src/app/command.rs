use crate::app::App;
use crate::config::{clamp_live_poll_secs, AppConfig};

impl App {
    pub fn execute_command(&mut self, line: &str) {
        let line = line.trim();
        if line.is_empty() {
            return;
        }

        let mut parts = line.split_whitespace();
        let cmd = parts.next().unwrap_or("").to_lowercase();

        match cmd.as_str() {
            "context" | "ctx" => {
                if let Some(name) = parts.next() {
                    self.switch_cluster(name);
                } else {
                    self.status = self.format_clusters_list();
                }
            }
            "clusters" => self.status = self.format_clusters_list(),
            "limit" => {
                if let Some(n) = parts.next() {
                    match n.parse::<usize>() {
                        Ok(limit) if (10..=10_000).contains(&limit) => {
                            self.set_message_limit(limit)
                        }
                        _ => self.status = "limit must be 10–10000".into(),
                    }
                } else {
                    self.status = "usage: :limit <N>  (10–10000)".into();
                }
            }
            "poll" => {
                if let Some(n) = parts.next() {
                    match n.parse::<u64>() {
                        Ok(secs) => {
                            let secs = clamp_live_poll_secs(secs);
                            self.config.defaults.live_poll_secs = secs;
                            self.last_live_poll = None;
                            match self.config.save() {
                                Ok(()) => {
                                    self.status = format!("live poll interval → {secs}s (saved)")
                                }
                                Err(e) => {
                                    self.status =
                                        format!("live poll interval → {secs}s (not saved: {e:#})")
                                }
                            }
                        }
                        _ => self.status = "poll must be seconds (1–30)".into(),
                    }
                } else {
                    let secs = clamp_live_poll_secs(self.config.defaults.live_poll_secs);
                    self.status = format!("live poll interval: {secs}s  (:poll <1-30>)");
                }
            }
            "groups" | "g" => self.open_groups(),
            "labels" => self.switch_nav(crate::views::Screen::Labels),
            "contexts" => self.switch_nav(crate::views::Screen::Contexts),
            "acls" | "acl" => self.switch_nav(crate::views::Screen::Acls),
            "schemas" | "schema" | "sr" => self.switch_nav(crate::views::Screen::Schemas),
            "connect" | "connectors" => self.switch_nav(crate::views::Screen::Connectors),
            "label-delete" | "label-rm" => {
                if let Some(name) = parts.next() {
                    self.run_delete_label(name.to_string());
                } else {
                    self.status = "usage: :label-delete <name>".into();
                }
            }
            "label" => {
                if let Some(name) = parts.next() {
                    let label = name.to_string();
                    let cluster = self.cluster_name.clone();
                    let mut tv = crate::views::TopicsView::new();
                    tv.set_label_filter(Some(label.clone()));
                    if !self.cached_topics.is_empty() {
                        tv.load_with_labels(
                            self.cached_topics.clone(),
                            &self.config.topic_labels,
                            &cluster,
                        );
                    }
                    self.stack = vec![crate::views::ViewStack::Topics(tv)];
                    if self.cached_topics.is_empty() {
                        self.refresh_topics();
                    } else {
                        self.status = format!("filter: label @{label}");
                    }
                } else {
                    self.status = "usage: :label <name>".into();
                }
            }
            "help" | "h" | "?" => {
                let cfg = AppConfig::config_path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|_| "?".into());
                self.status = format!(
                    "config: {cfg}  |  :context  :contexts  :groups  :labels  :acls  :schemas  :connect  :label  :label-delete  :limit  :poll  :help"
                );
            }
            _ => {
                self.status = format!("unknown command '{cmd}' — try :help");
            }
        }
    }

    pub fn format_clusters_list(&self) -> String {
        let active = self.cluster_name.as_str();
        let names = self.config.cluster_names();
        let listed: Vec<String> = names
            .iter()
            .map(|n| {
                if n == active {
                    format!("*{n}")
                } else {
                    n.clone()
                }
            })
            .collect();
        format!("clusters: {}  (current: {active})", listed.join(", "))
    }

    pub fn switch_cluster(&mut self, name: &str) {
        if let Err(e) = self.config.set_active_cluster(name) {
            self.status = format!("{e:#}");
            return;
        }

        let cluster = match self.config.clusters.get(name) {
            Some(c) => c.clone(),
            None => return,
        };

        self.cluster_name = name.to_string();
        self.cluster = cluster;
        self.connection = None;
        self.cached_topics.clear();
        self.stack = vec![crate::views::ViewStack::Topics(
            crate::views::TopicsView::new(),
        )];
        self.show_partitions_popup = false;
        self.loading = true;
        self.status = format!("switching to {name}…");

        match self.config.save() {
            Ok(()) => self.status = format!("cluster → {name} (saved to config)"),
            Err(e) => self.status = format!("cluster → {name} (config not saved: {e:#})"),
        }

        self.init_connection();
    }
}

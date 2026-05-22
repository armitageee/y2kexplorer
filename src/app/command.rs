use crate::app::App;
use crate::config::AppConfig;

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
                        Ok(limit) if (10..=10_000).contains(&limit) => self.set_message_limit(limit),
                        _ => self.status = "limit must be 10–10000".into(),
                    }
                } else {
                    self.status = "usage: :limit <N>  (10–10000)".into();
                }
            }
            "help" | "h" | "?" => {
                let cfg = AppConfig::config_path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|_| "?".into());
                self.status = format!(
                    "config: {cfg}  |  :context <name>  :clusters  :limit <N>  :help  (alias: ctx)"
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
        self.stack = vec![crate::views::ViewStack::Topics(crate::views::TopicsView::new())];
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

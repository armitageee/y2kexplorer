use ratatui::layout::Rect;
use ratatui::Frame;

use crate::config::{AppConfig, AuthConfig};
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k",
    "nav",
    "Enter",
    "switch cluster",
    "/",
    "filter",
    "1-4",
    "nav pane",
    "?",
    "help",
    "q",
    "quit",
];

const HINT: &[&str] = &[
    "Enter",
    "switch",
    "/",
    "filter",
    "1",
    "topics",
    "?",
    "help",
];

pub struct ContextListView {
    pub table: TableView,
    pub show_help: bool,
}

impl ContextListView {
    pub fn new() -> Self {
        Self {
            table: TableView::new(
                "Contexts",
                vec![
                    "CONTEXT".into(),
                    "".into(),
                    "BROKERS".into(),
                    "AUTH".into(),
                ],
            ),
            show_help: false,
        }
    }

    pub fn load(&mut self, config: &AppConfig, active: &str) {
        let rows: Vec<Vec<String>> = config
            .cluster_names()
            .into_iter()
            .filter_map(|name| {
                let cluster = config.clusters.get(&name)?;
                let mark = if name == active { "*" } else { "" };
                let brokers = cluster.brokers.join(",");
                let brokers = if brokers.len() > 48 {
                    format!("{}…", &brokers[..45])
                } else {
                    brokers
                };
                Some(vec![name, mark.into(), brokers, auth_label(&cluster.auth)])
            })
            .collect();
        self.table.set_rows(rows);
        // Курсор на активный контекст.
        if let Some(i) = self
            .table
            .rows
            .iter()
            .position(|r| r.get(1).map(String::as_str) == Some("*"))
        {
            self.table.state.select(Some(i));
        }
    }

    pub fn selected_context(&self) -> Option<String> {
        let row = self.table.selected_row()?;
        row.first().cloned()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn render(
        &mut self,
        frame: &mut Frame,
        main: Rect,
        status_area: Rect,
        keys_area: Rect,
        cluster: &str,
        status: &str,
        loading: bool,
    ) {
        self.table.render(frame, main);
        draw_status(frame, status_area, cluster, status, loading);
        if self.show_help {
            draw_help(frame, keys_area, HELP);
        } else {
            draw_help(frame, keys_area, HINT);
        }
    }
}

fn auth_label(auth: &AuthConfig) -> String {
    match auth {
        AuthConfig::None => "none".into(),
        AuthConfig::SaslPlain { tls, .. } => {
            if *tls {
                "sasl_plain+tls".into()
            } else {
                "sasl_plain".into()
            }
        }
        AuthConfig::SaslScram { mechanism, tls, .. } => {
            let mech = match mechanism {
                crate::config::ScramMechanism::ScramSha256 => "scram256",
                crate::config::ScramMechanism::ScramSha512 => "scram512",
            };
            if *tls {
                format!("{mech}+tls")
            } else {
                mech.into()
            }
        }
        AuthConfig::Ssl { .. } => "ssl".into(),
        AuthConfig::Kerberos { tls, .. } => {
            if *tls {
                "kerberos+tls".into()
            } else {
                "kerberos".into()
            }
        }
    }
}

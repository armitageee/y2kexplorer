use ratatui::layout::Rect;
use ratatui::Frame;

use crate::kafka_connect::ConnectorSummary;
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k",
    "nav",
    "Enter",
    "detail",
    "/",
    "filter",
    "r",
    "refresh",
    "Esc",
    "back",
    "?",
    "help",
    "q",
    "quit",
];

const HINT: &[&str] = &["Enter", "detail", "/", "filter", "r", "refresh", "?", "help"];

pub struct ConnectorsView {
    pub table: TableView,
    pub show_help: bool,
    pub connectors: Vec<ConnectorSummary>,
}

impl ConnectorsView {
    pub fn new() -> Self {
        Self {
            table: TableView::new(
                "Connectors",
                vec![
                    "NAME".into(),
                    "TYPE".into(),
                    "STATE".into(),
                    "TASKS".into(),
                ],
            ),
            show_help: false,
            connectors: Vec::new(),
        }
    }

    pub fn load(&mut self, connectors: Vec<ConnectorSummary>) {
        self.connectors = connectors.clone();
        let rows = connectors
            .into_iter()
            .map(|c| {
                let tasks = if c.tasks_failed > 0 {
                    format!(
                        "{}/{} ({} failed)",
                        c.tasks_running, c.tasks_total, c.tasks_failed
                    )
                } else {
                    format!("{}/{}", c.tasks_running, c.tasks_total)
                };
                vec![c.name, c.kind, c.state, tasks]
            })
            .collect();
        self.table.set_rows(rows);
    }

    pub fn selected_name(&self) -> Option<&str> {
        let row = self.table.selected_row()?;
        row.first().map(String::as_str)
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

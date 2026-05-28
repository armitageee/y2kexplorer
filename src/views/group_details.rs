use ratatui::layout::Rect;
use ratatui::Frame;

use crate::kafka::GroupOffset;
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k",
    "nav",
    "R",
    "reset offsets",
    "r",
    "refresh",
    "Esc",
    "back",
    "?",
    "help",
    "q",
    "quit",
];

const HINT: &[&str] = &["R", "reset", "r", "refresh", "Esc", "back", "?", "help"];

pub struct GroupDetailsView {
    pub group: String,
    pub state: String,
    pub members: usize,
    pub table: TableView,
    pub show_help: bool,
    pub offsets: Vec<GroupOffset>,
}

impl GroupDetailsView {
    pub fn help_pairs(&self) -> &'static [&'static str] {
        if self.show_help {
            HELP
        } else {
            HINT
        }
    }

    pub fn new(group: impl Into<String>) -> Self {
        let group = group.into();
        Self {
            table: TableView::new(
                format!("group {group}"),
                vec![
                    "TOPIC".into(),
                    "PART".into(),
                    "CURRENT".into(),
                    "LOG-END".into(),
                    "LAG".into(),
                ],
            ),
            group,
            state: String::new(),
            members: 0,
            show_help: false,
            offsets: Vec::new(),
        }
    }

    pub fn load(&mut self, offsets: Vec<GroupOffset>) {
        self.offsets = offsets.clone();
        let total_lag: i64 = offsets.iter().map(|o| o.lag).sum();
        self.table.title = format!(
            "group {} · {} part · total lag {}",
            self.group,
            offsets.len(),
            total_lag
        );
        let rows = offsets
            .into_iter()
            .map(|o| {
                vec![
                    o.topic,
                    o.partition.to_string(),
                    o.current_offset
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "—".into()),
                    o.log_end_offset.to_string(),
                    o.lag.to_string(),
                ]
            })
            .collect();
        self.table.set_rows(rows);
    }

    pub fn set_meta(&mut self, state: String, members: usize) {
        self.state = state;
        self.members = members;
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
        draw_help(frame, keys_area, self.help_pairs());
    }
}

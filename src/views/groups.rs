use ratatui::layout::Rect;
use ratatui::Frame;

use crate::kafka::ConsumerGroupInfo;
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k",
    "nav",
    ":",
    "command",
    "/",
    "filter",
    "Enter",
    "details",
    "R",
    "reset offsets",
    "d",
    "delete",
    "r",
    "refresh",
    "Esc",
    "back",
    "?",
    "help",
    "q",
    "quit",
];

const HINT: &[&str] = &[
    "Enter", "details", "R", "reset", "d", "delete", "/", "filter", "Esc", "back", "?", "help",
];

pub struct GroupsView {
    pub table: TableView,
    pub show_help: bool,
    pub groups: Vec<ConsumerGroupInfo>,
}

impl GroupsView {
    pub fn new() -> Self {
        Self {
            table: TableView::new(
                "Consumer Groups",
                vec![
                    "ID".into(),
                    "STATE".into(),
                    "MEMBERS".into(),
                    "PROTOCOL".into(),
                    "TYPE".into(),
                ],
            ),
            show_help: false,
            groups: Vec::new(),
        }
    }

    pub fn load(&mut self, groups: Vec<ConsumerGroupInfo>) {
        self.groups = groups.clone();
        let rows = groups
            .into_iter()
            .map(|g| {
                vec![
                    g.id,
                    g.state,
                    g.members.to_string(),
                    g.protocol,
                    g.protocol_type,
                ]
            })
            .collect();
        self.table.set_rows(rows);
    }

    pub fn selected_id(&self) -> Option<&str> {
        let row = self.table.selected_row()?;
        row.first().map(String::as_str)
    }

    pub fn selected_group(&self) -> Option<&ConsumerGroupInfo> {
        let id = self.selected_id()?;
        self.groups.iter().find(|g| g.id == id)
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

use ratatui::layout::Rect;
use ratatui::Frame;

use crate::labels::TopicLabelStore;
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k",
    "nav",
    "Enter",
    "filter topics",
    "/",
    "filter",
    "1/2/3",
    "nav pane",
    "?",
    "help",
    "q",
    "quit",
];

const HINT: &[&str] = &[
    "Enter",
    "topics with label",
    "/",
    "filter",
    "1",
    "topics",
    "2",
    "groups",
    "?",
    "help",
];

pub struct LabelListView {
    pub table: TableView,
    pub show_help: bool,
}

impl LabelListView {
    pub fn new() -> Self {
        Self {
            table: TableView::new("Labels", vec!["LABEL".into(), "TOPICS".into()]),
            show_help: false,
        }
    }

    pub fn load(&mut self, store: &TopicLabelStore, cluster: &str) {
        let rows = store
            .all_labels(cluster)
            .into_iter()
            .map(|(label, count)| vec![label, count.to_string()])
            .collect();
        self.table.set_rows(rows);
    }

    pub fn selected_label(&self) -> Option<&str> {
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
        let _ = cluster;
        self.table.render(frame, main);
        draw_status(frame, status_area, cluster, status, loading);
        if self.show_help {
            draw_help(frame, keys_area, HELP);
        } else {
            draw_help(frame, keys_area, HINT);
        }
    }
}

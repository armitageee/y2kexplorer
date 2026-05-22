use ratatui::layout::Rect;
use ratatui::Frame;

use crate::kafka::TopicInfo;
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k",
    "nav",
    ":",
    "command",
    "/",
    "filter",
    "Enter",
    "messages",
    "n",
    "produce",
    "c",
    "create",
    "d",
    "delete",
    "p",
    "partitions",
    "r",
    "refresh",
    "?",
    "help",
    "q",
    "quit",
];

const HINT: &[&str] = &[
    ":", "context", "Enter", "open", "n", "produce", "c", "create", "d", "delete", "/", "filter",
    "?", "help",
];

pub struct TopicsView {
    pub table: TableView,
    pub show_help: bool,
    pub topic_infos: Vec<TopicInfo>,
}

impl TopicsView {
    pub fn new() -> Self {
        Self {
            table: TableView::new(
                "Topics",
                vec![
                    "NAME".into(),
                    "MESSAGES".into(),
                    "PARTITIONS".into(),
                    "REPLICATION".into(),
                    "INTERNAL".into(),
                ],
            ),
            show_help: false,
            topic_infos: Vec::new(),
        }
    }

    pub fn load(&mut self, topics: Vec<TopicInfo>) {
        self.topic_infos = topics.clone();
        let rows = topics
            .into_iter()
            .map(|t| {
                vec![
                    t.name,
                    t.message_count.to_string(),
                    t.partitions.to_string(),
                    t.replication.to_string(),
                    if t.internal { "yes" } else { "" }.into(),
                ]
            })
            .collect();
        self.table.set_rows(rows);
    }

    pub fn selected_topic(&self) -> Option<&str> {
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

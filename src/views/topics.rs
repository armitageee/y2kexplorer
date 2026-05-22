use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::Frame;

use crate::kafka::TopicInfo;
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k ↑↓", "nav",
    "/", "filter",
    "r", "refresh",
    "Enter", "messages",
    "p", "partitions",
    "?", "help",
    "q", "quit",
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

    pub fn render(&mut self, frame: &mut Frame, area: Rect, cluster: &str, status: &str, loading: bool) {
        let chunks = Layout::vertical([
            Constraint::Min(3),
            Constraint::Length(1),
            Constraint::Length(if self.show_help { 2 } else { 1 }),
        ])
        .split(area);

        self.table.render(frame, chunks[0]);
        draw_status(frame, chunks[1], cluster, status, loading);
        if self.show_help {
            draw_help(frame, chunks[2], HELP);
        } else {
            draw_help(
                frame,
                chunks[2],
                &["Enter messages", "p partitions", "/ filter", "? help"],
            );
        }
    }
}

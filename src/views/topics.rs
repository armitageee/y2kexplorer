use ratatui::layout::Rect;
use ratatui::Frame;

use crate::kafka::TopicInfo;
use crate::labels::{format_labels, TopicLabelStore};
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k",
    "nav",
    "Space",
    "mark",
    "L",
    "add label",
    "U",
    "remove label",
    "D",
    "clear marks",
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
    "g",
    "groups",
    "1-4",
    "nav",
    "r",
    "refresh",
    "?",
    "help",
    "q",
    "quit",
];

const HINT: &[&str] = &[
    "Space", "mark", "L", "label", "Enter", "open", "/", "filter", "g", "groups", "?", "help",
];

pub struct TopicsView {
    pub table: TableView,
    pub show_help: bool,
    pub topic_infos: Vec<TopicInfo>,
    /// Фильтр по лейблу (из Labels view или `:label foo`).
    pub label_filter: Option<String>,
}

impl TopicsView {
    pub fn new() -> Self {
        Self {
            table: TableView::new(
                "Topics",
                vec![
                    "NAME".into(),
                    "LABELS".into(),
                    "MESSAGES".into(),
                    "PARTITIONS".into(),
                    "REPLICATION".into(),
                    "INTERNAL".into(),
                ],
            )
            .enable_multi_select(),
            show_help: false,
            topic_infos: Vec::new(),
            label_filter: None,
        }
    }

    pub fn set_label_filter(&mut self, label: Option<String>) {
        self.label_filter = label;
    }

    pub fn load_with_labels(
        &mut self,
        topics: Vec<TopicInfo>,
        store: &TopicLabelStore,
        cluster: &str,
    ) {
        self.topic_infos = topics;
        self.rebuild_rows(store, cluster);
    }

    /// Обновить таблицу после изменения лейблов (без refetch с брокера).
    pub fn refresh_labels(&mut self, store: &TopicLabelStore, cluster: &str) {
        self.rebuild_rows(store, cluster);
    }

    fn rebuild_rows(&mut self, store: &TopicLabelStore, cluster: &str) {
        let mut rows: Vec<Vec<String>> = self
            .topic_infos
            .iter()
            .map(|t| {
                let labels = store.labels_for(cluster, &t.name);
                vec![
                    t.name.clone(),
                    format_labels(&labels),
                    t.message_count.to_string(),
                    t.partitions.to_string(),
                    t.replication.to_string(),
                    if t.internal { "yes" } else { "" }.into(),
                ]
            })
            .collect();
        self.filter_rows(&mut rows);
        self.table.set_rows(rows);
    }

    fn filter_rows(&self, rows: &mut Vec<Vec<String>>) {
        if let Some(ref label) = self.label_filter {
            let needle = label.to_lowercase();
            rows.retain(|row| {
                row.get(1)
                    .map(|s| s.to_lowercase())
                    .is_some_and(|l| l.split(',').any(|part| part.trim() == needle.as_str()))
            });
        }
    }

    pub fn clear_label_filter(&mut self) {
        self.label_filter = None;
    }

    pub fn selected_topic(&self) -> Option<&str> {
        let row = self.table.selected_row()?;
        row.first().map(String::as_str)
    }

    pub fn target_topic_names(&self) -> Vec<String> {
        self.table.marked_or_current_first_col()
    }

    pub fn toggle_mark(&mut self) {
        self.table.toggle_mark();
    }

    pub fn clear_marks(&mut self) {
        self.table.clear_marks();
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
        let mut title = "Topics".to_string();
        if let Some(ref l) = self.label_filter {
            title = format!("{title}  @{l}");
        }
        self.table.title = title;
        self.table.render(frame, main);
        draw_status(frame, status_area, cluster, status, loading);
        if self.show_help {
            draw_help(frame, keys_area, HELP);
        } else {
            draw_help(frame, keys_area, HINT);
        }
    }
}

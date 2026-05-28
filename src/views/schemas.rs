use ratatui::layout::Rect;
use ratatui::Frame;

use crate::ui::{draw_help, draw_status, TableView};
use y2kexplorer::schema_registry::SchemaSubjectSummary;

const HELP: &[&str] = &[
    "j/k", "nav", "Enter", "schema", "/", "filter", "r", "refresh", "Esc", "back", "?", "help",
    "q", "quit",
];

const HINT: &[&str] = &[
    "Enter", "detail", "/", "filter", "r", "refresh", "?", "help",
];

pub struct SchemasView {
    pub table: TableView,
    pub show_help: bool,
    pub subjects: Vec<SchemaSubjectSummary>,
}

impl SchemasView {
    pub fn help_pairs(&self) -> &'static [&'static str] {
        if self.show_help {
            HELP
        } else {
            HINT
        }
    }

    pub fn new() -> Self {
        Self {
            table: TableView::new(
                "Schemas",
                vec![
                    "SUBJECT".into(),
                    "LATEST".into(),
                    "VERSIONS".into(),
                    "TYPE".into(),
                ],
            ),
            show_help: false,
            subjects: Vec::new(),
        }
    }

    pub fn load(&mut self, subjects: Vec<SchemaSubjectSummary>) {
        self.subjects = subjects.clone();
        let rows = subjects
            .into_iter()
            .map(|s| {
                vec![
                    s.subject,
                    s.latest_version.to_string(),
                    s.version_count.to_string(),
                    s.schema_type,
                ]
            })
            .collect();
        self.table.set_rows(rows);
    }

    pub fn selected_subject(&self) -> Option<&str> {
        let row = self.table.selected_row()?;
        row.first().map(String::as_str)
    }

    pub fn selected_summary(&self) -> Option<&SchemaSubjectSummary> {
        let name = self.selected_subject()?;
        self.subjects.iter().find(|s| s.subject == name)
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

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use y2kexplorer::schema_registry::SchemaVersionDetail;
use crate::ui::{draw_help, draw_status, theme};

const HELP: &[&str] = &[
    "j/k",
    "version",
    "u/d",
    "scroll",
    "PgUp/PgDn",
    "scroll fast",
    "r",
    "reload",
    "Esc",
    "back",
    "?",
    "help",
    "q",
    "quit",
];

const HINT: &[&str] = &["j/k", "version", "u/d", "scroll", "Esc", "back", "?", "help"];

pub struct SchemaDetailView {
    pub subject: String,
    pub title: String,
    pub versions: Vec<i32>,
    pub version_index: usize,
    pub detail: Option<SchemaVersionDetail>,
    pub schema_display: String,
    pub detail_scroll: usize,
    pub show_help: bool,
}

impl SchemaDetailView {
    pub fn new(subject: &str) -> Self {
        Self {
            subject: subject.to_string(),
            title: format!("Schema: {subject}"),
            versions: Vec::new(),
            version_index: 0,
            detail: None,
            schema_display: String::new(),
            detail_scroll: 0,
            show_help: false,
        }
    }

    pub fn set_versions(&mut self, versions: Vec<i32>) {
        self.versions = versions;
        self.version_index = self.versions.len().saturating_sub(1);
    }

    pub fn current_version(&self) -> Option<i32> {
        self.versions.get(self.version_index).copied()
    }

    pub fn set_detail(&mut self, detail: SchemaVersionDetail) {
        self.schema_display = pretty_schema(&detail.schema);
        self.detail_scroll = 0;
        self.detail = Some(detail);
    }

    pub fn next_version(&mut self) {
        if self.versions.is_empty() {
            return;
        }
        if self.version_index + 1 < self.versions.len() {
            self.version_index += 1;
        }
    }

    pub fn prev_version(&mut self) {
        if self.version_index > 0 {
            self.version_index -= 1;
        }
    }

    pub fn scroll_detail_down(&mut self, lines: usize) {
        self.detail_scroll = self.detail_scroll.saturating_add(lines);
    }

    pub fn scroll_detail_up(&mut self, lines: usize) {
        self.detail_scroll = self.detail_scroll.saturating_sub(lines);
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
        let chunks = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(5),
        ])
        .split(main);

        let meta = if let Some(d) = &self.detail {
            format!(
                "v{}  id={}  {}  ({}/{})",
                d.version,
                d.id,
                d.schema_type,
                self.version_index + 1,
                self.versions.len().max(1)
            )
        } else if loading {
            "loading…".into()
        } else {
            "no schema".into()
        };

        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(&self.subject, theme::title()),
                Span::raw("  "),
                Span::styled(meta, theme::value()),
            ]))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme::block_border())
                    .title(" subject ")
                    .title_style(theme::block_title()),
            ),
            chunks[0],
        );

        let lines: Vec<Line> = self
            .schema_display
            .lines()
            .skip(self.detail_scroll)
            .map(|l| Line::from(Span::styled(l, theme::row())))
            .collect();

        frame.render_widget(
            Paragraph::new(lines)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(theme::block_border())
                        .title(" schema ")
                        .title_style(theme::block_title()),
                )
                .wrap(Wrap { trim: false }),
            chunks[1],
        );

        draw_status(frame, status_area, cluster, status, loading);
        if self.show_help {
            draw_help(frame, keys_area, HELP);
        } else {
            draw_help(frame, keys_area, HINT);
        }
    }
}

fn pretty_schema(raw: &str) -> String {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) {
        serde_json::to_string_pretty(&v).unwrap_or_else(|_| raw.to_string())
    } else {
        raw.to_string()
    }
}

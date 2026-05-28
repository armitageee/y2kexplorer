use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::kafka_connect::ConnectorDetail;
use crate::ui::{draw_help, draw_status, theme};

const HELP: &[&str] = &[
    "P",
    "pause",
    "O",
    "resume",
    "R",
    "restart",
    "d",
    "delete",
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

const HINT: &[&str] = &[
    "P", "pause", "O", "resume", "R", "restart", "d", "delete", "u/d", "scroll", "Esc", "back",
];

pub struct ConnectorDetailView {
    pub name: String,
    pub title: String,
    pub detail: Option<ConnectorDetail>,
    pub body_display: String,
    pub detail_scroll: usize,
    pub show_help: bool,
}

impl ConnectorDetailView {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            title: format!("Connector: {name}"),
            detail: None,
            body_display: String::new(),
            detail_scroll: 0,
            show_help: false,
        }
    }

    pub fn set_detail(&mut self, detail: ConnectorDetail) {
        self.body_display = format_body(&detail);
        self.detail_scroll = 0;
        self.detail = Some(detail);
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
        let chunks = Layout::vertical([Constraint::Length(3), Constraint::Min(5)]).split(main);

        let meta = if let Some(d) = &self.detail {
            format!(
                "{}  {}  tasks={}  worker={}",
                d.kind,
                d.state,
                d.tasks.len(),
                d.worker_id.as_deref().unwrap_or("—")
            )
        } else if loading {
            "loading…".into()
        } else {
            "no detail".into()
        };

        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(&self.name, theme::title()),
                Span::raw("  "),
                Span::styled(meta, theme::value()),
            ]))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme::block_border())
                    .title(" connector ")
                    .title_style(theme::block_title()),
            ),
            chunks[0],
        );

        let lines: Vec<Line> = self
            .body_display
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
                        .title(" status / config ")
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

fn format_body(d: &ConnectorDetail) -> String {
    let mut out = String::from("=== tasks ===\n");
    if d.tasks.is_empty() {
        out.push_str("(none)\n");
    }
    for t in &d.tasks {
        out.push_str(&format!(
            "  #{}  {}  worker={}\n",
            t.id,
            t.state,
            t.worker_id.as_deref().unwrap_or("—")
        ));
        if let Some(trace) = &t.trace {
            out.push_str(&format!("    trace: {trace}\n"));
        }
    }
    out.push_str("\n=== config ===\n");
    out.push_str(&d.config_json);
    out
}

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use crate::kafka::FetchedMessage;
use crate::ui::{draw_help, draw_status, theme};

const HELP: &[&str] = &[
    "j/k", "nav",
    "b", "tail",
    "t", "head",
    "r", "reload",
    "Esc", "back",
    "q", "quit",
];

pub struct MessagesView {
    pub topic: String,
    pub title: String,
    pub messages: Vec<FetchedMessage>,
    pub list_state: ListState,
    pub detail_scroll: u16,
    pub from_end: bool,
    pub show_help: bool,
}

impl MessagesView {
    pub fn new(topic: impl Into<String>) -> Self {
        let topic = topic.into();
        let title = format!("Messages: {topic}");
        Self {
            title: title.clone(),
            topic,
            messages: Vec::new(),
            list_state: ListState::default().with_selected(Some(0)),
            detail_scroll: 0,
            from_end: true,
            show_help: false,
        }
    }

    pub fn load(&mut self, messages: Vec<FetchedMessage>) {
        self.messages = messages;
        if !self.messages.is_empty() {
            self.list_state.select(Some(0));
        }
    }

    pub fn selected(&self) -> Option<&FetchedMessage> {
        let i = self.list_state.selected()?;
        self.messages.get(i)
    }

    pub fn next(&mut self) {
        let len = self.messages.len();
        if len == 0 {
            return;
        }
        let i = match self.list_state.selected() {
            Some(i) => (i + 1).min(len - 1),
            None => 0,
        };
        self.list_state.select(Some(i));
        self.detail_scroll = 0;
    }

    pub fn prev(&mut self) {
        let i = self.list_state.selected().unwrap_or(0).saturating_sub(1);
        self.list_state.select(Some(i));
        self.detail_scroll = 0;
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, cluster: &str, status: &str, loading: bool) {
        let chunks = Layout::vertical([
            Constraint::Percentage(35),
            Constraint::Min(4),
            Constraint::Length(1),
            Constraint::Length(if self.show_help { 2 } else { 1 }),
        ])
        .split(area);

        let list_items: Vec<ListItem> = self
            .messages
            .iter()
            .map(|m| {
                let ts = m
                    .timestamp_ms
                    .map(|t| format_timestamp(t))
                    .unwrap_or_else(|| "-".into());
                let key = m
                    .key
                    .as_deref()
                    .map(|k| truncate(k, 24))
                    .unwrap_or_else(|| "<null>".into());
                ListItem::new(format!(
                    "p{:02} @ {:>12}  {}  {}",
                    m.partition, m.offset, ts, key
                ))
            })
            .collect();

        let mode = if self.from_end { "tail" } else { "head" };
        let list = List::new(list_items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("{} ({mode})", self.title)),
            )
            .highlight_style(theme::SELECTED)
            .highlight_symbol("▸ ");

        frame.render_stateful_widget(list, chunks[0], &mut self.list_state);

        let detail = self
            .selected()
            .map(format_message_detail)
            .unwrap_or_else(|| vec![Line::from("No messages")]);

        let detail_widget = Paragraph::new(detail)
            .wrap(Wrap { trim: false })
            .scroll((self.detail_scroll, 0))
            .block(Block::default().borders(Borders::ALL).title("detail"));
        frame.render_widget(detail_widget, chunks[1]);

        draw_status(frame, chunks[2], cluster, status, loading);
        if self.show_help {
            draw_help(frame, chunks[3], HELP);
        } else {
            draw_help(frame, chunks[3], &["b tail", "t head", "Esc back"]);
        }
    }
}

fn format_message_detail(m: &FetchedMessage) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(vec![
            Span::styled("partition ", theme::KEY),
            Span::raw(m.partition.to_string()),
            Span::raw("  "),
            Span::styled("offset ", theme::KEY),
            Span::raw(m.offset.to_string()),
        ]),
    ];
    if let Some(ts) = m.timestamp_ms {
        lines.push(Line::from(vec![
            Span::styled("timestamp ", theme::KEY),
            Span::raw(format_timestamp(ts)),
        ]));
    }
    if let Some(key) = &m.key {
        lines.push(Line::from(vec![
            Span::styled("key ", theme::KEY),
            Span::styled(key.clone(), theme::VALUE),
        ]));
    }
    if !m.headers.is_empty() {
        lines.push(Line::from(Span::styled("headers", theme::KEY)));
        for (k, v) in &m.headers {
            lines.push(Line::from(format!("  {k}: {v}")));
        }
    }
    lines.push(Line::from(Span::styled("payload", theme::KEY)));
    lines.push(Line::from(
        m.payload
            .clone()
            .unwrap_or_else(|| "<null>".into()),
    ));
    lines
}

fn format_timestamp(ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| ms.to_string())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(max).collect::<String>())
    }
}

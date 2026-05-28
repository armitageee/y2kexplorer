use std::collections::HashMap;

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use anyhow::{Context, Result};
use arboard::Clipboard;

use crate::kafka::FetchedMessage;
use crate::ui::{draw_help, draw_status, payload, theme};

const HELP: &[&str] = &[
    "j/k",
    "nav",
    ":",
    "command",
    "n",
    "produce",
    "b",
    "tail",
    "t",
    "head",
    "+/-",
    "limit ±50",
    "l",
    "set limit",
    "p",
    "partition",
    "i",
    "partitions info",
    "s",
    "sort time/offset",
    "f",
    "live follow",
    "[/]",
    "poll ±1s",
    "o",
    "toggle JSON pretty",
    "y",
    "copy message",
    "u/d",
    "scroll detail",
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
    ":",
    "context",
    "n",
    "produce",
    "b",
    "tail",
    "t",
    "head",
    "p",
    "partition",
    "f",
    "live",
    "y",
    "copy",
    "s",
    "sort",
    "Esc",
    "back",
    "?",
    "help",
];

pub struct MessagesView {
    pub topic: String,
    pub title: String,
    pub messages: Vec<FetchedMessage>,
    pub list_state: ListState,
    pub detail_scroll: u16,
    pub from_end: bool,
    pub show_help: bool,
    pub message_limit: usize,
    pub partition: Option<i32>,
    pub partition_ids: Vec<i32>,
    pub sort_by_time: bool,
    /// Pretty-print JSON в detail и при копировании.
    pub pretty_json: bool,
    pub live: bool,
    /// Следующий offset для чтения по партициям (live-poll).
    pub next_offsets: HashMap<i32, i64>,
}

impl MessagesView {
    pub fn new(topic: impl Into<String>, message_limit: usize) -> Self {
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
            message_limit,
            partition: None,
            partition_ids: Vec::new(),
            sort_by_time: true,
            pretty_json: true,
            live: false,
            next_offsets: HashMap::new(),
        }
    }

    pub fn list_title(&self) -> String {
        let mode = if self.live {
            "LIVE"
        } else if self.from_end {
            "tail"
        } else {
            "head"
        };
        let part = match self.partition {
            Some(p) => format!("p{p}"),
            None => "all".into(),
        };
        let sort = if self.sort_by_time { "time" } else { "offset" };
        let json = if self.pretty_json { "json+" } else { "json-" };
        format!(
            "{} [{mode} lim={} {part} sort={sort} {json}]",
            self.title, self.message_limit
        )
    }

    pub fn scroll_detail_up(&mut self, n: u16) {
        self.detail_scroll = self.detail_scroll.saturating_sub(n);
    }

    pub fn scroll_detail_down(&mut self, n: u16) {
        self.detail_scroll = self.detail_scroll.saturating_add(n);
    }

    pub fn copy_selected(&self) -> Result<usize> {
        let m = self.selected().context("no message selected")?;
        let text = message_clipboard_text(m, self.pretty_json);
        let len = text.len();
        Clipboard::new()
            .context("open clipboard")?
            .set_text(text)
            .context("copy to clipboard")?;
        Ok(len)
    }

    pub fn sync_next_offsets(&mut self) {
        for m in &self.messages {
            let next = m.offset.saturating_add(1);
            self.next_offsets
                .entry(m.partition)
                .and_modify(|o| *o = (*o).max(next))
                .or_insert(next);
        }
    }

    /// Добавить новые сообщения (live), убрать дубликаты, обрезать хвост буфера.
    pub fn append_live(
        &mut self,
        new: Vec<FetchedMessage>,
        limit: usize,
        sort_by_time: bool,
        single_partition: bool,
    ) -> usize {
        if new.is_empty() {
            return 0;
        }
        let before = self.messages.len();
        self.messages.extend(new);
        self.messages.sort_by_key(|a| (a.partition, a.offset));
        self.messages
            .dedup_by(|a, b| a.partition == b.partition && a.offset == b.offset);
        sort_messages_in_place(&mut self.messages, single_partition, sort_by_time);
        if self.messages.len() > limit {
            self.messages.truncate(limit);
        }
        self.sync_next_offsets();
        let added = self.messages.len().saturating_sub(before);
        if !self.messages.is_empty() {
            self.list_state.select(Some(0));
        }
        self.detail_scroll = 0;
        added
    }

    pub fn cycle_partition(&mut self) {
        if self.partition_ids.is_empty() {
            self.partition = None;
            return;
        }
        self.partition = match self.partition {
            None => Some(self.partition_ids[0]),
            Some(current) => {
                if let Some(pos) = self.partition_ids.iter().position(|&id| id == current) {
                    if pos + 1 < self.partition_ids.len() {
                        Some(self.partition_ids[pos + 1])
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };
    }

    pub fn load(&mut self, messages: Vec<FetchedMessage>) {
        self.messages = messages;
        self.sync_next_offsets();
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
        let body = Layout::vertical([Constraint::Percentage(35), Constraint::Min(8)]).split(main);
        let list_area = body[0];
        let detail_area = body[1];

        let list_items: Vec<ListItem> = self
            .messages
            .iter()
            .map(|m| {
                let ts = m
                    .timestamp_ms
                    .map(format_timestamp)
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

        let list = List::new(list_items)
            .block(theme::block(self.list_title()))
            .highlight_style(theme::selected())
            .highlight_symbol("▸ ");

        frame.render_stateful_widget(list, list_area, &mut self.list_state);

        let pretty = self.pretty_json;
        let detail = self
            .selected()
            .map(|m| format_message_detail(m, pretty))
            .unwrap_or_else(|| vec![Line::from("No messages")]);

        let detail_widget = Paragraph::new(detail)
            .wrap(Wrap { trim: false })
            .scroll((self.detail_scroll, 0))
            .block(theme::block("detail"));
        frame.render_widget(detail_widget, detail_area);

        draw_status(frame, status_area, cluster, status, loading);
        if self.show_help {
            draw_help(frame, keys_area, HELP);
        } else {
            draw_help(frame, keys_area, HINT);
        }
    }
}

fn format_message_detail(m: &FetchedMessage, pretty_json: bool) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(vec![
        Span::styled("partition ", theme::key()),
        Span::raw(m.partition.to_string()),
        Span::raw("  "),
        Span::styled("offset ", theme::key()),
        Span::raw(m.offset.to_string()),
    ])];
    if let Some(ts) = m.timestamp_ms {
        lines.push(Line::from(vec![
            Span::styled("timestamp ", theme::key()),
            Span::raw(format_timestamp(ts)),
        ]));
    }
    if let Some(key) = &m.key {
        lines.push(Line::from(Span::styled("key", theme::key())));
        if pretty_json {
            if let Some(pretty) = payload::try_pretty_json(key) {
                lines.extend(payload::payload_lines(&pretty, false));
            } else {
                lines.push(Line::from(Span::styled(key.clone(), theme::value())));
            }
        } else {
            lines.push(Line::from(Span::styled(key.clone(), theme::value())));
        }
    }
    if !m.headers.is_empty() {
        lines.push(Line::from(Span::styled("headers", theme::key())));
        for (k, v) in &m.headers {
            lines.push(Line::from(format!("  {k}: {v}")));
        }
    }
    lines.push(Line::from(Span::styled("payload", theme::key())));
    match &m.payload {
        Some(p) => lines.extend(payload::payload_lines(p, pretty_json)),
        None => lines.push(Line::from("<null>")),
    }
    lines
}

pub fn message_clipboard_text(m: &FetchedMessage, pretty_json: bool) -> String {
    let mut out = String::new();
    out.push_str(&format!("partition: {}\n", m.partition));
    out.push_str(&format!("offset: {}\n", m.offset));
    if let Some(ts) = m.timestamp_ms {
        out.push_str(&format!("timestamp: {}\n", format_timestamp(ts)));
    }
    if let Some(key) = &m.key {
        out.push_str("key:\n");
        out.push_str(&format_optional_json(key, pretty_json));
        out.push('\n');
    }
    if !m.headers.is_empty() {
        out.push_str("headers:\n");
        for (k, v) in &m.headers {
            out.push_str(&format!("  {k}: {v}\n"));
        }
    }
    out.push_str("payload:\n");
    match &m.payload {
        Some(p) => out.push_str(&format_optional_json(p, pretty_json)),
        None => out.push_str("<null>"),
    }
    out
}

fn format_optional_json(raw: &str, pretty: bool) -> String {
    if pretty {
        payload::try_pretty_json(raw).unwrap_or_else(|| raw.to_string())
    } else {
        raw.to_string()
    }
}

fn format_timestamp(ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| ms.to_string())
}

fn sort_messages_in_place(
    messages: &mut [FetchedMessage],
    single_partition: bool,
    sort_by_time: bool,
) {
    if single_partition {
        messages.sort_by_key(|m| std::cmp::Reverse(m.offset));
    } else if sort_by_time {
        messages.sort_by(|a, b| match (a.timestamp_ms, b.timestamp_ms) {
            (Some(ta), Some(tb)) => tb.cmp(&ta),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => (b.partition, b.offset).cmp(&(a.partition, a.offset)),
        });
    } else {
        messages.sort_by_key(|m| std::cmp::Reverse((m.partition, m.offset)));
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(max).collect::<String>())
    }
}

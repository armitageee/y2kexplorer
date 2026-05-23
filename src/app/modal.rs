use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::ui::theme;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModalField {
    First,
    Second,
    Third,
}

#[derive(Debug, Clone)]
pub enum Modal {
    Filter,
    Command,
    Produce {
        topic: String,
        key: String,
        payload: String,
        field: ModalField,
    },
    CreateTopic {
        name: String,
        partitions: String,
        field: ModalField,
    },
    DeleteConfirm {
        topic: String,
    },
    MessageLimit {
        value: String,
    },
    DeleteGroupConfirm {
        group: String,
    },
    /// Удалить лейбл со всех топиков кластера.
    DeleteLabelConfirm {
        label: String,
        topic_count: usize,
    },
    /// Reset offsets для всех (topic, partition), на которые группа коммитила.
    /// `spec` — одна из строк: `earliest`, `latest`, `offset:N`, `timestamp:UNIX_MS`.
    ResetOffsets {
        group: String,
        spec: String,
    },
    /// Добавить/удалить лейбл у выбранных топиков.
    TopicLabel {
        label: String,
        add: bool,
        topic_count: usize,
    },
}

impl Modal {
    pub fn title(&self) -> &'static str {
        match self {
            Modal::Filter => "Filter topics",
            Modal::Command => "Command",
            Modal::Produce { .. } => "Produce message",
            Modal::CreateTopic { .. } => "Create topic",
            Modal::DeleteConfirm { .. } => "Delete topic",
            Modal::MessageLimit { .. } => "Message limit",
            Modal::DeleteGroupConfirm { .. } => "Delete consumer group",
            Modal::DeleteLabelConfirm { .. } => "Delete label",
            Modal::ResetOffsets { .. } => "Reset offsets",
            Modal::TopicLabel { add, .. } => {
                if *add {
                    "Add label"
                } else {
                    "Remove label"
                }
            }
        }
    }

    pub fn next_field(&mut self) {
        match self {
            Modal::Produce { field, .. } => {
                *field = match field {
                    ModalField::First => ModalField::Second,
                    ModalField::Second | ModalField::Third => ModalField::Third,
                };
            }
            Modal::CreateTopic { field, .. } => {
                *field = match field {
                    ModalField::First => ModalField::Second,
                    _ => ModalField::First,
                };
            }
            _ => {}
        }
    }

    pub fn push_char(&mut self, c: char) {
        match self {
            Modal::Filter | Modal::Command => {}
            Modal::Produce {
                key,
                payload,
                field,
                ..
            } => match field {
                ModalField::First => key.push(c),
                _ => payload.push(c),
            },
            Modal::CreateTopic {
                name,
                partitions,
                field,
            } => match field {
                ModalField::First => name.push(c),
                _ => partitions.push(c),
            },
            Modal::DeleteConfirm { .. } => {}
            Modal::MessageLimit { value } => value.push(c),
            Modal::DeleteGroupConfirm { .. } => {}
            Modal::DeleteLabelConfirm { .. } => {}
            Modal::ResetOffsets { spec, .. } => spec.push(c),
            Modal::TopicLabel { label, .. } => label.push(c),
        }
    }

    pub fn backspace(&mut self) {
        match self {
            Modal::Filter | Modal::Command => {}
            Modal::Produce {
                key,
                payload,
                field,
                ..
            } => match field {
                ModalField::First => {
                    key.pop();
                }
                _ => {
                    payload.pop();
                }
            },
            Modal::CreateTopic {
                name,
                partitions,
                field,
            } => match field {
                ModalField::First => {
                    name.pop();
                }
                _ => {
                    partitions.pop();
                }
            },
            Modal::DeleteConfirm { .. } => {}
            Modal::MessageLimit { value } => {
                value.pop();
            }
            Modal::DeleteGroupConfirm { .. } => {}
            Modal::DeleteLabelConfirm { .. } => {}
            Modal::ResetOffsets { spec, .. } => {
                spec.pop();
            }
            Modal::TopicLabel { label, .. } => {
                label.pop();
            }
        }
    }

    pub fn is_yes(&self, c: char) -> bool {
        matches!(
            self,
            Modal::DeleteConfirm { .. }
                | Modal::DeleteGroupConfirm { .. }
                | Modal::DeleteLabelConfirm { .. }
        ) && matches!(c, 'y' | 'Y')
    }
}

pub fn draw_modal(frame: &mut Frame, area: Rect, modal: &Modal, extra_buf: Option<&str>) {
    let popup_w = area.width.clamp(40, 72);
    let popup_h = match modal {
        Modal::DeleteConfirm { .. }
        | Modal::DeleteGroupConfirm { .. }
        | Modal::DeleteLabelConfirm { .. } => 7,
        Modal::Produce { .. } => 11,
        Modal::CreateTopic { .. } => 9,
        Modal::ResetOffsets { .. } => 9,
        Modal::Filter | Modal::Command | Modal::MessageLimit { .. } => 5,
        Modal::TopicLabel { .. } => 7,
    };
    let popup = centered_rect(popup_w, popup_h, area);
    frame.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::modal_border())
        .title(format!(" {} ", modal.title()))
        .title_style(theme::block_title());

    let lines = match modal {
        Modal::Filter => {
            let buf = extra_buf.unwrap_or("");
            vec![
                Line::from(Span::styled("pattern: ", theme::modal_label())),
                Line::from(vec![
                    Span::styled(buf, theme::modal_input()),
                    Span::styled("_", theme::modal_cursor()),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "Enter apply · Esc cancel",
                    theme::footer_hint(),
                )),
            ]
        }
        Modal::Command => {
            let buf = extra_buf.unwrap_or("");
            vec![
                Line::from(vec![
                    Span::styled(": ", theme::modal_label()),
                    Span::styled(buf, theme::modal_input()),
                    Span::styled("_", theme::modal_cursor()),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "context <name> · clusters · help  (Enter run, Esc cancel)",
                    theme::footer_hint(),
                )),
            ]
        }
        Modal::Produce {
            topic,
            key,
            payload,
            field,
        } => {
            let mut out = vec![Line::from(vec![
                Span::styled("topic: ", theme::modal_label()),
                Span::styled(topic.as_str(), theme::value()),
            ])];
            out.push(field_line("key", key, *field == ModalField::First));
            out.push(field_line("payload", payload, *field != ModalField::First));
            out.push(Line::from(""));
            out.push(Line::from(Span::styled(
                "Tab next field · Enter send · Esc cancel",
                theme::footer_hint(),
            )));
            out
        }
        Modal::CreateTopic {
            name,
            partitions,
            field,
        } => {
            vec![
                field_line("name", name, *field == ModalField::First),
                field_line("partitions", partitions, *field != ModalField::First),
                Line::from(""),
                Line::from(Span::styled(
                    "Tab next · Enter create · Esc cancel",
                    theme::footer_hint(),
                )),
            ]
        }
        Modal::DeleteConfirm { topic } => vec![
            Line::from(Span::styled(
                format!("Delete topic \"{topic}\"?"),
                theme::error(),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "y confirm · n/Esc cancel",
                theme::footer_hint(),
            )),
        ],
        Modal::MessageLimit { value } => vec![
            field_line("limit", value, true),
            Line::from(""),
            Line::from(Span::styled(
                "10–10000 · Enter apply · Esc cancel",
                theme::footer_hint(),
            )),
        ],
        Modal::DeleteGroupConfirm { group } => vec![
            Line::from(Span::styled(
                format!("Delete consumer group \"{group}\"?"),
                theme::error(),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "y confirm · n/Esc cancel",
                theme::footer_hint(),
            )),
        ],
        Modal::DeleteLabelConfirm { label, topic_count } => vec![
            Line::from(Span::styled(
                format!("Remove label \"{label}\" from all topics?"),
                theme::error(),
            )),
            Line::from(Span::styled(
                format!("({topic_count} topic(s) in this cluster)"),
                theme::value(),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "y confirm · n/Esc cancel",
                theme::footer_hint(),
            )),
        ],
        Modal::ResetOffsets { group, spec } => vec![
            Line::from(vec![
                Span::styled("group: ", theme::modal_label()),
                Span::styled(group.as_str(), theme::value()),
            ]),
            field_line("spec", spec, true),
            Line::from(""),
            Line::from(Span::styled(
                "earliest · latest · offset:N · timestamp:UNIX_MS",
                theme::footer_hint(),
            )),
            Line::from(Span::styled(
                "Enter apply · Esc cancel  (group must be Empty/Dead)",
                theme::footer_hint(),
            )),
        ],
        Modal::TopicLabel {
            label,
            add,
            topic_count,
        } => {
            let action = if *add { "add to" } else { "remove from" };
            vec![
                Line::from(Span::styled(
                    format!("{action} {topic_count} topic(s)"),
                    theme::value(),
                )),
                field_line("label", label, true),
                Line::from(""),
                Line::from(Span::styled(
                    "lowercase, no spaces · Enter apply · Esc cancel",
                    theme::footer_hint(),
                )),
            ]
        }
    };

    let widget = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false })
        .alignment(Alignment::Left);
    frame.render_widget(widget, popup);
}

fn field_line(label: &str, value: &str, active: bool) -> Line<'static> {
    let mut spans = vec![Span::styled(format!("{label}: "), theme::modal_label())];
    if active {
        spans.push(Span::styled(value.to_string(), theme::modal_input()));
        spans.push(Span::styled("_", theme::modal_cursor()));
    } else {
        let text = if value.is_empty() {
            "(empty)".to_string()
        } else {
            value.to_string()
        };
        spans.push(Span::styled(text, theme::value()));
    }
    Line::from(spans)
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

pub fn footer_rows(show_full_help: bool) -> u16 {
    if show_full_help {
        3
    } else {
        2
    }
}

pub fn layout_main(area: Rect, show_full_help: bool) -> [Rect; 3] {
    let footer = footer_rows(show_full_help);
    let chunks = Layout::vertical([
        Constraint::Min(3),
        Constraint::Length(2),
        Constraint::Length(footer),
    ])
    .split(area);
    [chunks[0], chunks[1], chunks[2]]
}

/// Sidebar (14 cols) + main/status/keys, когда на корневом экране навигации.
pub fn layout_app(
    area: Rect,
    show_sidebar: bool,
    show_full_help: bool,
) -> (Option<Rect>, [Rect; 3]) {
    if !show_sidebar {
        return (None, layout_main(area, show_full_help));
    }
    let chunks = Layout::horizontal([Constraint::Length(14), Constraint::Min(10)]).split(area);
    (Some(chunks[0]), layout_main(chunks[1], show_full_help))
}

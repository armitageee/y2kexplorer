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
    DeleteConfirm { topic: String },
}

impl Modal {
    pub fn title(&self) -> &'static str {
        match self {
            Modal::Filter => "Filter topics",
            Modal::Command => "Command",
            Modal::Produce { .. } => "Produce message",
            Modal::CreateTopic { .. } => "Create topic",
            Modal::DeleteConfirm { .. } => "Delete topic",
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
            Modal::Produce { key, payload, field, .. } => match field {
                ModalField::First => key.push(c),
                _ => payload.push(c),
            },
            Modal::CreateTopic { name, partitions, field } => match field {
                ModalField::First => name.push(c),
                _ => partitions.push(c),
            },
            Modal::DeleteConfirm { .. } => {}
        }
    }

    pub fn backspace(&mut self) {
        match self {
            Modal::Filter | Modal::Command => {}
            Modal::Produce { key, payload, field, .. } => match field {
                ModalField::First => {
                    key.pop();
                }
                _ => {
                    payload.pop();
                }
            },
            Modal::CreateTopic { name, partitions, field } => match field {
                ModalField::First => {
                    name.pop();
                }
                _ => {
                    partitions.pop();
                }
            },
            Modal::DeleteConfirm { .. } => {}
        }
    }

    pub fn is_yes(&self, c: char) -> bool {
        matches!(self, Modal::DeleteConfirm { .. }) && matches!(c, 'y' | 'Y')
    }
}

pub fn draw_modal(frame: &mut Frame, area: Rect, modal: &Modal, extra_buf: Option<&str>) {
    let popup_w = area.width.min(72).max(40);
    let popup_h = match modal {
        Modal::DeleteConfirm { .. } => 7,
        Modal::Produce { .. } => 11,
        Modal::CreateTopic { .. } => 9,
        Modal::Filter | Modal::Command => 5,
    };
    let popup = centered_rect(popup_w, popup_h, area);
    frame.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::MODAL_BORDER)
        .title(format!(" {} ", modal.title()))
        .title_style(theme::BLOCK_TITLE);

    let lines = match modal {
        Modal::Filter => {
            let buf = extra_buf.unwrap_or("");
            vec![
                Line::from(Span::styled("pattern: ", theme::MODAL_LABEL)),
                Line::from(vec![
                    Span::styled(buf, theme::MODAL_INPUT),
                    Span::styled("_", theme::MODAL_CURSOR),
                ]),
                Line::from(""),
                Line::from(Span::styled("Enter apply · Esc cancel", theme::FOOTER_HINT)),
            ]
        }
        Modal::Command => {
            let buf = extra_buf.unwrap_or("");
            vec![
                Line::from(vec![
                    Span::styled(": ", theme::MODAL_LABEL),
                    Span::styled(buf, theme::MODAL_INPUT),
                    Span::styled("_", theme::MODAL_CURSOR),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "context <name> · clusters · help  (Enter run, Esc cancel)",
                    theme::FOOTER_HINT,
                )),
            ]
        }
        Modal::Produce { topic, key, payload, field } => {
            let mut out = vec![Line::from(vec![
                Span::styled("topic: ", theme::MODAL_LABEL),
                Span::styled(topic.as_str(), theme::VALUE),
            ])];
            out.push(field_line("key", key, *field == ModalField::First));
            out.push(field_line("payload", payload, *field != ModalField::First));
            out.push(Line::from(""));
            out.push(Line::from(Span::styled(
                "Tab next field · Enter send · Esc cancel",
                theme::FOOTER_HINT,
            )));
            out
        }
        Modal::CreateTopic { name, partitions, field } => {
            vec![
                field_line("name", name, *field == ModalField::First),
                field_line("partitions", partitions, *field != ModalField::First),
                Line::from(""),
                Line::from(Span::styled(
                    "Tab next · Enter create · Esc cancel",
                    theme::FOOTER_HINT,
                )),
            ]
        }
        Modal::DeleteConfirm { topic } => vec![
            Line::from(Span::styled(
                format!("Delete topic \"{topic}\"?"),
                theme::ERROR,
            )),
            Line::from(""),
            Line::from(Span::styled("y confirm · n/Esc cancel", theme::FOOTER_HINT)),
        ],
    };

    let widget = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false })
        .alignment(Alignment::Left);
    frame.render_widget(widget, popup);
}

fn field_line(label: &str, value: &str, active: bool) -> Line<'static> {
    let mut spans = vec![Span::styled(format!("{label}: "), theme::MODAL_LABEL)];
    if active {
        spans.push(Span::styled(value.to_string(), theme::MODAL_INPUT));
        spans.push(Span::styled("_", theme::MODAL_CURSOR));
    } else {
        let text = if value.is_empty() {
            "(empty)".to_string()
        } else {
            value.to_string()
        };
        spans.push(Span::styled(text, theme::VALUE));
    }
    Line::from(spans)
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

pub fn footer_rows(show_full_help: bool) -> u16 {
    if show_full_help { 3 } else { 2 }
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

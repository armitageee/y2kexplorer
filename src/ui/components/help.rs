use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::ui::theme;

/// `pairs`: ["key", "desc", "key2", "desc2", ...]
pub fn draw_help(frame: &mut Frame, area: Rect, pairs: &[&str]) {
    let mut spans = Vec::new();
    let iter = pairs.chunks(2);
    for chunk in iter {
        if !spans.is_empty() {
            spans.push(Span::styled("  │  ", theme::footer_hint()));
        }
        if let Some(key) = chunk.first() {
            spans.push(Span::styled(format!(" {key} "), theme::footer_key()));
        }
        if let Some(desc) = chunk.get(1) {
            spans.push(Span::styled(*desc, theme::footer_hint()));
        }
    }
    let widget = Paragraph::new(Line::from(spans))
        .style(theme::footer())
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(theme::footer())
                .title(" keys ")
                .title_style(theme::footer_title()),
        );
    frame.render_widget(widget, area);
}

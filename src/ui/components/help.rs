use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::ui::theme;

/// `pairs`: ["key", "desc", "key2", "desc2", ...]
pub fn draw_help(frame: &mut Frame, area: Rect, pairs: &[&str]) {
    let mut spans = Vec::new();
    let mut iter = pairs.chunks(2);
    while let Some(chunk) = iter.next() {
        if !spans.is_empty() {
            spans.push(Span::styled("  │  ", theme::FOOTER_HINT));
        }
        if let Some(key) = chunk.first() {
            spans.push(Span::styled(format!(" {key} "), theme::FOOTER_KEY));
        }
        if let Some(desc) = chunk.get(1) {
            spans.push(Span::styled(*desc, theme::FOOTER_HINT));
        }
    }
    let widget = Paragraph::new(Line::from(spans))
        .style(theme::FOOTER)
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(theme::FOOTER)
                .title(" keys ")
                .title_style(theme::FOOTER_TITLE),
        );
    frame.render_widget(widget, area);
}

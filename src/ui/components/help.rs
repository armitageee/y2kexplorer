use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::ui::theme;

pub fn draw_help(frame: &mut Frame, area: Rect, lines: &[&str]) {
    let text = lines.join("  ");
    let widget = Paragraph::new(text)
        .style(theme::STATUS)
        .block(Block::default().borders(Borders::TOP).title("keys"));
    frame.render_widget(widget, area);
}

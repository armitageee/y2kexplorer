use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::ui::theme;

pub fn draw_status(frame: &mut Frame, area: Rect, cluster: &str, message: &str, loading: bool) {
    let status = if loading {
        format!("{cluster}  ⏳ {message}")
    } else {
        format!("{cluster}  {message}")
    };
    let widget = Paragraph::new(status)
        .style(theme::STATUS)
        .block(Block::default().borders(Borders::TOP).title("status"));
    frame.render_widget(widget, area);
}

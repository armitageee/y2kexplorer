use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::ui::theme;

pub fn draw_status(frame: &mut Frame, area: Rect, cluster: &str, message: &str, loading: bool) {
    let indicator = if loading { "⏳ " } else { "● " };
    let line = Line::from(vec![
        Span::styled(format!("{indicator}{cluster}"), theme::FOOTER_TITLE),
        Span::styled(" │ ", theme::FOOTER_HINT),
        Span::styled(message, theme::FOOTER),
    ]);
    let widget = Paragraph::new(line).style(theme::FOOTER).block(
        Block::default()
            .borders(Borders::TOP)
            .border_style(theme::FOOTER)
            .title(" status ")
            .title_style(theme::FOOTER_TITLE),
    );
    frame.render_widget(widget, area);
}

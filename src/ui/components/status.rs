use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::ui::theme;

pub fn draw_status(frame: &mut Frame, area: Rect, cluster: &str, message: &str, loading: bool) {
    let indicator = if loading { "⏳ " } else { "● " };
    let line = Line::from(vec![
        Span::styled(format!("{indicator}{cluster}"), theme::footer_title()),
        Span::styled(" │ ", theme::footer_hint()),
        Span::styled(message, theme::footer()),
    ]);
    let widget = Paragraph::new(line).style(theme::footer()).block(
        Block::default()
            .borders(Borders::TOP)
            .border_style(theme::footer())
            .title(" status ")
            .title_style(theme::footer_title()),
    );
    frame.render_widget(widget, area);
}

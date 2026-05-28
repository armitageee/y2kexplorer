use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::ui::theme;
use crate::views::Screen;

const ITEMS: [(Screen, &str); 7] = [
    (Screen::Topics, "Topics"),
    (Screen::Groups, "Groups"),
    (Screen::Labels, "Labels"),
    (Screen::Contexts, "Contexts"),
    (Screen::Acls, "ACLs"),
    (Screen::Schemas, "Schemas"),
    (Screen::Connectors, "Connect"),
];

pub fn draw_sidebar(frame: &mut Frame, area: Rect, active: Screen) {
    let block = Block::default()
        .borders(Borders::RIGHT)
        .border_style(theme::block_border())
        .title(" nav ")
        .title_style(theme::block_title());

    let lines: Vec<Line> = ITEMS
        .iter()
        .enumerate()
        .map(|(i, (screen, label))| {
            let key = (b'1' + i as u8) as char;
            let is_active = *screen == active;
            let style = if is_active {
                theme::selected()
            } else {
                theme::row()
            };
            Line::from(vec![
                Span::styled(format!("{key} "), theme::key()),
                Span::styled(*label, style),
            ])
        })
        .collect();

    frame.render_widget(Paragraph::new(lines).block(block), area);
}

use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
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
        .border_type(BorderType::Rounded)
        .border_style(theme::block_border_focused())
        .title(" nav ")
        .title_style(theme::block_title());

    let lines: Vec<Line> = ITEMS
        .iter()
        .enumerate()
        .map(|(i, (screen, label))| {
            let key = (b'1' + i as u8) as char;
            let is_active = *screen == active;
            let mut spans = vec![Span::styled(format!("{key} "), theme::key())];
            if is_active {
                spans.push(Span::styled(
                    format!("▸ {label}"),
                    theme::selected().patch(theme::row()),
                ));
            } else {
                spans.push(Span::styled(
                    format!("  {label}"),
                    theme::row().add_modifier(Modifier::DIM),
                ));
            }
            Line::from(spans)
        })
        .collect();

    frame.render_widget(Paragraph::new(lines).block(block), area);
}

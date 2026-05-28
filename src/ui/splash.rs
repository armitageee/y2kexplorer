//! Минималистичный splash при старте (в духе eilmeldung — без декоративного шума).

use std::time::Duration;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use crate::ui::theme;

/// Сколько секунд держится splash, если пользователь ничего не жмёт.
pub const SPLASH_DURATION: Duration = Duration::from_millis(1400);

const BANNER: &[&str] = &[
    "██╗   ██╗ ██████╗ ██╗  ██╗",
    "╚██╗ ██╔╝ ╚════██╗██║ ██╔╝",
    " ╚████╔╝   █████╔╝█████╔╝ ",
    "  ╚██╔╝   ██╔═══╝ ██╔═██╗ ",
    "   ██║   ███████╗██║  ██╗ ",
    "   ╚═╝   ╚══════╝╚═╝  ╚═╝ ",
];

pub fn draw_splash(frame: &mut Frame, area: Rect) {
    let banner_h: u16 = (BANNER.len() as u16) + 4;
    let chunks = Layout::vertical([
        Constraint::Min(0),
        Constraint::Length(banner_h),
        Constraint::Min(0),
    ])
    .split(area);
    let body = chunks[1];

    let mut lines: Vec<Line> = Vec::with_capacity(banner_h as usize);
    for row in BANNER {
        lines.push(Line::from(Span::styled(*row, theme::title())).alignment(Alignment::Center));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled("explorer", theme::header())).alignment(Alignment::Center));
    lines.push(Line::from(""));
    lines.push(
        Line::from(Span::styled("kafka terminal ui", theme::tagline()))
            .alignment(Alignment::Center),
    );
    lines.push(Line::from(""));
    lines.push(
        Line::from(Span::styled(
            format!(
                "theme: {}  ·  press any key",
                theme::active_palette().label()
            ),
            theme::tagline(),
        ))
        .alignment(Alignment::Center),
    );

    let widget = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: false });
    frame.render_widget(widget, body);
}

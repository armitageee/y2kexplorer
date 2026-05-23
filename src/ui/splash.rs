//! Y2K/PS2-стилизованный splash-экран при старте приложения.
//!
//! Показывает большой ASCII-баннер с tagline-ом «kafka, but make it ps2».
//! Сам себя гасит через `SPLASH_DURATION`, либо при первом нажатии любой клавиши.

use std::time::Duration;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use crate::ui::theme;

/// Сколько секунд держится splash, если пользователь ничего не жмёт.
pub const SPLASH_DURATION: Duration = Duration::from_millis(1800);

/// Большой ASCII-баннер «Y2K» (figlet ANSI-shadow).
const BANNER: &[&str] = &[
    " ██╗   ██╗ ██████╗ ██╗  ██╗",
    " ╚██╗ ██╔╝ ╚════██╗██║ ██╔╝",
    "  ╚████╔╝   █████╔╝█████╔╝ ",
    "   ╚██╔╝   ██╔═══╝ ██╔═██╗ ",
    "    ██║   ███████╗██║  ██╗ ",
    "    ╚═╝   ╚══════╝╚═╝  ╚═╝ ",
];

const SPARKLE_LINE: &str = "✦ · ✦ · ✦ · ✦ · ✦ · ✦ · ✦ · ✦ · ✦";

pub fn draw_splash(frame: &mut Frame, area: Rect) {
    // Вертикально центрируем содержимое: верхний и нижний spacer + блок баннера.
    let banner_h: u16 = (BANNER.len() as u16) + 6;
    let chunks = Layout::vertical([
        Constraint::Min(0),
        Constraint::Length(banner_h),
        Constraint::Min(0),
    ])
    .split(area);
    let body = chunks[1];

    let mut lines: Vec<Line> = Vec::with_capacity(banner_h as usize);
    lines.push(
        Line::from(Span::styled(SPARKLE_LINE, theme::sparkle())).alignment(Alignment::Center),
    );
    lines.push(Line::from(""));
    for row in BANNER {
        lines.push(Line::from(Span::styled(*row, theme::title())).alignment(Alignment::Center));
    }
    lines.push(Line::from(""));
    lines.push(
        Line::from(Span::styled("e x p l o r e r", theme::header())).alignment(Alignment::Center),
    );
    lines.push(Line::from(""));
    lines.push(
        Line::from(Span::styled("kafka, but make it ps2", theme::tagline()))
            .alignment(Alignment::Center),
    );
    lines.push(Line::from(""));
    lines.push(
        Line::from(Span::styled("[ press any key ]", theme::footer_hint()))
            .alignment(Alignment::Center),
    );

    let widget = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: false });
    frame.render_widget(widget, body);
}

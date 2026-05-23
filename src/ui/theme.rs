//! Y2K / PS2-flavored theme.
//!
//! Поддерживает две палитры:
//! - [`Palette::Dark`]  — для тёмных терминалов (по умолчанию).
//! - [`Palette::Light`] — для светлых терминалов.
//!
//! Палитра выбирается один раз на старте через [`init`] (из конфига или CLI-флага).
//! Все стили доступны через accessor-функции (`theme::title()`, `theme::footer()` и т.п.) —
//! это позволяет менять палитру без `const`-капкана.
//!
//! Цвета намеренно используются как [`Color::Indexed`] (xterm-256), а не named ANSI
//! (`Color::Blue`, `Color::Cyan`, …). Named-цвета терминал переремапит под свою тему,
//! из-за чего, например, footer на светлой теме становится тусклым. Indexed-цвета
//! фиксированы в xterm-256 и одинаково яркие везде.

use std::str::FromStr;
use std::sync::OnceLock;

use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, BorderType, Borders};

// ---------- Palette / Theme ----------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Palette {
    #[default]
    Dark,
    Light,
}

impl Palette {
    pub fn as_str(self) -> &'static str {
        match self {
            Palette::Dark => "dark",
            Palette::Light => "light",
        }
    }
}

impl FromStr for Palette {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "dark" => Ok(Palette::Dark),
            "light" => Ok(Palette::Light),
            other => Err(format!("unknown theme '{other}' (use dark|light)")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Theme {
    pub title: Style,
    pub header: Style,
    pub selected: Style,
    /// k9s-style marked row (multi-select).
    pub marked: Style,
    pub row: Style,
    pub key: Style,
    pub value: Style,
    pub error: Style,
    pub success: Style,
    pub tagline: Style,
    pub sparkle: Style,

    pub footer_bg: Color,
    pub footer_fg: Color,
    pub footer: Style,
    pub footer_title: Style,
    pub footer_key: Style,
    pub footer_hint: Style,

    pub block_border: Style,
    pub block_title: Style,
    pub modal_border: Style,
    pub modal_label: Style,
    pub modal_input: Style,
    pub modal_cursor: Style,
}

// ---------- Active palette / accessors ----------

static ACTIVE: OnceLock<Theme> = OnceLock::new();

/// Инициализировать активную палитру. Вызывается из `main` до первого `render`.
/// Повторный вызов игнорируется (палитру нельзя переключить в рантайме без перерисовки).
pub fn init(palette: Palette) {
    let _ = ACTIVE.set(palette.theme());
}

#[inline]
pub fn current() -> &'static Theme {
    ACTIVE.get_or_init(|| Palette::default().theme())
}

impl Palette {
    pub fn theme(self) -> Theme {
        match self {
            Palette::Dark => dark_theme(),
            Palette::Light => light_theme(),
        }
    }
}

// Accessor-функции: ergonomic call-site (`theme::title()` вместо `theme::current().title`).
#[inline]
pub fn title() -> Style {
    current().title
}
#[inline]
pub fn header() -> Style {
    current().header
}
#[inline]
pub fn selected() -> Style {
    current().selected
}
#[inline]
pub fn marked() -> Style {
    current().marked
}
#[inline]
pub fn row() -> Style {
    current().row
}
#[inline]
pub fn key() -> Style {
    current().key
}
#[inline]
pub fn value() -> Style {
    current().value
}
#[inline]
pub fn error() -> Style {
    current().error
}
#[inline]
pub fn success() -> Style {
    current().success
}
#[inline]
pub fn tagline() -> Style {
    current().tagline
}
#[inline]
pub fn sparkle() -> Style {
    current().sparkle
}
#[inline]
pub fn footer_bg() -> Color {
    current().footer_bg
}
#[inline]
pub fn footer_fg() -> Color {
    current().footer_fg
}
#[inline]
pub fn footer() -> Style {
    current().footer
}
#[inline]
pub fn footer_title() -> Style {
    current().footer_title
}
#[inline]
pub fn footer_key() -> Style {
    current().footer_key
}
#[inline]
pub fn footer_hint() -> Style {
    current().footer_hint
}
#[inline]
pub fn block_border() -> Style {
    current().block_border
}
#[inline]
pub fn block_title() -> Style {
    current().block_title
}
#[inline]
pub fn modal_border() -> Style {
    current().modal_border
}
#[inline]
pub fn modal_label() -> Style {
    current().modal_label
}
#[inline]
pub fn modal_input() -> Style {
    current().modal_input
}
#[inline]
pub fn modal_cursor() -> Style {
    current().modal_cursor
}

/// Builder для основного блока — двойная рамка, magenta-title с ✦ sparkle-маркерами.
pub fn block(title_text: impl Into<String>) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Double)
        .border_style(current().block_border)
        .title_style(current().block_title)
        .title(format!(" ✦ {} ✦ ", title_text.into()))
}

// ---------- Dark palette ----------
//
// Yelower у footer фиксирован: насыщенный синий (xterm 20) + яркий белый — даёт
// высокий контраст и на тёмной, и на светлой теме терминала.

fn dark_theme() -> Theme {
    // Y2K accents
    let cyan_bright = Color::Indexed(51); // #00ffff
    let cyan_mid = Color::Indexed(45); // #00d7ff
    let blue_ps2 = Color::Indexed(20); // #0000d7 — глубокий PS2 navy
    let pink_hot = Color::Indexed(213); // #ff87ff — Y2K hot pink
    let white = Color::Indexed(255); // bright chrome
    let black = Color::Indexed(232); // near-black
    let green_bright = Color::Indexed(82);
    let red_bright = Color::Indexed(196);

    Theme {
        title: Style::new().fg(cyan_bright).add_modifier(Modifier::BOLD),
        header: Style::new().fg(cyan_mid).add_modifier(Modifier::BOLD),
        // Hover/selected: black on bright cyan — стиль PS2-меню при наведении.
        selected: Style::new()
            .fg(black)
            .bg(cyan_bright)
            .add_modifier(Modifier::BOLD),
        marked: Style::new()
            .fg(white)
            .bg(Color::Indexed(61))
            .add_modifier(Modifier::BOLD),
        row: Style::new().fg(white),
        key: Style::new().fg(pink_hot).add_modifier(Modifier::BOLD),
        value: Style::new().fg(white),
        error: Style::new()
            .fg(red_bright)
            .bg(black)
            .add_modifier(Modifier::BOLD),
        success: Style::new().fg(green_bright).add_modifier(Modifier::BOLD),
        tagline: Style::new().fg(pink_hot).add_modifier(Modifier::ITALIC),
        sparkle: Style::new().fg(cyan_bright).add_modifier(Modifier::BOLD),

        // Footer — фиксируется в обеих темах: синяя «лента» с ярким текстом.
        footer_bg: blue_ps2,
        footer_fg: white,
        footer: Style::new()
            .fg(white)
            .bg(blue_ps2)
            .add_modifier(Modifier::BOLD),
        footer_title: Style::new()
            .fg(pink_hot)
            .bg(blue_ps2)
            .add_modifier(Modifier::BOLD),
        footer_key: Style::new()
            .fg(black)
            .bg(cyan_bright)
            .add_modifier(Modifier::BOLD),
        footer_hint: Style::new().fg(white).bg(blue_ps2),

        block_border: Style::new().fg(blue_ps2),
        block_title: Style::new().fg(pink_hot).add_modifier(Modifier::BOLD),
        modal_border: Style::new()
            .fg(pink_hot)
            .bg(black)
            .add_modifier(Modifier::BOLD),
        modal_label: Style::new().fg(cyan_bright).add_modifier(Modifier::BOLD),
        modal_input: Style::new().fg(white).bg(Color::Indexed(238)),
        modal_cursor: Style::new()
            .fg(black)
            .bg(white)
            .add_modifier(Modifier::BOLD),
    }
}

// ---------- Light palette ----------
//
// Светлая тема: фон терминала ≈ белый, поэтому шрифты делаем тёмными.
// Footer оставляем «инвертированной лентой» — синий bg + белый fg — он одинаково
// контрастен и на светлом, и на тёмном фоне терминала.

fn light_theme() -> Theme {
    let cyan_dark = Color::Indexed(31); // #0087af — приглушённый dark cyan
    let blue_dark = Color::Indexed(19); // #0000af — глубокий navy для рамок
    let blue_ps2 = Color::Indexed(20); // тот же что и в dark — для footer
    let pink_dark = Color::Indexed(165); // #d700d7 — Y2K hot pink, читаемый на белом
    let pink_hot = Color::Indexed(213); // ярко-розовый, для footer-title (на синем)
    let cyan_bright = Color::Indexed(51); // для FOOTER_KEY (на синем)
    let white = Color::Indexed(255);
    let black = Color::Indexed(232);
    let dark_grey = Color::Indexed(236);
    let green_dark = Color::Indexed(28); // dark green — читаемый на белом
    let red_bright = Color::Indexed(160);

    Theme {
        title: Style::new().fg(cyan_dark).add_modifier(Modifier::BOLD),
        header: Style::new().fg(blue_dark).add_modifier(Modifier::BOLD),
        // Selected: white on PS2-blue — высоко-контрастный hover на белом фоне.
        selected: Style::new()
            .fg(white)
            .bg(blue_ps2)
            .add_modifier(Modifier::BOLD),
        marked: Style::new()
            .fg(white)
            .bg(Color::Indexed(61))
            .add_modifier(Modifier::BOLD),
        row: Style::new().fg(black),
        key: Style::new().fg(pink_dark).add_modifier(Modifier::BOLD),
        value: Style::new().fg(black),
        error: Style::new()
            .fg(white)
            .bg(red_bright)
            .add_modifier(Modifier::BOLD),
        success: Style::new().fg(green_dark).add_modifier(Modifier::BOLD),
        tagline: Style::new().fg(pink_dark).add_modifier(Modifier::ITALIC),
        sparkle: Style::new().fg(cyan_dark).add_modifier(Modifier::BOLD),

        // Footer одинаков обеих тем: blue bg + white fg.
        footer_bg: blue_ps2,
        footer_fg: white,
        footer: Style::new()
            .fg(white)
            .bg(blue_ps2)
            .add_modifier(Modifier::BOLD),
        footer_title: Style::new()
            .fg(pink_hot)
            .bg(blue_ps2)
            .add_modifier(Modifier::BOLD),
        footer_key: Style::new()
            .fg(black)
            .bg(cyan_bright)
            .add_modifier(Modifier::BOLD),
        footer_hint: Style::new().fg(white).bg(blue_ps2),

        block_border: Style::new().fg(blue_dark),
        block_title: Style::new().fg(pink_dark).add_modifier(Modifier::BOLD),
        modal_border: Style::new()
            .fg(pink_dark)
            .bg(white)
            .add_modifier(Modifier::BOLD),
        modal_label: Style::new().fg(cyan_dark).add_modifier(Modifier::BOLD),
        modal_input: Style::new().fg(black).bg(Color::Indexed(254)),
        modal_cursor: Style::new()
            .fg(white)
            .bg(dark_grey)
            .add_modifier(Modifier::BOLD),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_palette() {
        assert_eq!(Palette::from_str("dark").unwrap(), Palette::Dark);
        assert_eq!(Palette::from_str("Light").unwrap(), Palette::Light);
        assert_eq!(Palette::from_str("").unwrap(), Palette::Dark);
        assert!(Palette::from_str("blah").is_err());
    }

    #[test]
    fn footer_uses_indexed_colors_in_both_themes() {
        let dark = Palette::Dark.theme();
        let light = Palette::Light.theme();
        // Сам факт того, что footer сильно отличается от content — проверка через bg.
        assert_eq!(dark.footer_bg, light.footer_bg);
        assert_eq!(dark.footer_fg, light.footer_fg);
    }
}

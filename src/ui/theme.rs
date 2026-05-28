//! Темы UI в духе [eilmeldung](https://github.com/christo-auer/eilmeldung):
//! приглушённые rounded-рамки, акценты, выделение через `REVERSED`.
//!
//! Палитры: `midnight` (тёмный), `cream` (тёплый светлый), `mono` (монохром на белом
//! фоне), `latte` ([Catppuccin Latte](https://catppuccin.com/palette/)). Переключение в
//! рантайме — клавиша `T` (см. [`cycle_palette`]).
//!
//! Цвета — [`Color::Indexed`] (xterm-256), чтобы терминал не перекрашивал named ANSI.

use std::str::FromStr;
use std::sync::RwLock;

use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, BorderType, Borders};

// ---------- Palette / Theme ----------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Palette {
    /// Тёмный: magenta/blue, как дефолт eilmeldung.
    #[default]
    Midnight,
    /// Светлый: тёплые amber/brown акценты.
    Cream,
    /// Светлый: монохром, высокий контраст на белом фоне без ярких цветов.
    Mono,
    /// Светлый: [Catppuccin Latte](https://catppuccin.com/palette/) — приглушённые pastel.
    Latte,
}

impl Palette {
    pub const ALL: [Palette; 4] = [
        Palette::Midnight,
        Palette::Cream,
        Palette::Mono,
        Palette::Latte,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Palette::Midnight => "midnight",
            Palette::Cream => "cream",
            Palette::Mono => "mono",
            Palette::Latte => "latte",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Palette::Midnight => "Midnight",
            Palette::Cream => "Cream",
            Palette::Mono => "Mono",
            Palette::Latte => "Latte",
        }
    }

    pub fn is_dark(self) -> bool {
        matches!(self, Palette::Midnight)
    }

    pub fn next(self) -> Palette {
        let i = Self::ALL.iter().position(|p| *p == self).unwrap_or(0);
        Self::ALL[(i + 1) % Self::ALL.len()]
    }

    pub fn theme(self) -> Theme {
        match self {
            Palette::Midnight => midnight_theme(),
            Palette::Cream => cream_theme(),
            Palette::Mono => mono_theme(),
            Palette::Latte => latte_theme(),
        }
    }
}

impl FromStr for Palette {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "dark" | "midnight" | "slate" => Ok(Palette::Midnight),
            "cream" => Ok(Palette::Cream),
            "light" | "paper" | "mono" => Ok(Palette::Mono),
            "latte" => Ok(Palette::Latte),
            other => Err(format!(
                "unknown theme '{other}' (use midnight|cream|mono|latte; dark|light aliases ok)"
            )),
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
    pub block_border_focused: Style,
    pub block_title: Style,
    pub modal_border: Style,
    pub modal_label: Style,
    pub modal_input: Style,
    pub modal_cursor: Style,
}

// ---------- Active palette / accessors ----------

static ACTIVE: RwLock<Palette> = RwLock::new(Palette::Midnight);

/// Инициализировать палитру при старте (CLI / config).
pub fn init(palette: Palette) {
    if let Ok(mut guard) = ACTIVE.write() {
        *guard = palette;
    }
}

pub fn active_palette() -> Palette {
    ACTIVE.read().map(|g| *g).unwrap_or_default()
}

/// Следующая палитра по кругу; возвращает новую активную.
pub fn cycle_palette() -> Palette {
    let next = active_palette().next();
    init(next);
    next
}

#[inline]
pub fn current() -> Theme {
    active_palette().theme()
}

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
pub fn block_border_focused() -> Style {
    current().block_border_focused
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

/// Основной блок: rounded-рамка, заголовок без декоративных символов.
pub fn block(title_text: impl Into<String>) -> Block<'static> {
    block_with_focus(title_text, true)
}

pub fn block_with_focus(title_text: impl Into<String>, focused: bool) -> Block<'static> {
    let t = current();
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(if focused {
            t.block_border_focused
        } else {
            t.block_border
        })
        .title_style(t.block_title)
        .title(format!(" {} ", title_text.into()))
}

// ---------- Palette builders ----------

struct Colors {
    fg: Color,
    muted: Color,
    accent: Color,
    accent2: Color,
    accent3: Color,
    error: Color,
    success: Color,
    input_bg: Color,
    marked_bg: Color,
}

/// Footer bar and key pills — одинаковые внутри dark/light группы, не зависят от accent темы.
#[derive(Clone, Copy)]
struct FooterColors {
    bar_bg: Color,
    bar_fg: Color,
    title_fg: Color,
    key_fg: Color,
    key_bg: Color,
}

const FOOTER_DARK: FooterColors = FooterColors {
    bar_bg: Color::Indexed(236),
    bar_fg: Color::Indexed(252),
    title_fg: Color::Indexed(255),
    key_fg: Color::Indexed(236),
    key_bg: Color::Indexed(255),
};

const FOOTER_LIGHT: FooterColors = FooterColors {
    bar_bg: Color::Indexed(254),
    bar_fg: Color::Indexed(238),
    title_fg: Color::Indexed(234),
    key_fg: Color::Indexed(255),
    key_bg: Color::Indexed(236),
};

fn footer_styles(f: FooterColors) -> (Style, Style, Style, Style) {
    let bar = Style::new().fg(f.bar_fg).bg(f.bar_bg);
    let title = Style::new()
        .fg(f.title_fg)
        .bg(f.bar_bg)
        .add_modifier(Modifier::BOLD);
    // Без явного bg — наследует полосу footer; в модалках остаётся просто приглушённый текст.
    let hint = Style::new().fg(f.bar_fg);
    let key = Style::new()
        .fg(f.key_fg)
        .bg(f.key_bg)
        .add_modifier(Modifier::BOLD);
    (bar, title, key, hint)
}

fn from_colors(c: Colors, footer: FooterColors) -> Theme {
    let (footer_bar, footer_title, footer_key, footer_hint) = footer_styles(footer);

    Theme {
        title: Style::new().fg(c.accent).add_modifier(Modifier::BOLD),
        header: Style::new().fg(c.accent2).add_modifier(Modifier::BOLD),
        selected: Style::new().add_modifier(Modifier::REVERSED),
        marked: Style::new()
            .fg(c.fg)
            .bg(c.marked_bg)
            .add_modifier(Modifier::BOLD),
        row: Style::new().fg(c.fg),
        key: Style::new().fg(c.accent3).add_modifier(Modifier::BOLD),
        value: Style::new().fg(c.fg),
        error: Style::new().fg(c.error).add_modifier(Modifier::BOLD),
        success: Style::new().fg(c.success).add_modifier(Modifier::BOLD),
        tagline: Style::new().fg(c.muted).add_modifier(Modifier::ITALIC),
        sparkle: Style::new().fg(c.accent3),

        footer_bg: footer.bar_bg,
        footer_fg: footer.bar_fg,
        footer: footer_bar,
        footer_title,
        footer_key,
        footer_hint,

        block_border: Style::new().fg(c.muted),
        block_border_focused: Style::new().fg(c.accent).add_modifier(Modifier::BOLD),
        block_title: Style::new().fg(c.accent).add_modifier(Modifier::BOLD),
        modal_border: Style::new().fg(c.accent).add_modifier(Modifier::BOLD),
        modal_label: Style::new().fg(c.accent2).add_modifier(Modifier::BOLD),
        modal_input: Style::new().fg(c.fg).bg(c.input_bg),
        modal_cursor: Style::new()
            .fg(c.input_bg)
            .bg(c.fg)
            .add_modifier(Modifier::BOLD),
    }
}

fn midnight_theme() -> Theme {
    from_colors(
        Colors {
            fg: Color::Indexed(252),
            muted: Color::Indexed(240),
            accent: Color::Indexed(213),
            accent2: Color::Indexed(33),
            accent3: Color::Indexed(45),
            error: Color::Indexed(203),
            success: Color::Indexed(82),
            input_bg: Color::Indexed(236),
            marked_bg: Color::Indexed(61),
        },
        FOOTER_DARK,
    )
}

fn cream_theme() -> Theme {
    from_colors(
        Colors {
            fg: Color::Indexed(237),
            muted: Color::Indexed(244),
            accent: Color::Indexed(130),
            accent2: Color::Indexed(94),
            accent3: Color::Indexed(172),
            error: Color::Indexed(167),
            success: Color::Indexed(64),
            input_bg: Color::Indexed(255),
            marked_bg: Color::Indexed(222),
        },
        FOOTER_LIGHT,
    )
}

/// Монохромная палитра для белого/светлого фона терминала.
fn mono_theme() -> Theme {
    from_colors(
        Colors {
            fg: Color::Indexed(234),
            muted: Color::Indexed(245),
            accent: Color::Indexed(238),
            accent2: Color::Indexed(240),
            accent3: Color::Indexed(242),
            error: Color::Indexed(237),
            success: Color::Indexed(243),
            input_bg: Color::Indexed(255),
            marked_bg: Color::Indexed(252),
        },
        FOOTER_LIGHT,
    )
}

/// Catppuccin Latte — https://catppuccin.com/palette/
fn latte_theme() -> Theme {
    from_colors(
        Colors {
            fg: Color::Indexed(239),        // text #4c4f69
            muted: Color::Indexed(244),     // overlay0 #9ca0b0
            accent: Color::Indexed(135),    // mauve #8839ef
            accent2: Color::Indexed(31),    // sapphire #209fb5
            accent3: Color::Indexed(30),    // teal #179299
            error: Color::Indexed(160),     // red #d20f39
            success: Color::Indexed(28),    // green #40a02b
            input_bg: Color::Indexed(255),  // base #eff1f5
            marked_bg: Color::Indexed(251), // surface0 #ccd0da
        },
        FOOTER_LIGHT,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_palette() {
        assert_eq!(Palette::from_str("midnight").unwrap(), Palette::Midnight);
        assert_eq!(Palette::from_str("dark").unwrap(), Palette::Midnight);
        assert_eq!(Palette::from_str("slate").unwrap(), Palette::Midnight);
        assert_eq!(Palette::from_str("cream").unwrap(), Palette::Cream);
        assert_eq!(Palette::from_str("mono").unwrap(), Palette::Mono);
        assert_eq!(Palette::from_str("paper").unwrap(), Palette::Mono);
        assert_eq!(Palette::from_str("light").unwrap(), Palette::Mono);
        assert_eq!(Palette::from_str("latte").unwrap(), Palette::Latte);
        assert!(Palette::from_str("blah").is_err());
    }

    #[test]
    fn cycle_wraps() {
        init(Palette::Latte);
        assert_eq!(cycle_palette(), Palette::Midnight);
        init(Palette::Midnight);
        assert_eq!(cycle_palette(), Palette::Cream);
    }
}

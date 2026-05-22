use ratatui::style::{Color, Modifier, Style};

pub const TITLE: Style = Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD);
pub const STATUS: Style = Style::new().fg(Color::DarkGray);
pub const SELECTED: Style = Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD);
pub const ERROR: Style = Style::new().fg(Color::Red).add_modifier(Modifier::BOLD);
pub const HEADER: Style = Style::new().fg(Color::Blue).add_modifier(Modifier::BOLD);
pub const KEY: Style = Style::new().fg(Color::Magenta);
pub const VALUE: Style = Style::new().fg(Color::White);

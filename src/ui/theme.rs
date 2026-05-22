use ratatui::style::{Color, Modifier, Style};

// Основной контент
pub const TITLE: Style = Style::new()
    .fg(Color::LightCyan)
    .add_modifier(Modifier::BOLD);
pub const HEADER: Style = Style::new()
    .fg(Color::LightBlue)
    .add_modifier(Modifier::BOLD);
pub const SELECTED: Style = Style::new()
    .fg(Color::Black)
    .bg(Color::Yellow)
    .add_modifier(Modifier::BOLD);
pub const ROW: Style = Style::new().fg(Color::White);
pub const KEY: Style = Style::new()
    .fg(Color::LightMagenta)
    .add_modifier(Modifier::BOLD);
pub const VALUE: Style = Style::new().fg(Color::White);
pub const ERROR: Style = Style::new()
    .fg(Color::Red)
    .bg(Color::Black)
    .add_modifier(Modifier::BOLD);
pub const SUCCESS: Style = Style::new()
    .fg(Color::LightGreen)
    .add_modifier(Modifier::BOLD);

// Нижняя панель — высокий контраст на серых темах терминала
pub const FOOTER_BG: Color = Color::Blue;
pub const FOOTER_FG: Color = Color::White;
pub const FOOTER: Style = Style::new()
    .fg(FOOTER_FG)
    .bg(FOOTER_BG)
    .add_modifier(Modifier::BOLD);
pub const FOOTER_TITLE: Style = Style::new()
    .fg(Color::Yellow)
    .bg(FOOTER_BG)
    .add_modifier(Modifier::BOLD);
pub const FOOTER_KEY: Style = Style::new()
    .fg(Color::Black)
    .bg(Color::Cyan)
    .add_modifier(Modifier::BOLD);
pub const FOOTER_HINT: Style = Style::new().fg(Color::White).bg(FOOTER_BG);

// Блоки и модалки
pub const BLOCK_BORDER: Style = Style::new().fg(Color::LightCyan);
pub const BLOCK_TITLE: Style = Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD);
pub const MODAL_BORDER: Style = Style::new()
    .fg(Color::Yellow)
    .bg(Color::Black)
    .add_modifier(Modifier::BOLD);
pub const MODAL_LABEL: Style = Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD);
pub const MODAL_INPUT: Style = Style::new().fg(Color::White).bg(Color::DarkGray);
pub const MODAL_CURSOR: Style = Style::new()
    .fg(Color::Black)
    .bg(Color::White)
    .add_modifier(Modifier::BOLD);

pub fn block(title: impl Into<String>) -> ratatui::widgets::Block<'static> {
    use ratatui::widgets::{Block, Borders};
    Block::default()
        .borders(Borders::ALL)
        .border_style(BLOCK_BORDER)
        .title_style(BLOCK_TITLE)
        .title(title.into())
}

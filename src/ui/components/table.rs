use ratatui::layout::Rect;
use ratatui::widgets::{Cell, Row, Table, TableState};
use ratatui::Frame;

use crate::ui::theme;

pub struct TableView {
    pub title: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub state: TableState,
    pub filter: String,
}

impl TableView {
    pub fn new(title: impl Into<String>, headers: Vec<String>) -> Self {
        Self {
            title: title.into(),
            headers,
            rows: Vec::new(),
            state: TableState::default().with_selected(Some(0)),
            filter: String::new(),
        }
    }

    pub fn set_rows(&mut self, rows: Vec<Vec<String>>) {
        self.rows = rows;
        if self.selected().is_none() && !self.filtered_rows().is_empty() {
            self.state.select(Some(0));
        }
    }

    pub fn filtered_rows(&self) -> Vec<&Vec<String>> {
        if self.filter.is_empty() {
            return self.rows.iter().collect();
        }
        let f = self.filter.to_lowercase();
        self.rows
            .iter()
            .filter(|row| row.iter().any(|c| c.to_lowercase().contains(&f)))
            .collect()
    }

    pub fn selected_index(&self) -> Option<usize> {
        let sel = self.state.selected()?;
        let rows = self.filtered_rows();
        if sel < rows.len() {
            Some(sel)
        } else {
            None
        }
    }

    pub fn selected_row(&self) -> Option<&Vec<String>> {
        let sel = self.state.selected()?;
        self.filtered_rows().get(sel).copied()
    }

    pub fn selected(&self) -> Option<usize> {
        self.state.selected()
    }

    pub fn next(&mut self) {
        let len = self.filtered_rows().len();
        if len == 0 {
            return;
        }
        let i = match self.state.selected() {
            Some(i) => (i + 1).min(len - 1),
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn prev(&mut self) {
        let len = self.filtered_rows().len();
        if len == 0 {
            return;
        }
        let i = match self.state.selected() {
            Some(i) => i.saturating_sub(1),
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect) {
        let filtered: Vec<Vec<String>> = self.filtered_rows().into_iter().cloned().collect();
        let rows: Vec<Row> = filtered
            .iter()
            .map(|r| Row::new(r.iter().map(|c| Cell::from(c.as_str())).collect::<Vec<_>>()))
            .collect();

        let widths: Vec<_> = self
            .headers
            .iter()
            .map(|_| ratatui::layout::Constraint::Fill(1))
            .collect();

        let title = if self.filter.is_empty() {
            self.title.clone()
        } else {
            format!("{}  filter: {}", self.title, self.filter)
        };

        let table = Table::default()
            .header(
                Row::new(
                    self.headers
                        .iter()
                        .map(|h| Cell::from(h.as_str()).style(theme::header())),
                )
                .style(theme::header())
                .bottom_margin(1),
            )
            .block(theme::block(title))
            .rows(rows)
            .widths(widths)
            .row_highlight_style(theme::selected())
            .highlight_symbol("▸ ");

        frame.render_stateful_widget(table, area, &mut self.state);
    }
}

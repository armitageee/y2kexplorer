use std::collections::HashSet;

use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::{Cell, Row, Table, TableState};
use ratatui::Frame;

use crate::ui::theme;

pub struct TableView {
    pub title: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub state: TableState,
    pub filter: String,
    /// Индексы в `filtered_rows()` — k9s-style multi-select (Space).
    pub marked: HashSet<usize>,
    pub multi_select: bool,
}

impl TableView {
    pub fn new(title: impl Into<String>, headers: Vec<String>) -> Self {
        Self {
            title: title.into(),
            headers,
            rows: Vec::new(),
            state: TableState::default().with_selected(Some(0)),
            filter: String::new(),
            marked: HashSet::new(),
            multi_select: false,
        }
    }

    pub fn enable_multi_select(mut self) -> Self {
        self.multi_select = true;
        self
    }

    pub fn clear_marks(&mut self) {
        self.marked.clear();
    }

    pub fn marked_count(&self) -> usize {
        self.marked.len()
    }

    /// Toggle mark на текущей строке (filtered index).
    pub fn toggle_mark(&mut self) {
        if !self.multi_select {
            return;
        }
        let Some(sel) = self.state.selected() else {
            return;
        };
        if self.filtered_rows().get(sel).is_none() {
            return;
        }
        if self.marked.contains(&sel) {
            self.marked.remove(&sel);
        } else {
            self.marked.insert(sel);
        }
    }

    /// Имена/первые колонки отмеченных строк; если ничего не отмечено — текущая строка.
    pub fn marked_or_current_first_col(&self) -> Vec<String> {
        let filtered = self.filtered_rows();
        if !self.marked.is_empty() {
            let mut indices: Vec<_> = self.marked.iter().copied().collect();
            indices.sort_unstable();
            return indices
                .into_iter()
                .filter_map(|i| filtered.get(i))
                .filter_map(|row| row.first().cloned())
                .collect();
        }
        self.selected_row()
            .and_then(|r| r.first().cloned())
            .into_iter()
            .collect()
    }

    pub fn set_rows(&mut self, rows: Vec<Vec<String>>) {
        self.rows = rows;
        self.marked.clear();
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
        let sel = self.state.selected();
        let rows: Vec<Row> = filtered
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let is_marked = self.marked.contains(&i);
                let is_cursor = sel == Some(i);
                let sym = if is_marked {
                    "✓ "
                } else if is_cursor {
                    "▸ "
                } else {
                    "  "
                };
                let style = if is_marked && !is_cursor {
                    theme::marked()
                } else {
                    Style::default()
                };
                let cells: Vec<Cell> = r
                    .iter()
                    .enumerate()
                    .map(|(col, c)| {
                        let text = if col == 0 {
                            format!("{sym}{c}")
                        } else {
                            c.clone()
                        };
                        Cell::from(text).style(style)
                    })
                    .collect();
                Row::new(cells)
            })
            .collect();

        let widths: Vec<_> = self
            .headers
            .iter()
            .map(|_| ratatui::layout::Constraint::Fill(1))
            .collect();

        let mut title = self.title.clone();
        if !self.filter.is_empty() {
            title = format!("{title}  filter: {}", self.filter);
        }
        if self.multi_select && !self.marked.is_empty() {
            title = format!("{title}  [{} marked]", self.marked.len());
        }

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
            .highlight_symbol("");

        frame.render_stateful_widget(table, area, &mut self.state);
    }
}

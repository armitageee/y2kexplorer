use ratatui::layout::Rect;
use ratatui::Frame;

use crate::kafka::AclEntry;
use crate::ui::{draw_help, draw_status, TableView};

const HELP: &[&str] = &[
    "j/k", "nav", ":", "command", "/", "filter", "c", "create", "e", "edit", "d", "delete", "r",
    "refresh", "Esc", "back", "?", "help", "q", "quit",
];

const HINT: &[&str] = &[
    "c", "create", "e", "edit", "d", "delete", "/", "filter", "r", "refresh", "?", "help",
];

pub struct AclsView {
    pub table: TableView,
    pub show_help: bool,
    pub acls: Vec<AclEntry>,
}

impl AclsView {
    pub fn new() -> Self {
        Self {
            table: TableView::new(
                "ACLs",
                vec![
                    "TYPE".into(),
                    "RESOURCE".into(),
                    "PATTERN".into(),
                    "PRINCIPAL".into(),
                    "HOST".into(),
                    "OP".into(),
                    "PERM".into(),
                ],
            ),
            show_help: false,
            acls: Vec::new(),
        }
    }

    pub fn load(&mut self, acls: Vec<AclEntry>) {
        self.acls = acls.clone();
        let rows = acls
            .into_iter()
            .map(|a| {
                vec![
                    a.resource_type,
                    a.resource_name,
                    a.pattern_type,
                    a.principal,
                    a.host,
                    a.operation,
                    a.permission,
                ]
            })
            .collect();
        self.table.set_rows(rows);
    }

    pub fn selected_acl(&self) -> Option<&AclEntry> {
        let row = self.table.selected_row()?;
        let key = (
            row.first()?.as_str(),
            row.get(1).map(String::as_str).unwrap_or(""),
            row.get(2).map(String::as_str).unwrap_or(""),
            row.get(3).map(String::as_str).unwrap_or(""),
            row.get(5).map(String::as_str).unwrap_or(""),
            row.get(6).map(String::as_str).unwrap_or(""),
        );
        self.acls.iter().find(|a| {
            (
                a.resource_type.as_str(),
                a.resource_name.as_str(),
                a.pattern_type.as_str(),
                a.principal.as_str(),
                a.operation.as_str(),
                a.permission.as_str(),
            ) == key
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn render(
        &mut self,
        frame: &mut Frame,
        main: Rect,
        status_area: Rect,
        keys_area: Rect,
        cluster: &str,
        status: &str,
        loading: bool,
    ) {
        self.table.render(frame, main);
        draw_status(frame, status_area, cluster, status, loading);
        if self.show_help {
            draw_help(frame, keys_area, HELP);
        } else {
            draw_help(frame, keys_area, HINT);
        }
    }
}

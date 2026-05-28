use super::{
    AclsView, ConnectorDetailView, ConnectorsView, ContextListView, GroupDetailsView, GroupsView,
    LabelListView, MessagesView, SchemaDetailView, SchemasView, TopicsView,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Topics,
    Messages,
    Groups,
    GroupDetails,
    Labels,
    Contexts,
    Acls,
    Schemas,
    SchemaDetail,
    Connectors,
    ConnectorDetail,
}

pub enum ViewStack {
    Topics(TopicsView),
    Messages(MessagesView),
    Groups(GroupsView),
    GroupDetails(GroupDetailsView),
    Labels(LabelListView),
    Contexts(ContextListView),
    Acls(AclsView),
    Schemas(SchemasView),
    SchemaDetail(SchemaDetailView),
    Connectors(ConnectorsView),
    ConnectorDetail(ConnectorDetailView),
}

impl ViewStack {
    pub fn screen(&self) -> Screen {
        match self {
            ViewStack::Topics(_) => Screen::Topics,
            ViewStack::Messages(_) => Screen::Messages,
            ViewStack::Groups(_) => Screen::Groups,
            ViewStack::GroupDetails(_) => Screen::GroupDetails,
            ViewStack::Labels(_) => Screen::Labels,
            ViewStack::Contexts(_) => Screen::Contexts,
            ViewStack::Acls(_) => Screen::Acls,
            ViewStack::Schemas(_) => Screen::Schemas,
            ViewStack::SchemaDetail(_) => Screen::SchemaDetail,
            ViewStack::Connectors(_) => Screen::Connectors,
            ViewStack::ConnectorDetail(_) => Screen::ConnectorDetail,
        }
    }

    /// Корневой экран для sidebar (не drill-down).
    pub fn root_screen(&self) -> Screen {
        match self {
            ViewStack::Messages(_) => Screen::Topics,
            ViewStack::GroupDetails(_) => Screen::Groups,
            ViewStack::SchemaDetail(_) => Screen::Schemas,
            ViewStack::ConnectorDetail(_) => Screen::Connectors,
            other => other.screen(),
        }
    }

    pub fn is_root_nav(&self) -> bool {
        matches!(
            self,
            ViewStack::Topics(_)
                | ViewStack::Groups(_)
                | ViewStack::Labels(_)
                | ViewStack::Contexts(_)
                | ViewStack::Acls(_)
                | ViewStack::Schemas(_)
                | ViewStack::Connectors(_)
        )
    }

    pub fn title(&self) -> &str {
        match self {
            ViewStack::Topics(v) => &v.table.title,
            ViewStack::Messages(v) => &v.title,
            ViewStack::Groups(v) => &v.table.title,
            ViewStack::GroupDetails(v) => &v.table.title,
            ViewStack::Labels(v) => &v.table.title,
            ViewStack::Contexts(v) => &v.table.title,
            ViewStack::Acls(v) => &v.table.title,
            ViewStack::Schemas(v) => &v.table.title,
            ViewStack::SchemaDetail(v) => &v.title,
            ViewStack::Connectors(v) => &v.table.title,
            ViewStack::ConnectorDetail(v) => &v.title,
        }
    }

    /// Пары [`draw_help`]: короткий HINT или полный HELP (по `?`).
    pub fn help_pairs(&self) -> &'static [&'static str] {
        match self {
            ViewStack::Topics(v) => v.help_pairs(),
            ViewStack::Messages(v) => v.help_pairs(),
            ViewStack::Groups(v) => v.help_pairs(),
            ViewStack::GroupDetails(v) => v.help_pairs(),
            ViewStack::Labels(v) => v.help_pairs(),
            ViewStack::Contexts(v) => v.help_pairs(),
            ViewStack::Acls(v) => v.help_pairs(),
            ViewStack::Schemas(v) => v.help_pairs(),
            ViewStack::SchemaDetail(v) => v.help_pairs(),
            ViewStack::Connectors(v) => v.help_pairs(),
            ViewStack::ConnectorDetail(v) => v.help_pairs(),
        }
    }

    pub fn help_expanded(&self) -> bool {
        match self {
            ViewStack::Topics(v) => v.show_help,
            ViewStack::Messages(v) => v.show_help,
            ViewStack::Groups(v) => v.show_help,
            ViewStack::GroupDetails(v) => v.show_help,
            ViewStack::Labels(v) => v.show_help,
            ViewStack::Contexts(v) => v.show_help,
            ViewStack::Acls(v) => v.show_help,
            ViewStack::Schemas(v) => v.show_help,
            ViewStack::SchemaDetail(v) => v.show_help,
            ViewStack::Connectors(v) => v.show_help,
            ViewStack::ConnectorDetail(v) => v.show_help,
        }
    }
}

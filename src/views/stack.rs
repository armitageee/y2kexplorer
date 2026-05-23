use super::{GroupDetailsView, GroupsView, LabelListView, MessagesView, TopicsView};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Topics,
    Messages,
    Groups,
    GroupDetails,
    Labels,
}

pub enum ViewStack {
    Topics(TopicsView),
    Messages(MessagesView),
    Groups(GroupsView),
    GroupDetails(GroupDetailsView),
    Labels(LabelListView),
}

impl ViewStack {
    pub fn screen(&self) -> Screen {
        match self {
            ViewStack::Topics(_) => Screen::Topics,
            ViewStack::Messages(_) => Screen::Messages,
            ViewStack::Groups(_) => Screen::Groups,
            ViewStack::GroupDetails(_) => Screen::GroupDetails,
            ViewStack::Labels(_) => Screen::Labels,
        }
    }

    /// Корневой экран для sidebar (не drill-down).
    pub fn root_screen(&self) -> Screen {
        match self {
            ViewStack::Messages(_) => Screen::Topics,
            ViewStack::GroupDetails(_) => Screen::Groups,
            other => other.screen(),
        }
    }

    pub fn is_root_nav(&self) -> bool {
        matches!(
            self,
            ViewStack::Topics(_) | ViewStack::Groups(_) | ViewStack::Labels(_)
        )
    }

    pub fn title(&self) -> &str {
        match self {
            ViewStack::Topics(v) => &v.table.title,
            ViewStack::Messages(v) => &v.title,
            ViewStack::Groups(v) => &v.table.title,
            ViewStack::GroupDetails(v) => &v.table.title,
            ViewStack::Labels(v) => &v.table.title,
        }
    }
}

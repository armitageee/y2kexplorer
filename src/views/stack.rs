use super::{GroupDetailsView, GroupsView, MessagesView, TopicsView};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Topics,
    Messages,
    Groups,
    GroupDetails,
}

pub enum ViewStack {
    Topics(TopicsView),
    Messages(MessagesView),
    Groups(GroupsView),
    GroupDetails(GroupDetailsView),
}

impl ViewStack {
    pub fn screen(&self) -> Screen {
        match self {
            ViewStack::Topics(_) => Screen::Topics,
            ViewStack::Messages(_) => Screen::Messages,
            ViewStack::Groups(_) => Screen::Groups,
            ViewStack::GroupDetails(_) => Screen::GroupDetails,
        }
    }

    pub fn title(&self) -> &str {
        match self {
            ViewStack::Topics(v) => &v.table.title,
            ViewStack::Messages(v) => &v.title,
            ViewStack::Groups(v) => &v.table.title,
            ViewStack::GroupDetails(v) => &v.table.title,
        }
    }
}

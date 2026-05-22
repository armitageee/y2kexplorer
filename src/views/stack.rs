use super::{MessagesView, TopicsView};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Topics,
    Messages,
}

pub enum ViewStack {
    Topics(TopicsView),
    Messages(MessagesView),
}

impl ViewStack {
    pub fn screen(&self) -> Screen {
        match self {
            ViewStack::Topics(_) => Screen::Topics,
            ViewStack::Messages(_) => Screen::Messages,
        }
    }

    pub fn title(&self) -> &str {
        match self {
            ViewStack::Topics(v) => &v.table.title,
            ViewStack::Messages(v) => &v.title,
        }
    }
}

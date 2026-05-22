pub mod worker;

use std::io;
use std::path::PathBuf;
use std::sync::mpsc::Sender;

use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Layout};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::config::{AppConfig, ClusterConfig};
use crate::kafka::{ClusterConnection, PartitionInfo};
use crate::ui::theme;
use crate::views::{MessagesView, TopicsView, ViewStack};

use worker::{spawn_fetch_messages, spawn_list_topics, WorkerMsg};

pub struct App {
    config: AppConfig,
    cluster_name: String,
    cluster: ClusterConfig,
    pub connection: Option<ClusterConnection>,
    stack: Vec<ViewStack>,
    status: String,
    loading: bool,
    should_quit: bool,
    filter_mode: bool,
    filter_buf: String,
    show_partitions_popup: bool,
    partition_lines: Vec<String>,
    worker_tx: Sender<WorkerMsg>,
}

impl App {
    pub fn new(config: AppConfig, worker_tx: Sender<WorkerMsg>) -> Result<Self> {
        let (name, cluster) = config.active_cluster()?;
        let cluster_name = name.to_string();
        let cluster = cluster.clone();
        Ok(Self {
            config,
            cluster_name,
            cluster,
            connection: None,
            stack: vec![ViewStack::Topics(TopicsView::new())],
            status: "connecting…".into(),
            loading: true,
            should_quit: false,
            filter_mode: false,
            filter_buf: String::new(),
            show_partitions_popup: false,
            partition_lines: Vec::new(),
            worker_tx,
        })
    }

    pub fn init_connection(&mut self) {
        match ClusterConnection::connect(&self.cluster) {
            Ok(conn) => {
                self.connection = Some(conn);
                self.status = "connected".into();
                self.refresh_topics();
            }
            Err(e) => {
                self.status = format!("connect failed: {e:#}");
                self.loading = false;
            }
        }
    }

    pub fn handle_event(&mut self, ev: Event) -> io::Result<()> {
        if self.filter_mode {
            return self.handle_filter_event(ev);
        }

        if let Event::Key(key) = ev {
            if key.kind != KeyEventKind::Press {
                return Ok(());
            }
            self.handle_key(key)?;
        }
        Ok(())
    }

    fn handle_filter_event(&mut self, ev: Event) -> io::Result<()> {
        if let Event::Key(key) = ev {
            if key.kind != KeyEventKind::Press {
                return Ok(());
            }
            match key.code {
                KeyCode::Esc => {
                    self.filter_mode = false;
                    self.filter_buf.clear();
                }
                KeyCode::Enter => {
                    let filter = self.filter_buf.clone();
                    if let ViewStack::Topics(v) = self.current_mut() {
                        v.table.filter = filter;
                    }
                    self.filter_mode = false;
                }
                KeyCode::Backspace => {
                    self.filter_buf.pop();
                }
                KeyCode::Char(c) => self.filter_buf.push(c),
                _ => {}
            }
        }
        Ok(())
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> io::Result<()> {
        if self.show_partitions_popup {
            if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) {
                self.show_partitions_popup = false;
            }
            return Ok(());
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('?') => self.toggle_help(),
            KeyCode::Char('r') => self.refresh_current(),
            KeyCode::Char('/') => {
                if matches!(self.current(), ViewStack::Topics(_)) {
                    self.filter_mode = true;
                    if let ViewStack::Topics(v) = self.current_mut() {
                        self.filter_buf = v.table.filter.clone();
                    }
                }
            }
            KeyCode::Char('j') | KeyCode::Down => self.nav_down(),
            KeyCode::Char('k') | KeyCode::Up => self.nav_up(),
            KeyCode::Enter => self.enter(),
            KeyCode::Esc => self.pop(),
            KeyCode::Char('p') => self.show_partitions(),
            KeyCode::Char('b') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.from_end = true;
                    self.reload_messages();
                }
            }
            KeyCode::Char('t') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.from_end = false;
                    self.reload_messages();
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn toggle_help(&mut self) {
        match self.current_mut() {
            ViewStack::Topics(v) => v.show_help = !v.show_help,
            ViewStack::Messages(v) => v.show_help = !v.show_help,
        }
    }

    fn nav_down(&mut self) {
        match self.current_mut() {
            ViewStack::Topics(v) => v.table.next(),
            ViewStack::Messages(v) => v.next(),
        }
    }

    fn nav_up(&mut self) {
        match self.current_mut() {
            ViewStack::Topics(v) => v.table.prev(),
            ViewStack::Messages(v) => v.prev(),
        }
    }

    fn enter(&mut self) {
        if let ViewStack::Topics(v) = self.current() {
            if let Some(topic) = v.selected_topic() {
                let topic = topic.to_string();
                self.stack.push(ViewStack::Messages(MessagesView::new(&topic)));
                self.reload_messages();
            }
        }
    }

    fn pop(&mut self) {
        if self.stack.len() > 1 {
            self.stack.pop();
            self.status = "ready".into();
        }
    }

    fn refresh_current(&mut self) {
        match self.current() {
            ViewStack::Topics(_) => self.refresh_topics(),
            ViewStack::Messages(_) => self.reload_messages(),
        }
    }

    fn refresh_topics(&mut self) {
        let Some(conn) = self.connection.as_ref() else {
            self.init_connection();
            return;
        };
        self.loading = true;
        self.status = "loading topics…".into();
        if let Ok(conn) = conn.reconnect() {
            spawn_list_topics(conn, self.worker_tx.clone());
        }
    }

    fn reload_messages(&mut self) {
        let ViewStack::Messages(view) = self.current() else {
            return;
        };
        let topic = view.topic.clone();
        let from_end = view.from_end;
        let limit = self.config.defaults.message_limit;
        let Some(conn) = self.connection.as_ref() else {
            return;
        };
        self.loading = true;
        self.status = format!("loading messages for {topic}…");
        if let Ok(conn) = conn.reconnect() {
            spawn_fetch_messages(conn, topic, None, limit, from_end, self.worker_tx.clone());
        }
    }

    fn show_partitions(&mut self) {
        let topic = match self.current() {
            ViewStack::Topics(v) => v.selected_topic().map(str::to_string),
            ViewStack::Messages(v) => Some(v.topic.clone()),
        };
        let Some(topic) = topic else { return };
        let Some(conn) = self.connection.as_ref() else { return };

        self.loading = true;
        self.status = format!("loading partitions for {topic}…");
        match conn.topic_partitions(&topic) {
            Ok(parts) => {
                self.partition_lines = format_partitions(&parts);
                self.show_partitions_popup = true;
                self.status = format!("{topic}: {} partitions", parts.len());
                self.loading = false;
            }
            Err(e) => {
                self.status = format!("partitions error: {e:#}");
                self.loading = false;
            }
        }
    }

    pub fn on_worker_msg(&mut self, msg: WorkerMsg) {
        self.loading = false;
        match msg {
            WorkerMsg::Topics(Ok(topics)) => {
                if let ViewStack::Topics(v) = self.current_mut() {
                    v.load(topics);
                }
                self.status = "ready".into();
            }
            WorkerMsg::Topics(Err(e)) => self.status = format!("topics error: {e:#}"),
            WorkerMsg::Messages { topic, result } => match result {
                Ok(msgs) => {
                    if let ViewStack::Messages(v) = self.current_mut() {
                        if v.topic == topic {
                            v.load(msgs);
                            self.status = format!("{} messages", v.messages.len());
                        }
                    }
                }
                Err(e) => self.status = format!("messages error: {e:#}"),
            },
        }
    }

    pub fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        if self.filter_mode {
            let popup = Layout::vertical([Constraint::Length(3)]).split(area);
            let text = format!("filter: {}_", self.filter_buf);
            frame.render_widget(
                Paragraph::new(text)
                    .style(theme::TITLE)
                    .block(Block::default().borders(Borders::ALL).title("filter")),
                popup[0],
            );
            return;
        }

        if self.show_partitions_popup {
            let popup = Layout::vertical([Constraint::Min(5)]).split(area);
            let text = self.partition_lines.join("\n");
            frame.render_widget(
                Paragraph::new(text)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("partitions (Esc to close)"),
                    ),
                popup[0],
            );
            return;
        }

        let cluster = self.cluster_name.clone();
        let status = self.status.clone();
        let loading = self.loading;
        match self.current_mut() {
            ViewStack::Topics(v) => v.render(frame, area, &cluster, &status, loading),
            ViewStack::Messages(v) => v.render(frame, area, &cluster, &status, loading),
        }
    }

    fn current(&self) -> &ViewStack {
        self.stack.last().expect("stack never empty")
    }

    fn current_mut(&mut self) -> &mut ViewStack {
        self.stack.last_mut().expect("stack never empty")
    }

    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    pub fn config_path(&self) -> Result<PathBuf> {
        AppConfig::config_path()
    }
}

fn format_partitions(parts: &[PartitionInfo]) -> Vec<String> {
    let mut lines = vec![
        "PART  LEADER  REPLICAS           ISR".into(),
        "────  ──────  ─────────────────  ─────────────────".into(),
    ];
    for p in parts {
        lines.push(format!(
            "{:>4}  {:>6}  {:>17?}  {:>17?}",
            p.id, p.leader, p.replicas, p.isr
        ));
    }
    lines
}

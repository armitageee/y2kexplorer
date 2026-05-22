mod command;
pub mod modal;
pub mod worker;

use std::io;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::Frame;

use crate::config::{clamp_live_poll_secs, AppConfig, ClusterConfig};
use crate::kafka::{ClusterConnection, PartitionInfo};
use crate::views::{MessagesView, TopicsView, ViewStack};

use modal::{draw_modal, layout_main, Modal, ModalField};
use worker::{
    spawn_create_topic, spawn_delete_topic, spawn_fetch_messages, spawn_list_topics,
    spawn_poll_live_messages, spawn_produce, WorkerMsg,
};

pub struct App {
    config: AppConfig,
    cluster_name: String,
    cluster: ClusterConfig,
    pub connection: Option<ClusterConnection>,
    stack: Vec<ViewStack>,
    status: String,
    loading: bool,
    should_quit: bool,
    modal: Option<Modal>,
    filter_buf: String,
    command_buf: String,
    show_partitions_popup: bool,
    partition_lines: Vec<String>,
    worker_tx: Sender<WorkerMsg>,
    last_live_poll: Option<Instant>,
    live_fetch_in_flight: bool,
}

impl App {
    pub fn new(config: AppConfig, worker_tx: Sender<WorkerMsg>) -> Result<Self> {
        let (name, cluster) = config.active_cluster()?;
        let cluster_name = name.to_string();
        let cluster = cluster.clone();
        let cfg_path = AppConfig::config_path().ok();
        let clusters = config.cluster_names().join(", ");
        let status = cfg_path
            .map(|p| format!("config: {}  clusters: [{clusters}]", p.display()))
            .unwrap_or_else(|| format!("clusters: [{clusters}]"));
        Ok(Self {
            config,
            cluster_name,
            cluster,
            connection: None,
            stack: vec![ViewStack::Topics(TopicsView::new())],
            status,
            loading: true,
            should_quit: false,
            modal: None,
            filter_buf: String::new(),
            command_buf: String::new(),
            show_partitions_popup: false,
            partition_lines: Vec::new(),
            worker_tx,
            last_live_poll: None,
            live_fetch_in_flight: false,
        })
    }

    /// Периодический live-poll (вызывается из главного цикла, не блокирует UI).
    pub fn tick(&mut self) {
        if self.modal.is_some()
            || self.show_partitions_popup
            || self.live_fetch_in_flight
            || self.loading
        {
            return;
        }

        let ViewStack::Messages(v) = self.current() else {
            return;
        };
        if !v.live || v.next_offsets.is_empty() {
            return;
        }

        let interval =
            Duration::from_secs(clamp_live_poll_secs(self.config.defaults.live_poll_secs));
        let now = Instant::now();
        if let Some(last) = self.last_live_poll {
            if now.duration_since(last) < interval {
                return;
            }
        }

        let topic = v.topic.clone();
        let partition = v.partition;
        let after_offsets = v.next_offsets.clone();
        let sort_by_time = v.sort_by_time;
        let batch = v.message_limit.min(crate::kafka::LIVE_MAX_PER_POLL);
        let cluster = self.cluster.clone();

        self.last_live_poll = Some(now);
        self.live_fetch_in_flight = true;
        spawn_poll_live_messages(
            cluster,
            topic,
            partition,
            after_offsets,
            batch,
            sort_by_time,
            self.worker_tx.clone(),
        );
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
        if let Some(modal) = self.modal.clone() {
            return self.handle_modal_event(ev, modal);
        }

        if let Event::Key(key) = ev {
            if key.kind != KeyEventKind::Press {
                return Ok(());
            }
            self.handle_key(key)?;
        }
        Ok(())
    }

    fn handle_modal_event(&mut self, ev: Event, modal: Modal) -> io::Result<()> {
        if let Event::Key(key) = ev {
            if key.kind != KeyEventKind::Press {
                return Ok(());
            }
            match key.code {
                KeyCode::Esc => self.close_modal(),
                KeyCode::Tab => {
                    if let Some(m) = self.modal.as_mut() {
                        m.next_field();
                    }
                }
                KeyCode::Backspace => match modal {
                    Modal::Filter => {
                        self.filter_buf.pop();
                    }
                    Modal::Command => {
                        self.command_buf.pop();
                    }
                    _ => {
                        if let Some(m) = self.modal.as_mut() {
                            m.backspace();
                        }
                    }
                },
                KeyCode::Char(c) => match &modal {
                    Modal::Filter => self.filter_buf.push(c),
                    Modal::Command => self.command_buf.push(c),
                    Modal::DeleteConfirm { .. } => {
                        if modal.is_yes(c) {
                            self.confirm_delete();
                        } else if c == 'n' || c == 'N' {
                            self.close_modal();
                        }
                    }
                    _ => {
                        if let Some(m) = self.modal.as_mut() {
                            m.push_char(c);
                        }
                    }
                },
                KeyCode::Enter => self.submit_modal(modal),
                _ => {}
            }
        }
        Ok(())
    }

    fn close_modal(&mut self) {
        self.modal = None;
        self.filter_buf.clear();
        self.command_buf.clear();
    }

    fn submit_modal(&mut self, modal: Modal) {
        match modal {
            Modal::Filter => {
                let filter = self.filter_buf.clone();
                if let ViewStack::Topics(v) = self.current_mut() {
                    v.table.filter = filter;
                }
                self.close_modal();
            }
            Modal::Produce {
                topic,
                key,
                payload,
                ..
            } => {
                if payload.trim().is_empty() {
                    self.status = "payload is required".into();
                    return;
                }
                self.run_produce(topic, key, payload);
                self.close_modal();
            }
            Modal::CreateTopic {
                name, partitions, ..
            } => {
                let name = name.trim().to_string();
                if name.is_empty() {
                    self.status = "topic name is required".into();
                    return;
                }
                let partitions: i32 = match partitions.trim().parse() {
                    Ok(n) if n > 0 => n,
                    _ => {
                        self.status = "partitions must be a positive number".into();
                        return;
                    }
                };
                self.run_create_topic(name, partitions);
                self.close_modal();
            }
            Modal::DeleteConfirm { topic } => {
                self.run_delete_topic(topic);
                self.close_modal();
            }
            Modal::Command => {
                let cmd = self.command_buf.clone();
                self.close_modal();
                self.execute_command(&cmd);
            }
            Modal::MessageLimit { value } => {
                let value = value.trim();
                let limit: usize = match value.parse() {
                    Ok(n) if (10..=10_000).contains(&n) => n,
                    _ => {
                        self.status = "limit must be 10–10000".into();
                        return;
                    }
                };
                self.set_message_limit(limit);
                self.close_modal();
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> io::Result<()> {
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
                    if let ViewStack::Topics(v) = self.current() {
                        self.filter_buf = v.table.filter.clone();
                    }
                    self.modal = Some(Modal::Filter);
                }
            }
            KeyCode::Char(':') => {
                self.command_buf.clear();
                self.modal = Some(Modal::Command);
            }
            KeyCode::Char('c') => {
                if matches!(self.current(), ViewStack::Topics(_)) {
                    self.modal = Some(Modal::CreateTopic {
                        name: String::new(),
                        partitions: "3".into(),
                        field: ModalField::First,
                    });
                }
            }
            KeyCode::Char('d') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.scroll_detail_down(3);
                } else if let Some(topic) = self.selected_topic_name() {
                    if topic.starts_with('_') {
                        self.status = "cannot delete internal topics".into();
                    } else {
                        self.modal = Some(Modal::DeleteConfirm { topic });
                    }
                }
            }
            KeyCode::Char('n') => {
                if let Some(topic) = self.selected_topic_name() {
                    self.modal = Some(Modal::Produce {
                        topic,
                        key: String::new(),
                        payload: String::new(),
                        field: ModalField::First,
                    });
                }
            }
            KeyCode::Char('j') | KeyCode::Down => self.nav_down(),
            KeyCode::Char('k') | KeyCode::Up => self.nav_up(),
            KeyCode::Enter => self.enter(),
            KeyCode::Esc => self.pop(),
            KeyCode::Char('p') => {
                if matches!(self.current(), ViewStack::Messages(_)) {
                    if let ViewStack::Messages(v) = self.current_mut() {
                        v.cycle_partition();
                    }
                    self.reload_messages();
                } else {
                    self.show_partitions();
                }
            }
            KeyCode::Char('i') => {
                if matches!(self.current(), ViewStack::Messages(_)) {
                    self.show_partitions();
                }
            }
            KeyCode::Char('b') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.from_end = true;
                    v.live = false;
                    self.last_live_poll = None;
                    self.reload_messages();
                }
            }
            KeyCode::Char('t') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.from_end = false;
                    v.live = false;
                    self.last_live_poll = None;
                    self.reload_messages();
                }
            }
            KeyCode::Char('s') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.sort_by_time = !v.sort_by_time;
                    self.reload_messages();
                }
            }
            KeyCode::Char('l') => {
                if let ViewStack::Messages(v) = self.current() {
                    self.modal = Some(Modal::MessageLimit {
                        value: v.message_limit.to_string(),
                    });
                }
            }
            KeyCode::Char('+') | KeyCode::Char('=') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    let new = (v.message_limit + 50).min(10_000);
                    self.set_message_limit(new);
                }
            }
            KeyCode::Char('-') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    let new = v.message_limit.saturating_sub(50).max(10);
                    self.set_message_limit(new);
                }
            }
            KeyCode::Char('f') => self.toggle_live(),
            KeyCode::Char('[') => self.adjust_live_poll(-1),
            KeyCode::Char(']') => self.adjust_live_poll(1),
            KeyCode::Char('o') => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.pretty_json = !v.pretty_json;
                    self.status = if v.pretty_json {
                        "JSON pretty-print on (o toggle)"
                    } else {
                        "JSON pretty-print off (o toggle)"
                    }
                    .into();
                }
            }
            KeyCode::Char('y') => self.copy_selected_message(),
            KeyCode::Char('u') => {
                if matches!(self.current(), ViewStack::Messages(_)) {
                    if let ViewStack::Messages(v) = self.current_mut() {
                        v.scroll_detail_up(3);
                    }
                }
            }
            KeyCode::PageUp => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.scroll_detail_up(10);
                }
            }
            KeyCode::PageDown => {
                if let ViewStack::Messages(v) = self.current_mut() {
                    v.scroll_detail_down(10);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn selected_topic_name(&self) -> Option<String> {
        match self.current() {
            ViewStack::Topics(v) => v.selected_topic().map(str::to_string),
            ViewStack::Messages(v) => Some(v.topic.clone()),
        }
    }

    fn confirm_delete(&mut self) {
        if let Some(Modal::DeleteConfirm { topic }) = self.modal.clone() {
            self.run_delete_topic(topic);
            self.close_modal();
        }
    }

    fn run_create_topic(&mut self, name: String, partitions: i32) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = format!("creating topic {name}…");
        if let Ok(conn) = conn.reconnect() {
            spawn_create_topic(conn, name, partitions, self.worker_tx.clone());
        }
    }

    fn run_delete_topic(&mut self, name: String) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = format!("deleting topic {name}…");
        if let Ok(conn) = conn.reconnect() {
            spawn_delete_topic(conn, name, self.worker_tx.clone());
        }
    }

    fn run_produce(&mut self, topic: String, key: String, payload: String) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = format!("producing to {topic}…");
        let key_opt = if key.trim().is_empty() {
            None
        } else {
            Some(key)
        };
        if let Ok(conn) = conn.reconnect() {
            spawn_produce(conn, topic, key_opt, payload, self.worker_tx.clone());
        }
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
                let limit = self.config.defaults.message_limit;
                let mut mv = MessagesView::new(&topic, limit);
                if let Some(conn) = self.connection.as_ref() {
                    match conn.topic_partitions(&topic) {
                        Ok(parts) => {
                            mv.partition_ids = parts.iter().map(|p| p.id).collect();
                        }
                        Err(e) => {
                            self.status = format!("partitions error: {e:#}");
                        }
                    }
                }
                self.stack.push(ViewStack::Messages(mv));
                self.reload_messages();
            }
        }
    }

    fn pop(&mut self) {
        if self.stack.len() > 1 {
            self.stack.pop();
            self.live_fetch_in_flight = false;
            self.last_live_poll = None;
            self.status = "ready".into();
        }
    }

    fn copy_selected_message(&mut self) {
        let ViewStack::Messages(v) = self.current() else {
            return;
        };
        match v.copy_selected() {
            Ok(bytes) => self.status = format!("copied {bytes} bytes to clipboard (y)"),
            Err(e) => self.status = format!("copy failed: {e:#}"),
        }
    }

    fn toggle_live(&mut self) {
        let ViewStack::Messages(v) = self.current_mut() else {
            return;
        };
        v.live = !v.live;
        if v.live {
            v.from_end = true;
            if v.messages.is_empty() {
                self.reload_messages();
            } else {
                v.sync_next_offsets();
            }
            self.last_live_poll = None;
            let secs = clamp_live_poll_secs(self.config.defaults.live_poll_secs);
            self.status = format!("live on · poll every {secs}s (f off, [/] interval)");
        } else {
            self.live_fetch_in_flight = false;
            self.last_live_poll = None;
            self.status = "live off".into();
        }
    }

    fn adjust_live_poll(&mut self, delta: i64) {
        if !matches!(self.current(), ViewStack::Messages(_)) {
            return;
        }
        let cur = self.config.defaults.live_poll_secs as i64;
        let next = clamp_live_poll_secs((cur + delta) as u64);
        self.config.defaults.live_poll_secs = next;
        self.last_live_poll = None;
        match self.config.save() {
            Ok(()) => self.status = format!("live poll interval → {next}s (saved)"),
            Err(e) => self.status = format!("live poll interval → {next}s (not saved: {e:#})"),
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
        let topic = match self.current() {
            ViewStack::Messages(v) => v.topic.clone(),
            _ => return,
        };
        self.reload_messages_for(&topic);
    }

    fn reload_messages_for(&mut self, topic: &str) {
        let topic = topic.to_string();
        let (partition, limit, from_end, sort_by_time) = match self.current() {
            ViewStack::Messages(v) => (v.partition, v.message_limit, v.from_end, v.sort_by_time),
            _ => return,
        };
        let Some(conn) = self.connection.as_ref() else {
            return;
        };
        self.loading = true;
        self.status = format!("loading messages for {topic}…");
        if let Ok(conn) = conn.reconnect() {
            spawn_fetch_messages(
                conn,
                topic,
                partition,
                limit,
                from_end,
                sort_by_time,
                self.worker_tx.clone(),
            );
        }
    }

    fn set_message_limit(&mut self, limit: usize) {
        self.config.defaults.message_limit = limit;
        if let ViewStack::Messages(v) = self.current_mut() {
            v.message_limit = limit;
        }
        match self.config.save() {
            Ok(()) => self.status = format!("message limit → {limit} (saved)"),
            Err(e) => self.status = format!("message limit → {limit} (not saved: {e:#})"),
        }
        self.reload_messages();
    }

    fn show_partitions(&mut self) {
        let topic = self.selected_topic_name();
        let Some(topic) = topic else { return };
        let Some(conn) = self.connection.as_ref() else {
            return;
        };

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
                    let poll_secs = clamp_live_poll_secs(self.config.defaults.live_poll_secs);
                    if let ViewStack::Messages(v) = self.current_mut() {
                        if v.topic == topic {
                            let count = msgs.len();
                            let live = v.live;
                            v.load(msgs);
                            self.status = if live {
                                format!("live · {count} msgs · poll {poll_secs}s")
                            } else {
                                format!("{count} messages")
                            };
                        }
                    }
                }
                Err(e) => self.status = format!("messages error: {e:#}"),
            },
            WorkerMsg::LiveMessages { topic, result } => {
                self.live_fetch_in_flight = false;
                let poll_secs = clamp_live_poll_secs(self.config.defaults.live_poll_secs);
                match result {
                    Ok(new) => {
                        if let ViewStack::Messages(v) = self.current_mut() {
                            if v.topic == topic && v.live {
                                let single = v.partition.is_some();
                                let limit = v.message_limit;
                                let sort = v.sort_by_time;
                                let n = v.append_live(new, limit, sort, single);
                                let total = v.messages.len();
                                self.status = if n > 0 {
                                    format!("live · {total} msgs (+{n}) · {poll_secs}s")
                                } else {
                                    format!("live · {total} msgs · {poll_secs}s")
                                };
                            }
                        }
                    }
                    Err(e) => self.status = format!("live error: {e:#}"),
                }
            }
            WorkerMsg::Op(Ok(msg)) => {
                let reload_msgs = msg.contains("produced");
                self.status = msg;
                self.refresh_topics();
                if reload_msgs {
                    if let ViewStack::Messages(v) = self.current() {
                        let topic = v.topic.clone();
                        self.reload_messages_for(&topic);
                    }
                }
            }
            WorkerMsg::Op(Err(e)) => self.status = format!("error: {e:#}"),
        }
    }

    pub fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();

        if self.show_partitions_popup && self.modal.is_none() {
            self.render_partitions_popup(frame, area);
            return;
        }

        let cluster = self.cluster_name.clone();
        let status = self.status.clone();
        let loading = self.loading;

        let show_help = match self.current() {
            ViewStack::Topics(v) => v.show_help,
            ViewStack::Messages(v) => v.show_help,
        };
        let chunks = layout_main(area, show_help);

        match self.current_mut() {
            ViewStack::Topics(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::Messages(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
        }

        if let Some(ref modal) = self.modal {
            let extra = match modal {
                Modal::Filter => Some(self.filter_buf.as_str()),
                Modal::Command => Some(self.command_buf.as_str()),
                _ => None,
            };
            draw_modal(frame, area, modal, extra);
        }
    }

    fn render_partitions_popup(&mut self, frame: &mut Frame, area: ratatui::layout::Rect) {
        use crate::ui::theme;
        use ratatui::layout::{Constraint, Layout};
        use ratatui::widgets::{Block, Borders, Clear, Paragraph};

        frame.render_widget(Clear, area);
        let popup = Layout::vertical([Constraint::Min(5)]).split(area);
        let text = self.partition_lines.join("\n");
        frame.render_widget(
            Paragraph::new(text).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme::MODAL_BORDER)
                    .title(" partitions (Esc) ")
                    .title_style(theme::BLOCK_TITLE),
            ),
            popup[0],
        );
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

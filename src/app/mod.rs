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

use crate::config::{clamp_live_poll_secs, clamp_watermark_parallelism, AppConfig, ClusterConfig};
use crate::config::{KafkaConnectConfig, SchemaRegistryConfig};
use crate::kafka::{
    AclEntry, AclSpec, ClusterConnection, ListTopicsOptions, PartitionInfo, ResetStrategy,
    TopicInfo,
};
use crate::views::{
    AclsView, ConnectorDetailView, ConnectorsView, ContextListView, GroupDetailsView, GroupsView,
    LabelListView, MessagesView, SchemaDetailView, SchemasView, Screen, TopicsView, ViewStack,
};

use crate::ui::draw_sidebar;
use modal::{draw_modal, layout_app, AclFormField, Modal, ModalField};
use worker::{
    spawn_connect_delete, spawn_connect_pause, spawn_connect_restart, spawn_connect_resume,
    spawn_connector_detail, spawn_create_acl, spawn_create_topic, spawn_delete_acl,
    spawn_delete_group, spawn_delete_topic, spawn_fetch_messages, spawn_group_offsets,
    spawn_list_acls, spawn_list_connectors, spawn_list_groups, spawn_list_schemas,
    spawn_list_topics, spawn_poll_live_messages, spawn_produce, spawn_replace_acl,
    spawn_reset_group_offsets, spawn_schema_version, spawn_schema_versions, WorkerMsg,
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
    /// Время старта приложения — пока `splash_until` не истёк, показываем splash-screen.
    splash_until: Option<Instant>,
    /// Кэш последнего списка топиков с брокера (для переключения sidebar без refetch).
    cached_topics: Vec<TopicInfo>,
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
            splash_until: Some(Instant::now() + crate::ui::SPLASH_DURATION),
            cached_topics: Vec::new(),
        })
    }

    /// Активен ли splash-screen прямо сейчас.
    fn splash_active(&self) -> bool {
        self.splash_until
            .map(|deadline| Instant::now() < deadline)
            .unwrap_or(false)
    }

    /// Гасит splash немедленно (по клавише или по таймеру в render-tick).
    fn dismiss_splash(&mut self) {
        self.splash_until = None;
    }

    /// Периодический live-poll (вызывается из главного цикла, не блокирует UI).
    pub fn tick(&mut self) {
        if self.splash_active() {
            return;
        }
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
        let opts = ListTopicsOptions {
            fetch_watermarks: self.config.defaults.fetch_watermarks,
            parallelism: clamp_watermark_parallelism(self.config.defaults.watermark_parallelism),
        };
        match ClusterConnection::connect(&self.cluster).map(|c| c.with_list_topics_options(opts)) {
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
        // Любая клавиша во время splash гасит его и НЕ пропускается дальше — иначе
        // юзер случайно дёрнет, например, `q` и сразу выйдет, не увидев приложение.
        if self.splash_active() {
            if let Event::Key(key) = ev {
                if key.kind == KeyEventKind::Press {
                    self.dismiss_splash();
                }
            }
            return Ok(());
        }

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
                            self.confirm_delete_topic();
                        } else if c == 'n' || c == 'N' {
                            self.close_modal();
                        }
                    }
                    Modal::DeleteGroupConfirm { group } => {
                        if modal.is_yes(c) {
                            let group = group.clone();
                            self.close_modal();
                            self.run_delete_group(group);
                        } else if c == 'n' || c == 'N' {
                            self.close_modal();
                        }
                    }
                    Modal::DeleteLabelConfirm { label, .. } => {
                        if modal.is_yes(c) {
                            let label = label.clone();
                            self.close_modal();
                            self.run_delete_label(label);
                        } else if c == 'n' || c == 'N' {
                            self.close_modal();
                        }
                    }
                    Modal::DeleteAclConfirm { spec, .. } => {
                        if modal.is_yes(c) {
                            let spec = spec.clone();
                            self.close_modal();
                            self.run_delete_acl(spec);
                        } else if c == 'n' || c == 'N' {
                            self.close_modal();
                        }
                    }
                    Modal::DeleteConnectorConfirm { name } => {
                        if modal.is_yes(c) {
                            let name = name.clone();
                            self.close_modal();
                            self.run_connect_delete(name);
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
                match self.current_mut() {
                    ViewStack::Topics(v) => v.table.filter = filter,
                    ViewStack::Groups(v) => v.table.filter = filter,
                    ViewStack::Labels(v) => v.table.filter = filter,
                    ViewStack::Contexts(v) => v.table.filter = filter,
                    ViewStack::Acls(v) => v.table.filter = filter,
                    ViewStack::Schemas(v) => v.table.filter = filter,
                    ViewStack::Connectors(v) => v.table.filter = filter,
                    _ => {}
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
            Modal::DeleteGroupConfirm { group } => {
                self.run_delete_group(group);
                self.close_modal();
            }
            Modal::DeleteLabelConfirm { label, .. } => {
                self.run_delete_label(label);
                self.close_modal();
            }
            Modal::ResetOffsets { group, spec } => {
                let strategy = match parse_reset_spec(spec.trim()) {
                    Ok(s) => s,
                    Err(e) => {
                        self.status = e;
                        return;
                    }
                };
                self.run_reset_offsets(group, strategy);
                self.close_modal();
            }
            Modal::TopicLabel {
                label,
                add,
                topic_count: _,
            } => {
                self.apply_topic_label(label.trim(), add);
                self.close_modal();
            }
            Modal::AclForm {
                edit,
                replace,
                resource_type,
                resource_name,
                pattern_type,
                principal,
                host,
                operation,
                permission,
                ..
            } => {
                let spec = AclSpec {
                    resource_type: resource_type.trim().to_string(),
                    resource_name: resource_name.trim().to_string(),
                    pattern_type: pattern_type.trim().to_string(),
                    principal: principal.trim().to_string(),
                    host: host.trim().to_string(),
                    operation: operation.trim().to_string(),
                    permission: permission.trim().to_string(),
                };
                if spec.principal.is_empty() {
                    self.status = "principal is required".into();
                    return;
                }
                if edit {
                    if let Some(old) = replace {
                        self.run_replace_acl(old, spec);
                    } else {
                        self.status = "edit: missing original ACL".into();
                        return;
                    }
                } else {
                    self.run_create_acl(spec);
                }
                self.close_modal();
            }
            Modal::DeleteAclConfirm { spec, .. } => {
                self.run_delete_acl(spec);
                self.close_modal();
            }
            Modal::DeleteConnectorConfirm { name } => {
                self.run_connect_delete(name);
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
            KeyCode::Char('/') => match self.current() {
                ViewStack::Topics(v) => {
                    self.filter_buf = v.table.filter.clone();
                    self.modal = Some(Modal::Filter);
                }
                ViewStack::Groups(v) => {
                    self.filter_buf = v.table.filter.clone();
                    self.modal = Some(Modal::Filter);
                }
                ViewStack::Labels(v) => {
                    self.filter_buf = v.table.filter.clone();
                    self.modal = Some(Modal::Filter);
                }
                ViewStack::Contexts(v) => {
                    self.filter_buf = v.table.filter.clone();
                    self.modal = Some(Modal::Filter);
                }
                ViewStack::Acls(v) => {
                    self.filter_buf = v.table.filter.clone();
                    self.modal = Some(Modal::Filter);
                }
                ViewStack::Schemas(v) => {
                    self.filter_buf = v.table.filter.clone();
                    self.modal = Some(Modal::Filter);
                }
                ViewStack::Connectors(v) => {
                    self.filter_buf = v.table.filter.clone();
                    self.modal = Some(Modal::Filter);
                }
                _ => {}
            },
            KeyCode::Char('1') => self.switch_nav(Screen::Topics),
            KeyCode::Char('2') => self.switch_nav(Screen::Groups),
            KeyCode::Char('3') => self.switch_nav(Screen::Labels),
            KeyCode::Char('4') => self.switch_nav(Screen::Contexts),
            KeyCode::Char('5') => self.switch_nav(Screen::Acls),
            KeyCode::Char('6') => self.switch_nav(Screen::Schemas),
            KeyCode::Char('7') => self.switch_nav(Screen::Connectors),
            KeyCode::Char(' ') => {
                if let ViewStack::Topics(v) = self.current_mut() {
                    v.toggle_mark();
                    let n = v.table.marked_count();
                    self.status = if n > 0 {
                        format!("{n} topic(s) marked (Space toggle, D clear)")
                    } else {
                        "mark cleared".into()
                    };
                }
            }
            KeyCode::Char('L') => {
                if let ViewStack::Topics(v) = self.current() {
                    let topics = v.target_topic_names();
                    if topics.is_empty() {
                        self.status = "select topic(s) first".into();
                    } else {
                        self.modal = Some(Modal::TopicLabel {
                            label: String::new(),
                            add: true,
                            topic_count: topics.len(),
                        });
                    }
                }
            }
            KeyCode::Char('U') => {
                if let ViewStack::Topics(v) = self.current() {
                    let topics = v.target_topic_names();
                    if topics.is_empty() {
                        self.status = "select topic(s) first".into();
                    } else {
                        self.modal = Some(Modal::TopicLabel {
                            label: String::new(),
                            add: false,
                            topic_count: topics.len(),
                        });
                    }
                }
            }
            KeyCode::Char('D') => {
                if let ViewStack::Topics(v) = self.current_mut() {
                    v.clear_marks();
                    self.status = "marks cleared".into();
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
                } else if matches!(self.current(), ViewStack::Acls(_)) {
                    self.open_acl_create();
                }
            }
            KeyCode::Char('e') => {
                if let ViewStack::Acls(v) = self.current() {
                    if let Some(acl) = v.selected_acl().cloned() {
                        self.open_acl_edit(&acl);
                    } else {
                        self.status = "select an ACL first".into();
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
            KeyCode::Char('g') => self.open_groups(),
            KeyCode::Char('R') => {
                if let ViewStack::ConnectorDetail(v) = self.current() {
                    self.run_connect_restart(v.name.clone());
                } else if let Some(group) = self.selected_group_id() {
                    self.modal = Some(Modal::ResetOffsets {
                        group,
                        spec: String::new(),
                    });
                }
            }
            KeyCode::Char('P') => {
                if let ViewStack::ConnectorDetail(v) = self.current() {
                    self.run_connect_pause(v.name.clone());
                }
            }
            KeyCode::Char('O') => {
                if let ViewStack::ConnectorDetail(v) = self.current() {
                    self.run_connect_resume(v.name.clone());
                }
            }
            KeyCode::Char('u') => match self.current_mut() {
                ViewStack::Messages(v) => v.scroll_detail_up(3),
                ViewStack::SchemaDetail(v) => v.scroll_detail_up(3),
                ViewStack::ConnectorDetail(v) => v.scroll_detail_up(3),
                _ => {}
            },
            KeyCode::Char('d') => {
                if let ViewStack::ConnectorDetail(v) = self.current() {
                    self.modal = Some(Modal::DeleteConnectorConfirm {
                        name: v.name.clone(),
                    });
                } else if let ViewStack::Acls(v) = self.current() {
                    if let Some(acl) = v.selected_acl() {
                        let spec = acl.to_spec();
                        let summary = acl_entry_summary(acl);
                        self.modal = Some(Modal::DeleteAclConfirm { spec, summary });
                    }
                } else if let ViewStack::Messages(v) = self.current_mut() {
                    v.scroll_detail_down(3);
                } else if let ViewStack::SchemaDetail(v) = self.current_mut() {
                    v.scroll_detail_down(3);
                } else if let ViewStack::Labels(v) = self.current() {
                    if let Some(label) = v.selected_label() {
                        let label = label.to_string();
                        let count = self
                            .config
                            .topic_labels
                            .label_topic_count(&self.cluster_name, &label);
                        self.modal = Some(Modal::DeleteLabelConfirm {
                            label,
                            topic_count: count,
                        });
                    }
                } else if let ViewStack::Groups(v) = self.current() {
                    if let Some(group) = v.selected_group().cloned() {
                        if !group.is_empty_or_dead() {
                            self.status = format!(
                                "cannot delete: group is {} (must be Empty/Dead)",
                                group.state
                            );
                        } else {
                            self.modal = Some(Modal::DeleteGroupConfirm { group: group.id });
                        }
                    }
                } else if let Some(topic) = self.selected_topic_name() {
                    if topic.starts_with('_') {
                        self.status = "cannot delete internal topics".into();
                    } else {
                        self.modal = Some(Modal::DeleteConfirm { topic });
                    }
                }
            }
            KeyCode::PageUp => match self.current_mut() {
                ViewStack::Messages(v) => v.scroll_detail_up(10),
                ViewStack::SchemaDetail(v) => v.scroll_detail_up(10),
                ViewStack::ConnectorDetail(v) => v.scroll_detail_up(10),
                _ => {}
            },
            KeyCode::PageDown => match self.current_mut() {
                ViewStack::Messages(v) => v.scroll_detail_down(10),
                ViewStack::SchemaDetail(v) => v.scroll_detail_down(10),
                ViewStack::ConnectorDetail(v) => v.scroll_detail_down(10),
                _ => {}
            },
            _ => {}
        }
        Ok(())
    }

    fn selected_topic_name(&self) -> Option<String> {
        match self.current() {
            ViewStack::Topics(v) => v.selected_topic().map(str::to_string),
            ViewStack::Messages(v) => Some(v.topic.clone()),
            ViewStack::Groups(_)
            | ViewStack::GroupDetails(_)
            | ViewStack::Labels(_)
            | ViewStack::Contexts(_)
            | ViewStack::Acls(_)
            | ViewStack::Schemas(_)
            | ViewStack::SchemaDetail(_)
            | ViewStack::Connectors(_)
            | ViewStack::ConnectorDetail(_) => None,
        }
    }

    fn selected_group_id(&self) -> Option<String> {
        match self.current() {
            ViewStack::Groups(v) => v.selected_id().map(str::to_string),
            ViewStack::GroupDetails(v) => Some(v.group.clone()),
            _ => None,
        }
    }

    fn confirm_delete_topic(&mut self) {
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
            ViewStack::Groups(v) => v.show_help = !v.show_help,
            ViewStack::GroupDetails(v) => v.show_help = !v.show_help,
            ViewStack::Labels(v) => v.show_help = !v.show_help,
            ViewStack::Contexts(v) => v.show_help = !v.show_help,
            ViewStack::Acls(v) => v.show_help = !v.show_help,
            ViewStack::Schemas(v) => v.show_help = !v.show_help,
            ViewStack::SchemaDetail(v) => v.show_help = !v.show_help,
            ViewStack::Connectors(v) => v.show_help = !v.show_help,
            ViewStack::ConnectorDetail(v) => v.show_help = !v.show_help,
        }
    }

    fn nav_down(&mut self) {
        if let ViewStack::SchemaDetail(v) = self.current_mut() {
            v.next_version();
            if let Some(ver) = v.current_version() {
                let subject = v.subject.clone();
                self.reload_schema_version(subject, ver);
            }
            return;
        }
        match self.current_mut() {
            ViewStack::Topics(v) => v.table.next(),
            ViewStack::Messages(v) => v.next(),
            ViewStack::Groups(v) => v.table.next(),
            ViewStack::GroupDetails(v) => v.table.next(),
            ViewStack::Labels(v) => v.table.next(),
            ViewStack::Contexts(v) => v.table.next(),
            ViewStack::Acls(v) => v.table.next(),
            ViewStack::Schemas(v) => v.table.next(),
            ViewStack::Connectors(v) => v.table.next(),
            _ => {}
        }
    }

    fn nav_up(&mut self) {
        if let ViewStack::SchemaDetail(v) = self.current_mut() {
            v.prev_version();
            if let Some(ver) = v.current_version() {
                let subject = v.subject.clone();
                self.reload_schema_version(subject, ver);
            }
            return;
        }
        match self.current_mut() {
            ViewStack::Topics(v) => v.table.prev(),
            ViewStack::Messages(v) => v.prev(),
            ViewStack::Groups(v) => v.table.prev(),
            ViewStack::GroupDetails(v) => v.table.prev(),
            ViewStack::Labels(v) => v.table.prev(),
            ViewStack::Contexts(v) => v.table.prev(),
            ViewStack::Acls(v) => v.table.prev(),
            ViewStack::Schemas(v) => v.table.prev(),
            ViewStack::Connectors(v) => v.table.prev(),
            _ => {}
        }
    }

    fn enter(&mut self) {
        match self.current() {
            ViewStack::Topics(v) => {
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
            ViewStack::Groups(v) => {
                if let Some(id) = v.selected_id() {
                    let id = id.to_string();
                    let mut details = GroupDetailsView::new(&id);
                    if let Some(g) = v.selected_group() {
                        details.set_meta(g.state.clone(), g.members);
                    }
                    self.stack.push(ViewStack::GroupDetails(details));
                    self.reload_group_offsets(id);
                }
            }
            ViewStack::Labels(v) => {
                if let Some(label) = v.selected_label() {
                    let label = label.to_string();
                    let cluster = self.cluster_name.clone();
                    let mut tv = TopicsView::new();
                    tv.set_label_filter(Some(label.clone()));
                    if !self.cached_topics.is_empty() {
                        tv.load_with_labels(
                            self.cached_topics.clone(),
                            &self.config.topic_labels,
                            &cluster,
                        );
                    }
                    self.stack = vec![ViewStack::Topics(tv)];
                    if self.cached_topics.is_empty() {
                        self.refresh_topics();
                    } else {
                        self.status = format!("filter: label @{label}");
                    }
                }
            }
            ViewStack::Contexts(v) => {
                if let Some(name) = v.selected_context() {
                    if name == self.cluster_name {
                        self.switch_nav(Screen::Topics);
                        self.status = format!("context: {name}");
                    } else {
                        self.switch_cluster(&name);
                    }
                }
            }
            ViewStack::Schemas(v) => {
                if let Some(subject) = v.selected_subject() {
                    self.open_schema_detail(subject.to_string());
                }
            }
            ViewStack::Connectors(v) => {
                if let Some(name) = v.selected_name() {
                    self.open_connector_detail(name.to_string());
                }
            }
            _ => {}
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
            ViewStack::Groups(_) => self.refresh_groups(),
            ViewStack::GroupDetails(v) => {
                let group = v.group.clone();
                self.reload_group_offsets(group);
            }
            ViewStack::Labels(_) => {
                let cluster = self.cluster_name.clone();
                let store = self.config.topic_labels.clone();
                if let ViewStack::Labels(v) = self.current_mut() {
                    v.load(&store, &cluster);
                }
                self.status = "labels refreshed".into();
            }
            ViewStack::Contexts(_) => {
                let active = self.cluster_name.clone();
                let config = self.config.clone();
                if let ViewStack::Contexts(v) = self.current_mut() {
                    v.load(&config, &active);
                }
                self.status = "contexts refreshed".into();
            }
            ViewStack::Acls(_) => self.refresh_acls(),
            ViewStack::Schemas(_) => self.refresh_schemas(),
            ViewStack::Connectors(_) => self.refresh_connectors(),
            ViewStack::ConnectorDetail(v) => {
                self.reload_connector_detail(v.name.clone());
            }
            ViewStack::SchemaDetail(v) => {
                let subject = v.subject.clone();
                if let Some(ver) = v.current_version() {
                    self.reload_schema_version(subject, ver);
                } else {
                    self.reload_schema_versions(subject);
                }
            }
        }
    }

    fn schema_registry_config(&self) -> Option<SchemaRegistryConfig> {
        self.cluster.schema_registry.clone()
    }

    fn refresh_schemas(&mut self) {
        let Some(cfg) = self.schema_registry_config() else {
            self.status =
                "schema_registry not configured — add [clusters.<name>.schema_registry]".into();
            return;
        };
        self.loading = true;
        self.status = "loading schemas…".into();
        spawn_list_schemas(cfg, self.worker_tx.clone());
    }

    fn kafka_connect_config(&self) -> Option<KafkaConnectConfig> {
        self.cluster.kafka_connect.clone()
    }

    fn refresh_connectors(&mut self) {
        let Some(cfg) = self.kafka_connect_config() else {
            self.status =
                "kafka_connect not configured — add [clusters.<name>.kafka_connect]".into();
            return;
        };
        self.loading = true;
        self.status = "loading connectors…".into();
        spawn_list_connectors(cfg, self.worker_tx.clone());
    }

    fn open_connector_detail(&mut self, name: String) {
        let Some(cfg) = self.kafka_connect_config() else {
            self.status = "kafka_connect not configured".into();
            return;
        };
        self.stack
            .push(ViewStack::ConnectorDetail(ConnectorDetailView::new(&name)));
        self.loading = true;
        self.status = format!("loading connector {name}…");
        spawn_connector_detail(cfg, name, self.worker_tx.clone());
    }

    fn reload_connector_detail(&mut self, name: String) {
        let Some(cfg) = self.kafka_connect_config() else {
            self.status = "kafka_connect not configured".into();
            return;
        };
        self.loading = true;
        self.status = format!("reloading {name}…");
        spawn_connector_detail(cfg, name, self.worker_tx.clone());
    }

    fn run_connect_pause(&mut self, name: String) {
        let Some(cfg) = self.kafka_connect_config() else {
            self.status = "kafka_connect not configured".into();
            return;
        };
        self.loading = true;
        self.status = format!("pausing {name}…");
        spawn_connect_pause(cfg, name, self.worker_tx.clone());
    }

    fn run_connect_resume(&mut self, name: String) {
        let Some(cfg) = self.kafka_connect_config() else {
            self.status = "kafka_connect not configured".into();
            return;
        };
        self.loading = true;
        self.status = format!("resuming {name}…");
        spawn_connect_resume(cfg, name, self.worker_tx.clone());
    }

    fn run_connect_restart(&mut self, name: String) {
        let Some(cfg) = self.kafka_connect_config() else {
            self.status = "kafka_connect not configured".into();
            return;
        };
        self.loading = true;
        self.status = format!("restarting {name}…");
        spawn_connect_restart(cfg, name, self.worker_tx.clone());
    }

    fn run_connect_delete(&mut self, name: String) {
        let Some(cfg) = self.kafka_connect_config() else {
            self.status = "kafka_connect not configured".into();
            return;
        };
        self.loading = true;
        self.status = format!("deleting {name}…");
        spawn_connect_delete(cfg, name, self.worker_tx.clone());
    }

    fn open_schema_detail(&mut self, subject: String) {
        let Some(cfg) = self.schema_registry_config() else {
            self.status = "schema_registry not configured".into();
            return;
        };
        self.stack
            .push(ViewStack::SchemaDetail(SchemaDetailView::new(&subject)));
        self.loading = true;
        self.status = format!("loading {subject}…");
        spawn_schema_versions(cfg, subject, self.worker_tx.clone());
    }

    fn reload_schema_versions(&mut self, subject: String) {
        let Some(cfg) = self.schema_registry_config() else {
            self.status = "schema_registry not configured".into();
            return;
        };
        self.loading = true;
        self.status = format!("loading versions for {subject}…");
        spawn_schema_versions(cfg, subject, self.worker_tx.clone());
    }

    fn reload_schema_version(&mut self, subject: String, version: i32) {
        let Some(cfg) = self.schema_registry_config() else {
            self.status = "schema_registry not configured".into();
            return;
        };
        self.loading = true;
        self.status = format!("loading {subject} v{version}…");
        spawn_schema_version(cfg, subject, version, self.worker_tx.clone());
    }

    fn refresh_groups(&mut self) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = "loading consumer groups…".into();
        if let Ok(conn) = conn.reconnect() {
            spawn_list_groups(conn, self.worker_tx.clone());
        }
    }

    fn reload_group_offsets(&mut self, group: String) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = format!("loading offsets for {group}…");
        if let Ok(conn) = conn.reconnect() {
            spawn_group_offsets(conn, group, self.worker_tx.clone());
        }
    }

    fn open_groups(&mut self) {
        self.switch_nav(Screen::Groups);
    }

    fn switch_nav(&mut self, screen: Screen) {
        while self.stack.len() > 1 {
            self.stack.pop();
        }
        let cluster = self.cluster_name.clone();
        match screen {
            Screen::Topics => {
                let mut v = TopicsView::new();
                if !self.cached_topics.is_empty() {
                    v.load_with_labels(
                        self.cached_topics.clone(),
                        &self.config.topic_labels,
                        &cluster,
                    );
                }
                self.stack = vec![ViewStack::Topics(v)];
                if self.cached_topics.is_empty() {
                    self.refresh_topics();
                } else {
                    self.status = "topics".into();
                }
            }
            Screen::Groups => {
                self.stack = vec![ViewStack::Groups(GroupsView::new())];
                self.refresh_groups();
            }
            Screen::Labels => {
                let mut v = LabelListView::new();
                let store = self.config.topic_labels.clone();
                v.load(&store, &cluster);
                self.stack = vec![ViewStack::Labels(v)];
                self.status = "labels".into();
            }
            Screen::Contexts => {
                let mut v = ContextListView::new();
                let config = self.config.clone();
                v.load(&config, &cluster);
                self.stack = vec![ViewStack::Contexts(v)];
                self.status = "contexts".into();
            }
            Screen::Acls => {
                self.stack = vec![ViewStack::Acls(AclsView::new())];
                self.refresh_acls();
            }
            Screen::Schemas => {
                self.stack = vec![ViewStack::Schemas(SchemasView::new())];
                self.refresh_schemas();
            }
            Screen::Connectors => {
                self.stack = vec![ViewStack::Connectors(ConnectorsView::new())];
                self.refresh_connectors();
            }
            _ => {}
        }
    }

    fn open_acl_create(&mut self) {
        self.modal = Some(Modal::AclForm {
            edit: false,
            replace: None,
            resource_type: "topic".into(),
            resource_name: String::new(),
            pattern_type: "literal".into(),
            principal: "User:".into(),
            host: "*".into(),
            operation: "read".into(),
            permission: "allow".into(),
            field: AclFormField::ResourceType,
        });
    }

    fn open_acl_edit(&mut self, entry: &AclEntry) {
        self.modal = Some(Modal::AclForm {
            edit: true,
            replace: Some(entry.to_spec()),
            resource_type: entry.resource_type.clone(),
            resource_name: entry.resource_name.clone(),
            pattern_type: entry.pattern_type.clone(),
            principal: entry.principal.clone(),
            host: entry.host.clone(),
            operation: entry.operation.clone(),
            permission: entry.permission.clone(),
            field: AclFormField::ResourceType,
        });
    }

    fn run_create_acl(&mut self, spec: AclSpec) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = "creating ACL…".into();
        if let Ok(conn) = conn.reconnect() {
            spawn_create_acl(conn, spec, self.worker_tx.clone());
        }
    }

    fn run_delete_acl(&mut self, spec: AclSpec) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = "deleting ACL…".into();
        if let Ok(conn) = conn.reconnect() {
            spawn_delete_acl(conn, spec, self.worker_tx.clone());
        }
    }

    fn run_replace_acl(&mut self, old: AclSpec, new: AclSpec) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = "updating ACL…".into();
        if let Ok(conn) = conn.reconnect() {
            spawn_replace_acl(conn, old, new, self.worker_tx.clone());
        }
    }

    fn refresh_acls(&mut self) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = "loading ACLs…".into();
        if let Ok(conn) = conn.reconnect() {
            spawn_list_acls(conn, self.worker_tx.clone());
        }
    }

    fn apply_topic_label(&mut self, label: &str, add: bool) {
        if label.is_empty() {
            self.status = "label cannot be empty".into();
            return;
        }
        let topics = match self.current() {
            ViewStack::Topics(v) => v.target_topic_names(),
            _ => Vec::new(),
        };
        if topics.is_empty() {
            self.status = "no topics selected".into();
            return;
        }
        let cluster = self.cluster_name.clone();
        if add {
            self.config
                .topic_labels
                .add_label_to_many(&cluster, &topics, label);
        } else {
            self.config
                .topic_labels
                .remove_label_from_many(&cluster, &topics, label);
        }
        let action = if add { "added" } else { "removed" };
        match self.config.save() {
            Ok(()) => {
                self.status = format!(
                    "{action} label '{label}' on {} topic(s) (saved)",
                    topics.len()
                );
            }
            Err(e) => {
                self.status = format!("{action} label '{label}' ({e:#}, not saved)");
            }
        }
        let store = self.config.topic_labels.clone();
        if let ViewStack::Topics(v) = self.current_mut() {
            v.refresh_labels(&store, &cluster);
        }
    }

    fn run_delete_label(&mut self, label: String) {
        let cluster = self.cluster_name.clone();
        let affected = self.config.topic_labels.delete_label(&cluster, &label);
        match self.config.save() {
            Ok(()) => {
                self.status = format!("deleted label '{label}' from {affected} topic(s) (saved)");
            }
            Err(e) => {
                self.status =
                    format!("deleted label '{label}' from {affected} topic(s) (not saved: {e:#})");
            }
        }
        let store = self.config.topic_labels.clone();
        if let ViewStack::Labels(v) = self.current_mut() {
            v.load(&store, &cluster);
        }
        if let ViewStack::Topics(v) = self.current_mut() {
            v.refresh_labels(&store, &cluster);
        }
    }

    fn run_delete_group(&mut self, group: String) {
        let Some(conn) = self.connection.as_ref() else {
            self.status = "not connected".into();
            return;
        };
        self.loading = true;
        self.status = format!("deleting group {group}…");
        if let Ok(conn) = conn.reconnect() {
            spawn_delete_group(conn, group, self.worker_tx.clone());
        }
    }

    fn run_reset_offsets(&mut self, group: String, strategy: ResetStrategy) {
        if self.connection.is_none() {
            self.status = "not connected".into();
            return;
        }
        self.loading = true;
        self.status = format!("resetting offsets for {group}…");
        spawn_reset_group_offsets(
            self.cluster.clone(),
            group,
            strategy,
            self.worker_tx.clone(),
        );
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
                self.cached_topics = topics.clone();
                let cluster = self.cluster_name.clone();
                let store = self.config.topic_labels.clone();
                if let ViewStack::Topics(v) = self.current_mut() {
                    v.load_with_labels(topics, &store, &cluster);
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
            WorkerMsg::Groups(Ok(groups)) => {
                if let ViewStack::Groups(v) = self.current_mut() {
                    v.load(groups);
                }
                self.status = "ready".into();
            }
            WorkerMsg::Groups(Err(e)) => self.status = format!("groups error: {e:#}"),
            WorkerMsg::Acls(Ok(acls)) => {
                if let ViewStack::Acls(v) = self.current_mut() {
                    v.load(acls);
                }
                self.status = "ready".into();
            }
            WorkerMsg::Acls(Err(e)) => self.status = format!("ACL error: {e:#}"),
            WorkerMsg::Schemas(Ok(subjects)) => {
                if let ViewStack::Schemas(v) = self.current_mut() {
                    v.load(subjects);
                }
                self.status = "ready".into();
            }
            WorkerMsg::Schemas(Err(e)) => self.status = format!("schema registry error: {e:#}"),
            WorkerMsg::Connectors(Ok(list)) => {
                if let ViewStack::Connectors(v) = self.current_mut() {
                    v.load(list);
                }
                self.status = "ready".into();
            }
            WorkerMsg::Connectors(Err(e)) => self.status = format!("kafka connect error: {e:#}"),
            WorkerMsg::ConnectorDetail { name, result } => match result {
                Ok(detail) => {
                    if let ViewStack::ConnectorDetail(v) = self.current_mut() {
                        if v.name == name {
                            v.set_detail(detail);
                        }
                    }
                    self.status = format!("connector {name}");
                }
                Err(e) => self.status = format!("connector error: {e:#}"),
            },
            WorkerMsg::SchemaVersions { subject, result } => match result {
                Ok(mut versions) => {
                    versions.sort_unstable();
                    let ver = if let ViewStack::SchemaDetail(v) = self.current_mut() {
                        if v.subject == subject {
                            v.set_versions(versions);
                            v.current_version()
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let Some(ver) = ver {
                        self.reload_schema_version(subject, ver);
                    } else {
                        self.status = "ready".into();
                    }
                }
                Err(e) => self.status = format!("schema versions error: {e:#}"),
            },
            WorkerMsg::SchemaVersion {
                subject,
                version,
                result,
            } => match result {
                Ok(detail) => {
                    if let ViewStack::SchemaDetail(v) = self.current_mut() {
                        if v.subject == subject && v.current_version() == Some(version) {
                            v.set_detail(detail);
                        }
                    }
                    self.status = format!("{subject} v{version}");
                }
                Err(e) => self.status = format!("schema error: {e:#}"),
            },
            WorkerMsg::GroupOffsets { group, result } => match result {
                Ok((info, offsets)) => {
                    let n = offsets.len();
                    let total_lag: i64 = offsets.iter().map(|o| o.lag).sum();
                    if let ViewStack::GroupDetails(v) = self.current_mut() {
                        if v.group == group {
                            v.set_meta(info.state.clone(), info.members);
                            v.load(offsets);
                        }
                    }
                    self.status = format!(
                        "{group} · {} · {n} part · total lag {total_lag}",
                        info.state
                    );
                }
                Err(e) => self.status = format!("offsets error: {e:#}"),
            },
            WorkerMsg::Op(Ok(msg)) => {
                let is_produce = msg.starts_with("produced");
                let is_topic_op =
                    msg.starts_with("created topic") || msg.starts_with("deleted topic");
                let is_group_op = msg.starts_with("deleted group");
                let is_reset = msg.starts_with("reset offsets");
                let is_acl_op = msg.contains("ACL");
                let is_connect_op = msg.contains("connector");
                let deleted_connector = msg.contains("deleted connector");
                self.status = msg;

                match self.current() {
                    ViewStack::Topics(_) if is_topic_op || is_produce => self.refresh_topics(),
                    ViewStack::Messages(v) if is_produce => {
                        let topic = v.topic.clone();
                        self.reload_messages_for(&topic);
                    }
                    ViewStack::Groups(_) if is_group_op || is_reset => self.refresh_groups(),
                    ViewStack::GroupDetails(v) if is_reset => {
                        let g = v.group.clone();
                        self.reload_group_offsets(g);
                    }
                    ViewStack::Acls(_) if is_acl_op => self.refresh_acls(),
                    ViewStack::Connectors(_) | ViewStack::ConnectorDetail(_) if is_connect_op => {
                        self.refresh_connectors();
                        if deleted_connector {
                            if matches!(self.current(), ViewStack::ConnectorDetail(_)) {
                                self.pop();
                            }
                        } else if let ViewStack::ConnectorDetail(v) = self.current() {
                            self.reload_connector_detail(v.name.clone());
                        }
                    }
                    _ => {}
                }
            }
            WorkerMsg::Op(Err(e)) => self.status = format!("error: {e:#}"),
        }
    }

    pub fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();

        if self.splash_active() {
            crate::ui::draw_splash(frame, area);
            return;
        }
        // splash истёк по таймеру → скидываем флаг, чтобы не звать draw_splash снова.
        if self.splash_until.is_some() {
            self.dismiss_splash();
        }

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
            ViewStack::Groups(v) => v.show_help,
            ViewStack::GroupDetails(v) => v.show_help,
            ViewStack::Labels(v) => v.show_help,
            ViewStack::Contexts(v) => v.show_help,
            ViewStack::Acls(v) => v.show_help,
            ViewStack::Schemas(v) => v.show_help,
            ViewStack::SchemaDetail(v) => v.show_help,
            ViewStack::Connectors(v) => v.show_help,
            ViewStack::ConnectorDetail(v) => v.show_help,
        };
        let show_sidebar = self.stack.len() == 1 && self.current().is_root_nav();
        let root_screen = self.current().root_screen();
        let (sidebar_area, chunks) = layout_app(area, show_sidebar, show_help);

        if let Some(sb) = sidebar_area {
            draw_sidebar(frame, sb, root_screen);
        }

        match self.current_mut() {
            ViewStack::Topics(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::Messages(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::Groups(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::GroupDetails(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::Labels(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::Contexts(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::Acls(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::Schemas(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::SchemaDetail(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::Connectors(v) => v.render(
                frame, chunks[0], chunks[1], chunks[2], &cluster, &status, loading,
            ),
            ViewStack::ConnectorDetail(v) => v.render(
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
                    .border_style(theme::modal_border())
                    .title(" partitions (Esc) ")
                    .title_style(theme::block_title()),
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

/// Парсит строку формата `earliest` / `latest` / `offset:N` / `timestamp:UNIX_MS`
/// в `ResetStrategy`. Ошибка возвращается как пользовательская строка статуса.
fn acl_entry_summary(a: &AclEntry) -> String {
    format!(
        "{} {} · {} · {} · {} · {}",
        a.resource_type, a.resource_name, a.principal, a.host, a.operation, a.permission
    )
}

fn parse_reset_spec(spec: &str) -> Result<ResetStrategy, String> {
    let spec = spec.trim();
    if spec.eq_ignore_ascii_case("earliest") {
        return Ok(ResetStrategy::Earliest);
    }
    if spec.eq_ignore_ascii_case("latest") {
        return Ok(ResetStrategy::Latest);
    }
    if let Some(rest) = spec
        .strip_prefix("offset:")
        .or_else(|| spec.strip_prefix("offset="))
    {
        let n: i64 = rest
            .trim()
            .parse()
            .map_err(|_| format!("invalid offset '{rest}', expected integer"))?;
        return Ok(ResetStrategy::ToOffset(n));
    }
    if let Some(rest) = spec
        .strip_prefix("timestamp:")
        .or_else(|| spec.strip_prefix("timestamp="))
        .or_else(|| spec.strip_prefix("ts:"))
    {
        let n: i64 = rest
            .trim()
            .parse()
            .map_err(|_| format!("invalid timestamp '{rest}', expected unix millis"))?;
        return Ok(ResetStrategy::ToTimestamp(n));
    }
    Err(format!(
        "invalid spec '{spec}'; use earliest | latest | offset:N | timestamp:UNIX_MS"
    ))
}

// Часть тем/таблиц/экранов готовится для следующих view (consumer groups, ACL и т.п.).
#![allow(dead_code)]

mod app;
mod ui;
mod views;

pub use y2kexplorer::config;
pub use y2kexplorer::kafka;
pub use y2kexplorer::labels;

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use crossterm::event;
use tracing_subscriber::EnvFilter;

use app::App;
use config::AppConfig;
use ui::theme::Palette;

#[derive(Parser, Debug)]
#[command(name = "y2k", about = "Terminal UI for Apache Kafka")]
struct Cli {
    /// Path to config.toml (default: ~/.config/y2kexplorer/config.toml)
    #[arg(long)]
    config: Option<PathBuf>,

    /// Cluster name from config (overrides defaults.cluster)
    #[arg(short, long)]
    cluster: Option<String>,

    /// UI theme: `dark` (default) or `light`. Overrides `defaults.theme` from config.
    #[arg(long, value_parser = parse_palette)]
    theme: Option<Palette>,
}

fn parse_palette(s: &str) -> Result<Palette, String> {
    s.parse()
}

fn main() -> Result<()> {
    let log_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,librdkafka=off,rdkafka=off"));
    tracing_subscriber::fmt()
        .with_env_filter(log_filter)
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let mut cfg = AppConfig::load(cli.config.as_deref())?;
    if let Some(name) = cli.cluster {
        cfg.defaults.cluster = Some(name);
    }

    // Тема: CLI > config > default(dark). Парсинг из конфига с фолбэком на dark
    // при невалидном значении (без падения — просто варним и продолжаем).
    let palette = cli.theme.unwrap_or_else(|| {
        cfg.defaults
            .theme
            .as_deref()
            .map(|s| s.parse::<Palette>())
            .transpose()
            .unwrap_or_else(|err| {
                eprintln!("y2k: {err}; falling back to dark");
                None
            })
            .unwrap_or_default()
    });
    ui::theme::init(palette);

    let (tx, rx) = mpsc::channel();
    let mut application = App::new(cfg, tx)?;
    application.init_connection();

    ratatui::run(|terminal| -> std::io::Result<()> {
        let tick = Duration::from_millis(100);
        loop {
            while let Ok(msg) = rx.try_recv() {
                application.on_worker_msg(msg);
            }

            application.tick();

            terminal.draw(|frame| application.render(frame))?;

            if event::poll(tick)? {
                application.handle_event(event::read()?)?;
            }

            if application.should_quit() {
                break;
            }
        }
        Ok(())
    })?;

    Ok(())
}

mod app;
mod config;
mod kafka;
mod ui;
mod views;

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use crossterm::event;
use tracing_subscriber::EnvFilter;

use app::App;
use config::AppConfig;

#[derive(Parser, Debug)]
#[command(name = "y2k", about = "Terminal UI for Apache Kafka")]
struct Cli {
    /// Path to config.toml (default: XDG config dir)
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Cluster name from config (overrides defaults.cluster)
    #[arg(short, long)]
    cluster: Option<String>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let mut cfg = AppConfig::load(cli.config.as_deref())?;
    if let Some(name) = cli.cluster {
        cfg.defaults.cluster = Some(name);
    }

    let (tx, rx) = mpsc::channel();
    let mut application = App::new(cfg, tx)?;
    application.init_connection();

    ratatui::run(|terminal| -> std::io::Result<()> {
        let tick = Duration::from_millis(100);
        loop {
            while let Ok(msg) = rx.try_recv() {
                application.on_worker_msg(msg);
            }

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

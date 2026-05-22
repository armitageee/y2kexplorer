//! Диагностика подключения к Kafka (Kerberos + TLS).
//!   cargo run --bin y2k-probe -- -c secure
//!   cargo run --bin y2k-probe -- --config ~/.config/y2kexplorer/config.toml -c secure

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::util::Timeout;

use y2kexplorer::config::{
    apply_kerberos_env, krb5_ca_candidates, resolve_kerberos_ssl_ca, AppConfig, AuthConfig,
    ClusterConfig,
};

#[derive(Parser)]
#[command(name = "y2k-probe")]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(short, long, default_value = "secure")]
    cluster: String,
}

#[derive(Clone, Copy)]
struct TlsMode {
    verify_hostname: bool,
    ca: Option<&'static str>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    let cfg = AppConfig::load(cli.config.as_deref())?;
    let cluster = cfg
        .clusters
        .get(&cli.cluster)
        .with_context(|| format!("cluster '{}' not in config", cli.cluster))?;

    println!("=== y2k-probe: cluster '{}' ===\n", cli.cluster);
    println!("brokers: {:?}", cluster.brokers);

    if let AuthConfig::Kerberos {
        keytab,
        principal,
        krb5_conf,
        ..
    } = &cluster.auth
    {
        println!("principal: {principal}");
        println!("keytab: {} (exists={})", keytab.display(), keytab.exists());
        if let Some(k) = krb5_conf {
            println!("krb5_conf: {} (exists={})", k.display(), k.exists());
            for ca in krb5_ca_candidates(k) {
                println!(
                    "  krb5 CA candidate: {ca} (exists={})",
                    std::path::Path::new(&ca).exists()
                );
            }
        }
        println!();
        test_kinit(keytab, principal, krb5_conf.as_ref())?;
    }

    let scenarios: Vec<(&str, TlsMode)> = vec![
        (
            "current (as y2k TUI)",
            TlsMode {
                verify_hostname: true,
                ca: None,
            },
        ),
        (
            "TLS: no hostname verify",
            TlsMode {
                verify_hostname: false,
                ca: None,
            },
        ),
    ];

    run_scenarios(cluster, &scenarios)?;

    if let Some(ca) = resolve_kerberos_ssl_ca(&cluster.auth) {
        println!("--- resolved ssl_ca: {ca} ---");
        try_connect(
            cluster,
            TlsMode {
                verify_hostname: true,
                ca: None,
            },
            Some(&ca),
        )?;
    }

    Ok(())
}

fn run_scenarios(cluster: &ClusterConfig, scenarios: &[(&str, TlsMode)]) -> Result<()> {
    for (name, tls) in scenarios {
        println!("--- scenario: {name} ---");
        match try_connect(cluster, *tls, tls.ca) {
            Ok(n) => println!("OK: {n} topics\n"),
            Err(e) => println!("FAIL: {e:#}\n"),
        }
    }
    Ok(())
}

fn try_connect(cluster: &ClusterConfig, tls: TlsMode, ca_override: Option<&str>) -> Result<usize> {
    apply_kerberos_env(&cluster.auth);
    let cfg = build_config(cluster, tls, ca_override);
    let admin: AdminClient<DefaultClientContext> = cfg.create().context("create admin client")?;
    let md = admin
        .inner()
        .fetch_metadata(None, Timeout::After(Duration::from_secs(30)))
        .context("fetch metadata")?;
    Ok(md.topics().len())
}

fn build_config(cluster: &ClusterConfig, tls: TlsMode, ca_override: Option<&str>) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", cluster.brokers.join(","));
    cfg.set("client.id", "y2k-probe");
    cfg.set("socket.timeout.ms", "30000");
    cfg.set("socket.connection.setup.timeout.ms", "30000");
    cfg.set("api.version.request", "true");
    cfg.set("log.connection.close", "false");

    if let AuthConfig::Kerberos {
        keytab,
        principal,
        service_name,
        tls: use_tls,
        ssl_ca,
        ..
    } = &cluster.auth
    {
        let ca = ca_override.map(String::from).or_else(|| ssl_ca.clone());
        if *use_tls {
            cfg.set("security.protocol", "SASL_SSL");
            if tls.verify_hostname {
                cfg.set("ssl.endpoint.identification.algorithm", "https");
            } else {
                cfg.set("ssl.endpoint.identification.algorithm", "none");
            }
            if let Some(ca) = ca {
                if std::path::Path::new(&ca).exists() {
                    cfg.set("ssl.ca.location", ca);
                }
            }
        } else {
            cfg.set("security.protocol", "SASL_PLAINTEXT");
        }
        cfg.set("sasl.mechanism", "GSSAPI");
        cfg.set("sasl.kerberos.keytab", keytab.display().to_string());
        cfg.set("sasl.kerberos.principal", principal);
        cfg.set("sasl.kerberos.service.name", service_name);
        cfg.set(
            "sasl.kerberos.kinit.cmd",
            "kinit -t \"%{sasl.kerberos.keytab}\" -k %{sasl.kerberos.principal}",
        );
        cfg.set("sasl.kerberos.min.time.before.relogin", "86400");
    }

    cfg
}

fn test_kinit(
    keytab: &std::path::Path,
    principal: &str,
    krb5_conf: Option<&std::path::PathBuf>,
) -> Result<()> {
    apply_kerberos_env(&AuthConfig::Kerberos {
        keytab: keytab.to_path_buf(),
        principal: principal.to_string(),
        service_name: "kafka".into(),
        tls: true,
        krb5_conf: krb5_conf.cloned(),
        ssl_ca: None,
        tls_verify_hostname: true,
    });

    let ccache = std::env::var("KRB5CCNAME").unwrap_or_default();
    println!("KRB5CCNAME={ccache}");

    let out = Command::new("kinit")
        .arg("-t")
        .arg(keytab)
        .arg("-k")
        .arg(principal)
        .output()
        .context("run kinit")?;

    if out.status.success() {
        println!("kinit: OK");
    } else {
        println!(
            "kinit: FAIL\n  stdout: {}\n  stderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let klist = Command::new("klist").output();
    if let Ok(o) = klist {
        if o.status.success() {
            println!("klist:\n{}", String::from_utf8_lossy(&o.stdout));
        }
    }
    println!();
    Ok(())
}

# Configuration

Default config path: `~/.config/y2kexplorer/config.toml`.

```bash
mkdir -p ~/.config/y2kexplorer
cp config.example.toml ~/.config/y2kexplorer/config.toml
$EDITOR ~/.config/y2kexplorer/config.toml
```

Run with:

```bash
y2k                            # default cluster from defaults.cluster
y2k --cluster <name>           # pick a cluster from [clusters.<name>]
y2k --config /path/to.toml     # custom config path
y2k --theme mono              # UI theme (see [themes.md](themes.md))
y2k-probe --cluster <name>     # connection smoke test, no TUI
```

## Topic-list performance

The Topics view computes a `MESSAGES` column by querying low/high watermarks
per partition. On clusters with many topics (or high broker latency), this can
dominate initial load time. Two knobs control this:

```toml
[defaults]
# Skip per-topic message counts entirely — instant load, MESSAGES column = 0.
fetch_watermarks = true        # default true
# How many threads pipeline watermark RPCs in parallel (1..=64).
watermark_parallelism = 16     # default 16
```

Reference numbers (Kerberos+TLS cluster, 84 topics / 720 partitions):

| Mode | Time |
|---|---|
| sequential (legacy) | ~103 s |
| parallel(16) | ~6.4 s |
| `fetch_watermarks = false` | ~3 s (metadata only) |

Run `y2k-probe -c <cluster> --bench-topics` to measure on your own cluster.

## Authentication

Each cluster has its own `[clusters.<name>.auth]` section. Supported types:

| `type` | Required fields | Notes |
|---|---|---|
| `none` | — | PLAINTEXT, no auth |
| `sasl_plain` | `username`, `password`, `tls` | |
| `sasl_scram` | `username`, `password`, `mechanism` (`SCRAM-SHA-256` or `SCRAM-SHA-512`), `tls` | |
| `ssl` | `ca_location`, `certificate_location`, `key_location`, `key_password` | mTLS |
| `kerberos` | `keytab`, `principal`, `service_name`, `tls`, optional `krb5_conf`, `ssl_ca` | GSSAPI via keytab |

See [`config.example.toml`](../../config.example.toml) for full examples.

## Schema Registry & Kafka Connect

Confluent Schema Registry and Kafka Connect are separate HTTP services (not the Kafka protocol).
Per cluster, add `[clusters.<name>.schema_registry]` and `[clusters.<name>.kafka_connect]` with `url` —
see [`config.example.toml`](../../config.example.toml).

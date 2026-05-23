# y2kexplorer

A terminal UI for Apache Kafka, written in Rust with [ratatui](https://docs.rs/ratatui).
In spirit similar to [k9s](https://github.com/derailed/k9s) — a fast, keyboard-driven dashboard
for browsing topics, peeking at messages, and managing consumer groups.

> [Русская версия README](README_RU.md)

## Features

- **Topics** — list with filter (`/`), partition / replication / message-count columns
- **Messages** — head / tail, configurable limit, per-partition view, time-sorting, live follow
- **Produce** — send messages with key + payload (`n`)
- **Create / delete topics** (`c` / `d`)
- **Consumer groups** — list, state, members, lag per partition (`g` / `:groups`)
- **Reset offsets** — `earliest` / `latest` / `offset:N` / `timestamp:UNIX_MS` (`R`)
- **Delete empty groups** (`d` on Groups)
- **Multi-cluster config** — switch contexts in-app (`:context <name>`)
- **Authentication** — PLAINTEXT, SASL/PLAIN, SCRAM, SSL, **Kerberos (GSSAPI) via keytab**

## Installation

### Pre-built binaries (recommended)

Each `v*` tag publishes self-contained tarballs for two platforms.

#### macOS (Apple Silicon, arm64)

```bash
TAG=v0.0.2-rc        # use the latest tag from Releases
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-aarch64-apple-darwin.tar.gz"
tar -xzf "y2kexplorer-${VER}-aarch64-apple-darwin.tar.gz"
cd "y2kexplorer-${VER}-aarch64-apple-darwin"

# strip Gatekeeper quarantine if downloaded via browser
xattr -dr com.apple.quarantine .

./y2k --help
```

All required `.dylib`s (`libsasl2`, `libssl`, `libcrypto`, `libkrb5`, `libcurl`, …) are bundled
into `lib/` next to the binary and rewritten to `@executable_path/lib/...` via `dylibbundler`.
You **don't need** `brew install cyrus-sasl krb5 openssl@3`.

> If you still see `library load disallowed by system policy`, the CI signing step didn't
> reach the dylibs in this release — re-sign locally:
> ```bash
> codesign --force --sign - lib/*.dylib
> codesign --force --sign - y2k y2k-probe
> ```

#### Linux (x86_64)

Built on Ubuntu 22.04 (glibc 2.35); compatible with **Ubuntu 22.04+**, **Debian 12+**,
**RHEL/Rocky/Alma 9+**, **Fedora 36+**, **openSUSE Leap 15.5+**, Arch.

```bash
# 1. system libraries (once)
sudo apt install libsasl2-2 libssl3 libkrb5-3 libcurl4         # Debian/Ubuntu
# or
sudo dnf install cyrus-sasl-lib openssl-libs krb5-libs libcurl  # Fedora/RHEL

# 2. download & run
TAG=v0.0.2-rc
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
tar -xzf "y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
cd "y2kexplorer-${VER}-x86_64-unknown-linux-gnu"
./y2k --help
```

> Won't run on Ubuntu 20.04 / Debian 11 / RHEL 8 / Alpine (older glibc or musl) —
> build from source instead.

### Build from source

Requires Rust 1.75+, CMake, pkg-config, OpenSSL, Cyrus SASL, MIT Kerberos and libcurl:

```bash
# macOS
brew install cmake pkg-config openssl@3 cyrus-sasl krb5

# Debian/Ubuntu
sudo apt install cmake pkg-config libsasl2-dev libssl-dev libkrb5-dev libcurl4-openssl-dev
```

Then:

```bash
git clone https://github.com/armitageee/y2kexplorer.git
cd y2kexplorer
cargo build --release --bin y2k --bin y2k-probe --all-features
./target/release/y2k --help
```

If you have [`just`](https://github.com/casey/just), run `just build` instead.

## Configuration

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
y2k-probe --cluster <name>     # connection smoke test, no TUI
```

### Authentication

Each cluster has its own `[clusters.<name>.auth]` section. Supported types:

| `type` | Required fields | Notes |
|---|---|---|
| `none` | — | PLAINTEXT, no auth |
| `sasl_plain` | `username`, `password`, `tls` | |
| `sasl_scram` | `username`, `password`, `mechanism` (`SCRAM-SHA-256` or `SCRAM-SHA-512`), `tls` | |
| `ssl` | `ca_location`, `certificate_location`, `key_location`, `key_password` | mTLS |
| `kerberos` | `keytab`, `principal`, `service_name`, `tls`, optional `krb5_conf`, `ssl_ca` | GSSAPI via keytab |

See [`config.example.toml`](config.example.toml) for full examples.

## Keybindings

### Global

| Key | Action |
|---|---|
| `j` / `k`, `↑` / `↓` | navigate |
| `Enter` | open selection |
| `Esc` | back / close modal |
| `r` | refresh current view |
| `:` | command palette (`context`, `clusters`, `groups`, `limit`, `poll`, `help`) |
| `?` | toggle help |
| `q` | quit |

### Topics

| Key | Action |
|---|---|
| `/` | filter |
| `Enter` | open messages for selected topic |
| `n` | produce — open key + payload editor |
| `c` | create topic (with partitions) |
| `d` | delete topic (confirm with `y`) |
| `p` | partition metadata popup |
| `g` | open Consumer Groups |

### Messages

| Key | Action |
|---|---|
| `b` / `t` | tail (from end) / head (from start) |
| `p` | cycle partition (all → 0 → 1 → …) |
| `i` | partition metadata popup |
| `s` | toggle time-sort vs offset-sort |
| `+` / `-` | change message limit ±50 (10–10000) |
| `l` | enter exact message limit |
| `f` | live follow — poll new messages periodically |
| `[` / `]` | live-poll interval ±1s (1–30) |
| `o` | toggle pretty-print JSON |
| `y` | yank selected message to clipboard |
| `u` / `d` | scroll detail pane |
| `PgUp` / `PgDn` | scroll detail pane fast |
| `n` | produce |

### Consumer groups

| Key | Action |
|---|---|
| `/` | filter by id |
| `Enter` | group details (offsets / lag) |
| `R` | reset offsets |
| `d` | delete group (only when state is `Empty` / `Dead`) |

`R` opens a modal with a single **spec** field. Accepted values:

| Spec | Effect |
|---|---|
| `earliest` | move to low watermark of every partition |
| `latest` | move to high watermark (LEO) |
| `offset:N` | absolute N (clamped to `[low, high]` per partition) |
| `timestamp:UNIX_MS` | first offset with `timestamp >= UNIX_MS` (via `offsets_for_times`) |

> **Note:** offset reset only works when the group has **no active members**
> (state ∈ {`Empty`, `Dead`}). Otherwise the broker returns `REBALANCE_IN_PROGRESS`.
> y2kexplorer pre-checks the group state and surfaces a clear error if the group is live.

## Try it locally with Docker

A minimal Kafka cluster (PLAINTEXT, no auth, seeded with sample topics) is included:

```bash
just up                    # or: docker compose up -d
just dev                   # or: cargo run --release
just down                  # tear down
```

Kafka UI is exposed at <http://localhost:8080> for cross-checking.

## Development

If you have [`just`](https://github.com/casey/just):

```bash
just                # list tasks
just dev            # cargo run --release
just ci             # fmt --check + clippy -D warnings + test
just probe local    # y2k-probe --cluster local
just release v0.1.0 # tag + push (triggers Release workflow)
```

Otherwise the equivalents are:

```bash
cargo run --release
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features -- --skip fetch_messages_from_local_orders
cargo build --release --locked --bin y2k --bin y2k-probe --all-features
```

## License

MIT

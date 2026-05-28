<p align="center">
  <img src=".assets/y2kagent.png" width="560" alt="y2kexplorer mascot">
</p>

<h1 align="center">y2kexplorer</h1>

<p align="center">
  <em>kafka, but make it ps2</em><br>
  <sub>explore your kafka universe ✦ a retro-flavored Kafka TUI in Rust + ratatui</sub>
</p>

<p align="center">
  <a href="README_RU.md">Русская версия</a>
</p>

y2kexplorer is a keyboard-driven terminal user interface (TUI) for exploring and operating an Apache Kafka cluster.
It is an alternative in spirit to [k9s](https://github.com/derailed/k9s), [AKHQ](https://akhq.io/), or [Redpanda Console](https://www.redpanda.com/redpanda-console-kafka-ui) — but built in Rust on [ratatui](https://docs.rs/ratatui) with a clean multi-panel layout inspired by [eilmeldung](https://github.com/christo-auer/eilmeldung).

The tool offers the following features:

- Browse **topics** with filter, partition metadata, and optional message counts.
- Read **messages** — head / tail, per-partition view, time-sorting, live follow, JSON pretty-print.
- **Produce** messages and **create / delete topics**.
- Manage **consumer groups** — list, lag, reset offsets, delete empty groups.
- **Schema Registry** — subjects, versions, Avro/JSON schema bodies.
- **Kafka Connect** — connectors, status, pause / resume / restart / delete.
- **ACLs**, local **topic labels**, and **multi-cluster** switching in-app.
- **Authentication** — PLAINTEXT, SASL/PLAIN, SCRAM, SSL, Kerberos (GSSAPI) via keytab.
- Four **UI themes** for dark or light terminals — switch at runtime with `T`.

[![asciicast](https://asciinema.org/a/mtFnSVROdvCeQkC7.svg)](https://asciinema.org/a/mtFnSVROdvCeQkC7)

## Limitations

- Designed for day-to-day exploration and light operations — not a full cluster admin replacement.
- Avro schemas are displayed from Schema Registry; no embedded schema-registry client beyond HTTP browse.
- Large topic lists can be slow when message counts are enabled (see [configuration](docs/en/configuration.md#topic-list-performance)).
- Pre-built Linux binaries target glibc 2.35+ (Ubuntu 22.04 era); older distros need a source build.

## Getting started

```bash
mkdir -p ~/.config/y2kexplorer
cp config.example.toml ~/.config/y2kexplorer/config.toml
$EDITOR ~/.config/y2kexplorer/config.toml

y2k                            # default cluster from config
y2k --cluster <name>           # pick a named cluster
y2k-probe --cluster <name>     # connection smoke test, no TUI
```

Pre-built binaries for **macOS arm64** and **Linux x86_64** are published on the [Releases](https://github.com/armitageee/y2kexplorer/releases) page.
To build from source: `cargo build --release --bin y2k --bin y2k-probe --all-features`.

See [Installation](docs/en/installation.md) for platform-specific steps and bundled libraries.

## Try it

> [!NOTE]
> Docker is required for the bundled local KRaft stack (SASL/PLAIN, ACL, Schema Registry, Kafka Connect demo).

```bash
git clone https://github.com/armitageee/y2kexplorer.git
cd y2kexplorer
cp config.example.toml ~/.config/y2kexplorer/config.toml

just up      # or: docker compose up -d
just dev     # or: cargo run --release -- --cluster local
```

Details: [Local Docker stack](docs/en/docker.md).

## Documentation

| Topic | Guide |
|---|---|
| Installation | [docs/en/installation.md](docs/en/installation.md) |
| Configuration & auth | [docs/en/configuration.md](docs/en/configuration.md) |
| UI themes | [docs/en/themes.md](docs/en/themes.md) |
| Keybindings | [docs/en/keybindings.md](docs/en/keybindings.md) |
| Local Docker stack | [docs/en/docker.md](docs/en/docker.md) |
| Development | [docs/en/development.md](docs/en/development.md) |

Full index: [docs/README.md](docs/README.md) · [docs/README.ru.md](docs/README.ru.md) (русский).

## License

MIT

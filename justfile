# y2kexplorer dev tasks
# Run `just` to list, `just <task>` to run one.
#
# Install just: https://github.com/casey/just
#   macOS:   brew install just
#   Linux:   cargo install just
#   nix:     nix profile install nixpkgs#just

set shell := ["bash", "-euo", "pipefail", "-c"]

# default cluster name for `just probe` / `just run`
cluster := "local"

# default tag prefix for `just release`
default_tag := "v0.1.0"

# ---------- meta ----------

# show available tasks
default:
    @just --list

# ---------- run ----------

# cargo run --release (TUI), pass extra flags via VARARGS, e.g.: just dev -- --cluster secure
dev *FLAGS:
    cargo run --release --bin y2k -- {{FLAGS}}

# debug build cargo run (no --release)
dev-debug *FLAGS:
    cargo run --bin y2k -- {{FLAGS}}

# y2k-probe smoke test against a configured cluster
probe cluster=cluster:
    cargo run --release --bin y2k-probe -- --cluster {{cluster}}

# ---------- build ----------

# release build of both binaries (matches CI)
build:
    cargo build --release --locked --bin y2k --bin y2k-probe --all-features

# debug build
build-debug:
    cargo build --bin y2k --bin y2k-probe --all-features

# remove target/
clean:
    cargo clean

# ---------- quality ----------

# rustfmt + clippy + tests (full local CI parity)
ci: fmt-check lint test

# rustfmt write
fmt:
    cargo fmt --all

# rustfmt check (CI-style)
fmt-check:
    cargo fmt --all -- --check

# clippy with warnings-as-errors (matches CI)
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# clippy with auto-fix where possible
lint-fix:
    cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged

# unit tests, skipping integration tests that need Docker Kafka
test:
    cargo test --workspace --all-features -- --skip fetch_messages_from_local_orders

# include integration tests (requires `just up`)
test-all:
    cargo test --workspace --all-features

# ---------- docker test cluster ----------

# bring up local Kafka + Kafka-UI (SASL/PLAIN, ACL, seeded sample topics)
up:
    docker compose up -d
    @echo "waiting for kafka-init to seed topics…"
    @docker compose logs --no-color kafka-init 2>/dev/null | tail -5 || true
    @echo "Kafka UI: http://localhost:8080"

# tear down local cluster (volumes removed because docker-compose has no volume)
down:
    docker compose down

# follow kafka-init logs (handy when topics aren't appearing)
logs-init:
    docker compose logs -f kafka-init

# ---------- release ----------

# tag + push (the release workflow auto-builds and publishes)
# usage: just release v0.1.0
release tag=default_tag:
    @if [ -z "{{tag}}" ]; then echo "usage: just release v0.1.0"; exit 1; fi
    git tag {{tag}}
    git push origin {{tag}}
    @echo "→ https://github.com/armitageee/y2kexplorer/actions/workflows/release.yml"

# delete a tag locally and on origin (use with caution)
release-undo tag:
    git tag -d {{tag}} || true
    git push --delete origin {{tag}}

# ---------- maintenance ----------

# refresh Cargo.lock without applying any changes (useful for dependabot conflicts)
lock:
    cargo update --workspace --dry-run

# update direct deps to latest compatible
update:
    cargo update

# audit known security advisories (requires `cargo install cargo-audit`)
audit:
    cargo audit

# project size by language (requires `cargo install tokei` or system tokei)
loc:
    tokei src

# Разработка

Если установлен [`just`](https://github.com/casey/just):

```bash
just                # список задач
just dev            # cargo run --release
just ci             # fmt --check + clippy -D warnings + test
just probe local    # y2k-probe --cluster local
just release v0.1.0 # тег + push (триггерит Release workflow)
```

Без `just`:

```bash
cargo run --release
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features -- --skip fetch_messages_from_local_orders
cargo build --release --locked --bin y2k --bin y2k-probe --all-features
```

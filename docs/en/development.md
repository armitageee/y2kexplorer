# Development

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

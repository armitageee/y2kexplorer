# Local Docker stack

A minimal KRaft Kafka cluster with **SASL/PLAIN**, **ACL** (StandardAuthorizer), **Schema Registry**, and a **Kafka Connect** demo pipeline is included in the repository.

| User | Password | Role |
|---|---|---|
| `admin` | `admin-secret` | super user — ACL admin, all topics |
| `app` | `app-secret` | limited — sample ACL: Read+Describe on `orders` |

```bash
just up                    # or: docker compose up -d
just dev                   # or: cargo run --release -- --cluster local
just down                  # tear down
```

Copy `config.example.toml` → `~/.config/y2kexplorer/config.toml` (include `[clusters.local.schema_registry]` and `[clusters.local.kafka_connect]`). After `docker compose up -d`:

- `schema-init` registers Avro schemas for `orders`, `users.events`, and `payments.retry` (`*-value` subjects).
- `events-generator` appends JSON lines to `docker/connect-data/events.json` every ~2s.
- `connect-init` registers two connectors:
  - **file-source** — reads `/data/events.json` → topic **`connect.events`**
  - **file-sink** — consumes **`connect.events`** → `/data/sink.json` (on the host: `docker/connect-data/sink.json`)

| Service | URL |
|---|---|
| Kafka (SASL/PLAIN) | `localhost:9092` |
| Schema Registry | <http://localhost:8081> |
| Kafka Connect REST | <http://localhost:8083> |
| Kafka UI | <http://localhost:8080> (admin) |

Smoke-test SR: `curl -s http://localhost:8081/subjects`. In the TUI: `6` or `:schemas`.

Smoke-test Connect: `curl -s http://localhost:8083/connectors`. In the TUI: `7` or `:connect`. Watch the pipeline: `tail -f docker/connect-data/sink.json`.

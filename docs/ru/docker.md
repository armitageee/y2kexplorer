# Локальный Docker-стек

KRaft-кластер с **SASL/PLAIN**, **ACL** (StandardAuthorizer), **Schema Registry** и демо-пайплайном **Kafka Connect** включён в репозиторий.

| Пользователь | Пароль | Роль |
|---|---|---|
| `admin` | `admin-secret` | super user — ACL, все топики |
| `app` | `app-secret` | ограниченный — sample ACL: Read+Describe на `orders` |

```bash
just up                    # или: docker compose up -d
just dev                   # или: cargo run --release -- --cluster local
just down                  # снести
```

Скопируйте `config.example.toml` → `~/.config/y2kexplorer/config.toml` (включая `[clusters.local.schema_registry]` и `[clusters.local.kafka_connect]`). После `docker compose up -d`:

- `schema-init` — Avro-схемы для `orders`, `users.events`, `payments.retry` (`*-value`);
- `events-generator` — JSON в `docker/connect-data/events.json` каждые ~2 с;
- `connect-init` — **file-source** (`events.json` → топик `connect.events`) и **file-sink** (`connect.events` → `docker/connect-data/sink.json`).

| Сервис | URL |
|---|---|
| Kafka (SASL/PLAIN) | `localhost:9092` |
| Schema Registry | <http://localhost:8081> |
| Kafka Connect REST | <http://localhost:8083> |
| Kafka UI | <http://localhost:8080> (admin) |

Проверка SR: `curl -s http://localhost:8081/subjects`. В TUI: `6` или `:schemas`.

Проверка Connect: `curl -s http://localhost:8083/connectors`. В TUI: `7` или `:connect`. Пайплайн: `tail -f docker/connect-data/sink.json`.

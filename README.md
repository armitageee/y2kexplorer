# y2kexplorer

Terminal UI для Apache Kafka — по духу близко к [k9s](https://github.com/derailed/k9s), но на Rust и [ratatui](https://docs.rs/ratatui/latest/ratatui/).

## Возможности (MVP)

- Список топиков с фильтром (`/`)
- Просмотр сообщений (tail/head, `b` / `t`)
- Метаданные партиций (`p`)
- Аутентификация: PLAINTEXT, SASL/PLAIN, SCRAM, SSL, **Kerberos (GSSAPI) через keytab**

## Требования для сборки

- Rust 1.75+
- CMake, zlib (для сборки librdkafka через `rdkafka/cmake-build`)
- Для Kerberos: `libsasl2` с поддержкой GSSAPI (`libsasl2-dev` на Debian/Ubuntu)

```bash
# macOS (Homebrew)
brew install cmake openssl

# Debian/Ubuntu
sudo apt install cmake libsasl2-dev libssl-dev pkg-config
```

## Тестовый кластер (Docker)

```bash
docker compose up -d
docker compose logs kafka-init   # дождаться строки "done"
```

Поднимается Kafka на **localhost:9092** (PLAINTEXT, без auth) с топиками:

| Топик | Партиции | Сообщений (примерно) |
|-------|----------|----------------------|
| `orders` | 3 | 5 JSON |
| `users.events` | 3 | 5 JSON |
| `test.notifications` | 1 | 3 JSON |
| `payments.retry` | 2 | 3 JSON |

Конфиг по умолчанию (`config.example.toml`) уже указывает на `localhost:9092`.

```bash
cargo run --release
# в TUI: Enter на orders → сообщения, / — фильтр топиков
```

Остановка: `docker compose down` (данные не сохраняются между пересозданиями volume нет).

## Сборка и запуск

```bash
cargo build --release
cp config.example.toml ~/.config/y2kexplorer/config.toml
# отредактируйте brokers и auth
./target/release/y2k
```

Флаги:

```bash
y2k --config /path/to/config.toml
y2k --cluster prod
```

## Горячие клавиши

| Клавиша | Topics | Messages |
|---------|--------|----------|
| `j` / `k`, `↑` / `↓` | навигация | навигация |
| `/` | фильтр | — |
| `Enter` | открыть сообщения | — |
| `p` | партиции | партиции |
| `r` | обновить | обновить |
| `b` | — | tail (с конца) |
| `t` | — | head (с начала) |
| `Esc` | — | назад |
| `?` | справка | справка |
| `q` | выход | выход |

## Kerberos (keytab)

Настройка соответствует [librdkafka SASL](https://github.com/confluentinc/librdkafka/wiki/Using-SASL-with-librdkafka):

```toml
[clusters.secure.auth]
type = "kerberos"
keytab = "/etc/security/keytabs/kafka-client.keytab"
principal = "kafka-client/host.example.com@REALM"
service_name = "kafka"
tls = true
```

Пользователь процесса должен иметь право читать keytab. При необходимости задайте `KRB5_CONFIG` / `KRB5_CLIENT_KTNAME`.

## Архитектура

```
src/
  main.rs          # CLI, ratatui::run loop
  config/          # TOML, профили кластеров и auth
  kafka/           # rdkafka (admin + consumer)
  app/             # состояние, клавиши, worker threads
  views/           # экраны (stack как в k9s)
  ui/              # таблица, status bar, help
```

Фоновые запросы к Kafka выполняются в отдельных потоках; UI не блокируется на poll.

## Дальнейшее развитие

- Consumer groups, lag, members
- Produce / dry-run сообщения
- Просмотр по offset / partition
- `:command` режим и алиасы (как в k9s)
- Schema Registry, ACL (по необходимости)

## Лицензия

MIT

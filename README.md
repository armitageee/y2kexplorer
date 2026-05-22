# y2kexplorer

Terminal UI для Apache Kafka — по духу близко к [k9s](https://github.com/derailed/k9s), но на Rust и [ratatui](https://docs.rs/ratatui/latest/ratatui/).

## Возможности (MVP)

- Список топиков с фильтром (`/`) и колонкой **MESSAGES** (сумма по партициям)
- Просмотр сообщений (tail/head, `b` / `t`)
- **Отправка сообщений** (`n`) — key + payload
- **Создание / удаление топиков** (`c` / `d`)
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
| `orders` | 3 | 5 JSON (колонка **MESSAGES** в y2k) |
| `users.events` | 3 | 5 JSON |
| `test.notifications` | 1 | 3 JSON |
| `payments.retry` | 2 | 3 JSON |

Конфиг по умолчанию (`config.example.toml`) уже указывает на `localhost:9092`.

```bash
cargo run --release
# в TUI: Enter на orders → сообщения, / — фильтр топиков
```

**Kafka UI:** http://localhost:8080 (кластер `local`, bootstrap `kafka:29092` внутри compose).

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
y2k --cluster secure    # кластер из [clusters.<name>]
```

Конфиг по умолчанию: `~/.config/y2kexplorer/config.toml`.

Проверка при старте: в status bar показывается путь и список кластеров `[local, secure]`.

### Переключение кластера

1. **При запуске:** `y2k --cluster secure`
2. **В config:** `defaults.cluster = "secure"` в `~/.config/y2kexplorer/config.toml`
3. **В TUI (как k9s `:context`):**
   - `:` — командная строка
   - `:context secure` или `:ctx secure` — переключиться и сохранить в config
   - `:context` или `:clusters` — список кластеров (`*имя` = текущий)

## Горячие клавиши

| Клавиша | Topics | Messages |
|---------|--------|----------|
| `j` / `k`, `↑` / `↓` | навигация | навигация |
| `/` | фильтр | — |
| `Enter` | открыть сообщения | — |
| `n` | produce (отправить) | produce |
| `c` | создать топик | — |
| `d` | удалить топик (подтверждение `y`) | — |
| `p` | партиции | партиции |
| `r` | обновить | обновить |
| `b` | — | tail (с конца) |
| `t` | — | head (с начала) |
| `Tab` | в модалке — следующее поле | |
| `:` | command (`context`, `clusters`) | command |
| `Esc` | назад / закрыть модалку | назад |
| `?` | полная справка | полная справка |
| `q` | выход | выход |

Нижняя панель (status + keys) использует контрастную схему (синий фон, яркие подписи клавиш) — хорошо читается на серых темах терминала.

## Kerberos (keytab)

Настройка соответствует [librdkafka SASL](https://github.com/confluentinc/librdkafka/wiki/Using-SASL-with-librdkafka):

```toml
[clusters.secure.auth]
type = "kerberos"
keytab = "/etc/security/keytabs/kafka-client.keytab"
principal = "kafka-client/host.example.com@REALM"
service_name = "kafka"
tls = true
krb5_conf = "/etc/krb5.conf"           # опционально → KRB5_CONFIG
ssl_ca = "/path/to/corporate-ca.pem"   # опционально при tls = true
```

**Аутентификация через keytab** (не macOS login session):

- `sasl.kerberos.keytab` + `sasl.kerberos.principal` + `service_name`
- `kinit` только из keytab: `kinit -t <keytab> -k <principal>` (без `-R` renew из чужого cache)
- отдельный `KRB5CCNAME=FILE:/tmp/y2kexplorer-<pid>.ccache` — не UUID cache из shell
- `KRB5_CLIENT_KTNAME` указывает на тот же keytab

Пользователь процесса должен иметь право **читать keytab**. `principal` должен совпадать с записью в keytab.

### krb5.conf

Опционально **`krb5_conf`** → `KRB5_CONFIG` перед подключением.

```bash
KRB5_CONFIG=/path/to/krb5.conf y2k -c secure
```

Проверка keytab (без ccache):

```bash
export KRB5_CONFIG=/path/to/krb5.conf
klist -kt /path/to/keytab.bin   # есть ли principal@REALM?
```

### Типичные ошибки

| Сообщение | Что проверить |
|-----------|----------------|
| `no cache for <UUID>` | y2k подменяет `KRB5CCNAME` на свой FILE cache в `/tmp` |
| `TICKET NOT RENEWABLE` | principal/keytab vs KDC; realm в **krb5.conf** |
| `BrokerTransportFailure` | VPN, DNS, **`ssl_ca`** при TLS |

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
- Produce: headers, выбор partition
- Просмотр по offset / partition
- `:command` режим и алиасы (как в k9s)
- Schema Registry, ACL (по необходимости)

## Лицензия

MIT

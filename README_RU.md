<p align="center">
  <img src=".assets/y2kagent.png" width="560" alt="y2kexplorer mascot">
</p>

<h1 align="center">y2kexplorer</h1>

<p align="center">
  <em>kafka, but make it ps2</em><br>
  <sub>explore your kafka universe ✦ retro-Kafka TUI на Rust + ratatui</sub>
</p>

<p align="center">
  <a href="README.md">English version</a>
</p>

Клавиатурный дашборд для Apache Kafka — по духу близко к
[k9s](https://github.com/derailed/k9s), но на Rust + [ratatui](https://docs.rs/ratatui),
со скином в эстетике Y2K/PS2: тёмно-синий фон, хром-циан, магента-акценты, двойные рамки.

## Возможности

- **Topics** — список с фильтром (`/`), колонки partitions / replication / messages
- **Messages** — head / tail, лимит, выбор партиции, сортировка по времени, live-follow
- **Produce** — отправка key + payload (`n`)
- **Создание / удаление топиков** (`c` / `d`)
- **Consumer groups** — список, состояние, members, lag по партициям (`g` / `:groups`)
- **Reset offsets** — `earliest` / `latest` / `offset:N` / `timestamp:UNIX_MS` (`R`)
- **Удаление пустых групп** (`d` на Groups)
- **Мульти-кластерный конфиг** — переключение в TUI (`:context <name>`)
- **Аутентификация** — PLAINTEXT, SASL/PLAIN, SCRAM, SSL, **Kerberos (GSSAPI) через keytab**

## Установка

### Готовые бинарники (рекомендуется)

Каждый тег `v*` собирает self-contained тарбол под две платформы.

#### macOS (Apple Silicon, arm64)

```bash
TAG=v0.0.2-rc        # подставить актуальный тег из Releases
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-aarch64-apple-darwin.tar.gz"
tar -xzf "y2kexplorer-${VER}-aarch64-apple-darwin.tar.gz"
cd "y2kexplorer-${VER}-aarch64-apple-darwin"

# снять Gatekeeper-карантин, если архив скачан через браузер
xattr -dr com.apple.quarantine .

./y2k --help
```

Все нужные `.dylib` (`libsasl2`, `libssl`, `libcrypto`, `libkrb5`, `libcurl`, …) лежат в `lib/`
рядом с бинарником и подгружаются через `@executable_path/lib/...` (через `dylibbundler`).
`brew install cyrus-sasl krb5 openssl@3` **не требуется**.

> Если выскочит `library load disallowed by system policy` — значит, в этом релизе CI-подпись
> не доехала до dylib. Подписать ad-hoc локально:
> ```bash
> codesign --force --sign - lib/*.dylib
> codesign --force --sign - y2k y2k-probe
> ```

#### Linux (x86_64)

Собран на Ubuntu 22.04 (glibc 2.35); совместим с **Ubuntu 22.04+**, **Debian 12+**,
**RHEL/Rocky/Alma 9+**, **Fedora 36+**, **openSUSE Leap 15.5+**, Arch.

```bash
# 1. системные библиотеки (один раз)
sudo apt install libsasl2-2 libssl3 libkrb5-3 libcurl4         # Debian/Ubuntu
# или
sudo dnf install cyrus-sasl-lib openssl-libs krb5-libs libcurl  # Fedora/RHEL

# 2. скачать и запустить
TAG=v0.0.2-rc
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
tar -xzf "y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
cd "y2kexplorer-${VER}-x86_64-unknown-linux-gnu"
./y2k --help
```

> Не запустится на Ubuntu 20.04 / Debian 11 / RHEL 8 / Alpine (старая glibc или musl) —
> для таких систем собирайте из исходников.

### Сборка из исходников

Требуется Rust 1.75+, CMake, pkg-config, OpenSSL, Cyrus SASL, MIT Kerberos и libcurl:

```bash
# macOS
brew install cmake pkg-config openssl@3 cyrus-sasl krb5

# Debian/Ubuntu
sudo apt install cmake pkg-config libsasl2-dev libssl-dev libkrb5-dev libcurl4-openssl-dev
```

Затем:

```bash
git clone https://github.com/armitageee/y2kexplorer.git
cd y2kexplorer
cargo build --release --bin y2k --bin y2k-probe --all-features
./target/release/y2k --help
```

Если установлен [`just`](https://github.com/casey/just) — `just build` делает то же самое.

## Конфигурация

Путь по умолчанию: `~/.config/y2kexplorer/config.toml`.

```bash
mkdir -p ~/.config/y2kexplorer
cp config.example.toml ~/.config/y2kexplorer/config.toml
$EDITOR ~/.config/y2kexplorer/config.toml
```

Запуск:

```bash
y2k                            # дефолтный кластер из defaults.cluster
y2k --cluster <name>           # кластер из [clusters.<name>]
y2k --config /path/to.toml     # кастомный путь к конфигу
y2k --theme light              # тема UI: `dark` (по умолчанию) или `light`
y2k-probe --cluster <name>     # smoke-тест подключения без TUI
```

Тему можно зафиксировать в конфиге: `defaults.theme = "light"`.
`light` — для светлого фона терминала; контент-цвета переключаются на тёмные,
а status-bar остаётся ярко-синим в обеих темах.

### Производительность списка топиков

Колонка `MESSAGES` в Topics-view считается через high-low watermark per
partition. На кластерах с большим числом топиков (или высокой broker latency)
это может занимать большую часть времени загрузки. Регулируется двумя опциями:

```toml
[defaults]
# Не считать message_count вовсе — мгновенная загрузка, колонка MESSAGES = 0.
fetch_watermarks = true        # дефолт true
# Сколько потоков параллельно опрашивает watermarks (1..=64).
watermark_parallelism = 16     # дефолт 16
```

Реальные замеры (Kerberos+TLS кластер, 84 топика / 720 партиций):

| Режим | Время |
|---|---|
| sequential (legacy) | ~103 с |
| parallel(16) | ~6.4 с |
| `fetch_watermarks = false` | ~3 с (только metadata) |

Запустить замер на своём кластере: `y2k-probe -c <cluster> --bench-topics`.

### Аутентификация

У каждого кластера своя секция `[clusters.<name>.auth]`:

| `type` | Обязательные поля | Заметки |
|---|---|---|
| `none` | — | PLAINTEXT, без auth |
| `sasl_plain` | `username`, `password`, `tls` | |
| `sasl_scram` | `username`, `password`, `mechanism` (`SCRAM-SHA-256` / `SCRAM-SHA-512`), `tls` | |
| `ssl` | `ca_location`, `certificate_location`, `key_location`, `key_password` | mTLS |
| `kerberos` | `keytab`, `principal`, `service_name`, `tls`, опционально `krb5_conf`, `ssl_ca` | GSSAPI через keytab |

Полные примеры — в [`config.example.toml`](config.example.toml).

## Горячие клавиши

### Глобальные

| Клавиша | Действие |
|---|---|
| `j` / `k`, `↑` / `↓` | навигация |
| `Enter` | открыть выбранное |
| `Esc` | назад / закрыть модалку |
| `r` | обновить текущий вид |
| `:` | команды (`context`, `clusters`, `groups`, `labels`, `label`, `limit`, `poll`, `help`) |
| `1` / `2` / `3` / `4` | sidebar: Topics / Groups / Labels / Contexts |
| `?` | справка |
| `q` | выход |

### Topics

| Клавиша | Действие |
|---|---|
| `Space` | отметить / снять отметку (multi-select, как в k9s) |
| `L` | добавить лейбл к отмеченным (или текущему) топикам |
| `U` | удалить лейбл |
| `D` | сбросить все отметки |
| `/` | текстовый фильтр |
| `Enter` | открыть messages для топика |
| `n` | produce — редактор key + payload |
| `c` | создать топик (с указанием partitions) |
| `d` | удалить топик (подтверждение `y`) |
| `p` | popup с метаданными партиций |
| `g` | Consumer Groups (sidebar `2`) |

### Labels

Локальные теги топиков (в `config.toml`, не в Kafka) — группировка по микросервисам, окружению и т.д.

| Клавиша | Действие |
|---|---|
| `Enter` | Topics с фильтром по лейблу |
| `d` | удалить лейбл со всех топиков кластера (подтверждение `y`) |
| `/` | фильтр списка лейблов |
| `1` / `2` / `3` / `4` | навигация в sidebar |

```toml
[topic_labels.lt01]
"orders" = ["order-service", "prod"]
```

Команды: `:labels`, `:label billing`, `:label-delete billing` (удалить везде).

### Contexts

Список кластеров из `config.toml` и переключение между ними.

| Клавиша | Действие |
|---|---|
| `Enter` | переключиться на кластер (переподключение + Topics) |
| `/` | фильтр списка |
| `4` | экран Contexts (sidebar) |

Команды: `:contexts`, `:context <имя>`.

### Messages

| Клавиша | Действие |
|---|---|
| `b` / `t` | tail (с конца) / head (с начала) |
| `p` | цикл партиции (all → 0 → 1 → …) |
| `i` | popup с метаданными партиций |
| `s` | сортировка по времени ↔ по offset |
| `+` / `-` | лимит ±50 (10–10000) |
| `l` | ввод точного лимита |
| `f` | live follow — periodic poll новых сообщений |
| `[` / `]` | интервал live-poll ±1 с (1–30) |
| `o` | pretty-print JSON on/off |
| `y` | копировать выбранное сообщение в буфер |
| `u` / `d` | прокрутка детали |
| `PgUp` / `PgDn` | прокрутка детали быстрее |
| `n` | produce |

### Consumer groups

| Клавиша | Действие |
|---|---|
| `/` | фильтр по id |
| `Enter` | детали группы (offsets / lag) |
| `R` | reset offsets |
| `d` | удалить группу (только когда state ∈ {`Empty`, `Dead`}) |

`R` открывает модалку с полем **spec**:

| Spec | Что делает |
|---|---|
| `earliest` | сдвиг на low watermark всех партиций |
| `latest` | сдвиг на high watermark (LEO) |
| `offset:N` | абсолютный N (клампится в `[low, high]` по каждой партиции) |
| `timestamp:UNIX_MS` | первый offset с `timestamp >= UNIX_MS` (через `offsets_for_times`) |

> **Важно:** reset работает **только когда у группы нет активных consumer-ов**
> (state ∈ {`Empty`, `Dead`}). Иначе брокер вернёт `REBALANCE_IN_PROGRESS`.
> y2kexplorer проверяет state до коммита и возвращает понятное сообщение, если группа активна.

## Локальный запуск через Docker

В репо лежит docker-compose с локальным Kafka (PLAINTEXT, без auth) и сидингом тестовых топиков:

```bash
just up                    # или: docker compose up -d
just dev                   # или: cargo run --release
just down                  # снести
```

Kafka UI доступен на <http://localhost:8080> для cross-check.

## Разработка

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

## Лицензия

MIT

# y2kexplorer

Terminal UI для Apache Kafka — по духу близко к [k9s](https://github.com/derailed/k9s), но на Rust и [ratatui](https://docs.rs/ratatui/latest/ratatui/).

## Возможности (MVP)

- Список топиков с фильтром (`/`) и колонкой **MESSAGES** (сумма по партициям)
- Просмотр сообщений (tail/head, лимит, партиция, сортировка по времени)
- **Отправка сообщений** (`n`) — key + payload
- **Создание / удаление топиков** (`c` / `d`)
- Метаданные партиций (Topics: `p`, Messages: `i`)
- **Consumer groups** (`g` / `:groups`): список, состояние, members, lag по партициям
- **Reset offsets** (`R`): `earliest` / `latest` / `offset:N` / `timestamp:UNIX_MS`
- **Удаление пустых групп** (`d` на Groups)
- Аутентификация: PLAINTEXT, SASL/PLAIN, SCRAM, SSL, **Kerberos (GSSAPI) через keytab**

## Установка

### Готовые бинарники из GitHub Releases

Каждый тег `v*` собирает self-contained тарбол под две платформы.

#### macOS (Apple Silicon, arm64)

```bash
TAG=v0.0.2-rc        # подставить актуальный
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-aarch64-apple-darwin.tar.gz"
tar -xzf "y2kexplorer-${VER}-aarch64-apple-darwin.tar.gz"
cd "y2kexplorer-${VER}-aarch64-apple-darwin"

# снять карантин Gatekeeper, если архив скачан через браузер
xattr -dr com.apple.quarantine .

./y2k --help
```

Все нужные `.dylib` (`libsasl2`, `libssl`, `libcrypto`, `libkrb5`, `libcurl`…) лежат в `lib/` рядом с бинарником и подгружаются через `@executable_path/lib/...`. `brew install cyrus-sasl krb5 openssl@3` **не требуется**.

> Если выскочит `library load disallowed by system policy` — значит, в этом релизе CI-подпись не доехала до dylib. Подписать ad-hoc вручную:
> ```bash
> codesign --force --sign - lib/*.dylib
> codesign --force --sign - y2k y2k-probe
> ```

#### Linux (x86_64)

Собран на Ubuntu 22.04 (glibc 2.35), совместим с **Ubuntu 22.04+**, **Debian 12+**, **RHEL/Rocky/Alma 9+**, **Fedora 36+**, **openSUSE Leap 15.5+**, Arch.

```bash
# 1. Системные зависимости (один раз)
sudo apt install libsasl2-2 libssl3 libkrb5-3 libcurl4   # Debian/Ubuntu
# или
sudo dnf install cyrus-sasl-lib openssl-libs krb5-libs libcurl  # Fedora/RHEL

# 2. Скачать и запустить
TAG=v0.0.2-rc
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
tar -xzf "y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
cd "y2kexplorer-${VER}-x86_64-unknown-linux-gnu"
./y2k --help
```

> Не работает на Ubuntu 20.04 / Debian 11 / RHEL 8 / Alpine (старая glibc или musl). Для них собирайте локально из исходников.

### Конфигурация после установки

```bash
mkdir -p ~/.config/y2kexplorer
cp config.example.toml ~/.config/y2kexplorer/config.toml
$EDITOR ~/.config/y2kexplorer/config.toml
./y2k                            # дефолтный кластер
./y2k --cluster <name>           # из [clusters.<name>]
./y2k-probe --cluster <name>     # smoke-тест подключения без TUI
```

## Требования для сборки из исходников

- Rust 1.75+
- CMake, zlib (для сборки librdkafka через `rdkafka/cmake-build`)
- Для Kerberos: `libsasl2` с поддержкой GSSAPI (`libsasl2-dev` на Debian/Ubuntu)

```bash
# macOS (Homebrew)
brew install cmake openssl@3 cyrus-sasl krb5

# Debian/Ubuntu
sudo apt install cmake pkg-config libsasl2-dev libssl-dev libkrb5-dev libcurl4-openssl-dev
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

## CI/CD (GitHub Actions)

| Workflow | Триггер | Что делает |
|----------|---------|------------|
| [CI](.github/workflows/ci.yml) | push/PR в `main`, `master`, `develop` | `fmt`, `clippy`, `test`, сборка на 5 платформах |
| [Release](.github/workflows/release.yml) | тег `v*` (например `v0.1.0`) | release-артефакты в GitHub Releases |

**Платформы сборки:** `linux-x86_64`, `macos-arm64` (полный функционал: SASL + SSL + Kerberos/GSSAPI).

- **macOS (arm64):** артефакт self-contained — все нужные `.dylib` (`libsasl2`, `libssl`, `libcrypto`, `libkrb5`, `libcurl`…) кладутся рядом с бинарником в `lib/` и подменяются на `@executable_path/lib/...` через `dylibbundler`. Запуск возможен без brew.
- **Linux (x86_64):** сборка на `ubuntu-22.04`, чтобы glibc/`libssl3`/`libsasl2-2` были совместимы с большинством современных дистрибутивов (Ubuntu 22.04+, Debian 12+, RHEL 9+, Fedora 36+). Системные пакеты должны быть установлены: `apt install libsasl2-2 libssl3 libkrb5-3 libcurl4`.

> `linux-aarch64` и `windows-x86_64` отключены: librdkafka 2.12 + Cyrus SASL на этих платформах требуют либо ARM-runner / [`cross-rs`](https://github.com/cross-rs/cross), либо `vendored` сборки `sasl2-sys` на MSVC. Для Windows используйте WSL.

> Инструкция по установке готовых бинарников и снятию карантина macOS — см. секцию [Установка](#установка) выше.

Локально как в CI (Linux):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --release --locked --bin y2k --bin y2k-probe
```

Интеграционный тест Kafka (`fetch_messages_from_local_orders`) помечен `#[ignore]` — нужен `docker compose up`.

Релиз: `git tag v0.1.0 && git push origin v0.1.0`.

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
| `p` | партиции | цикл партиции (all → p0 → …) |
| `i` | — | метаданные партиций |
| `r` | обновить | обновить |
| `b` | — | tail (с конца) |
| `t` | — | head (с начала) |
| `+` / `-` | — | лимит ±50 (10–10000, в config) |
| `l` | — | ввод лимита |
| `s` | — | сортировка time / offset |
| `f` | — | live follow (poll новых сообщений) |
| `o` | — | pretty JSON on/off |
| `y` | — | копировать сообщение в буфер |
| `u` / `d` | — | прокрутка detail |
| PgUp/PgDn | — | прокрутка detail быстрее |
| `[` / `]` | — | интервал live-poll ±1 с (1–30, в config) |
| `:limit N` | — | лимит сообщений |
| `:poll N` | — | интервал live-poll (секунды) |
| `g` | открыть Consumer Groups | — |
| `Tab` | в модалке — следующее поле | |
| `:` | command (`context`, `clusters`, `groups`) | command |
| `Esc` | назад / закрыть модалку | назад |
| `?` | полная справка | полная справка |
| `q` | выход | выход |

### Consumer groups

| Клавиша | Groups | Group details |
|---------|--------|---------------|
| `j` / `k`, `↑` / `↓` | навигация | навигация |
| `/` | фильтр по id | — |
| `Enter` | детали (offsets/lag) | — |
| `R` | reset offsets | reset offsets |
| `d` | удалить группу (только Empty/Dead) | — |
| `r` | обновить | обновить |
| `Esc` | назад | назад |

`R` открывает модалку с полем **spec**. Принимает:

| spec | Что делает |
|------|------------|
| `earliest` | сдвинуть на low watermark всех партиций |
| `latest` | сдвинуть на high watermark (LEO) |
| `offset:N` | абсолютный N (клампится в `[low, high]` каждой партиции) |
| `timestamp:UNIX_MS` | первый offset с `timestamp >= UNIX_MS` (через `offsets_for_times`) |

> **Важно:** Reset работает **только когда у группы нет активных consumer-ов** (state ∈ {`Empty`, `Dead`}). Иначе брокер вернёт `REBALANCE_IN_PROGRESS`. Перед reset остановите всех потребителей этой группы. y2kexplorer проверяет state до коммита и возвращает понятное сообщение, если группа активна.

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
| `PartitionEOF` в status | Не ошибка: конец партиции при live-poll; в новых версиях подавлено (`enable.partition.eof=false`) |

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

- Produce: headers, выбор partition
- Просмотр сообщений по offset (jump to offset)
- Members per consumer group + assignment (rebalance view)
- Schema Registry, ACL (по необходимости)

## Лицензия

MIT

# Конфигурация

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
y2k --theme mono              # тема UI (см. [themes.md](themes.md))
y2k-probe --cluster <name>     # smoke-тест подключения без TUI
```

## Производительность списка топиков

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

## Аутентификация

У каждого кластера своя секция `[clusters.<name>.auth]`:

| `type` | Обязательные поля | Заметки |
|---|---|---|
| `none` | — | PLAINTEXT, без auth |
| `sasl_plain` | `username`, `password`, `tls` | |
| `sasl_scram` | `username`, `password`, `mechanism` (`SCRAM-SHA-256` / `SCRAM-SHA-512`), `tls` | |
| `ssl` | `ca_location`, `certificate_location`, `key_location`, `key_password` | mTLS |
| `kerberos` | `keytab`, `principal`, `service_name`, `tls`, опционально `krb5_conf`, `ssl_ca` | GSSAPI через keytab |

Полные примеры — в [`config.example.toml`](../../config.example.toml).

## Schema Registry и Kafka Connect

Confluent Schema Registry и Kafka Connect — отдельные HTTP-сервисы (не Kafka API).
Для кластера нужны секции `[clusters.<name>.schema_registry]` и `[clusters.<name>.kafka_connect]` с `url` —
см. [`config.example.toml`](../../config.example.toml).

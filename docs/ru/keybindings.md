# Горячие клавиши

## Глобальные

| Клавиша | Действие |
|---|---|
| `j` / `k`, `↑` / `↓` | навигация |
| `Enter` | открыть выбранное |
| `Esc` | назад / закрыть модалку |
| `r` | обновить текущий вид |
| `:` | команды (`context`, `clusters`, `groups`, `labels`, `acls`, `schemas`, `connect`, `label`, `limit`, `poll`, `help`) |
| `1` / `2` / `3` / `4` / `5` / `6` / `7` | sidebar: Topics / Groups / Labels / Contexts / ACLs / Schemas / Connect |
| `?` | краткая / полная справка по клавишам |
| `T` | смена темы (`midnight` → `cream` → `mono` → `latte`) |
| `q` | выход |

## Topics

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

## Labels

Локальные теги топиков (в `config.toml`, не в Kafka) — группировка по микросервисам, окружению и т.д.

| Клавиша | Действие |
|---|---|
| `Enter` | Topics с фильтром по лейблу |
| `d` | удалить лейбл со всех топиков кластера (подтверждение `y`) |
| `/` | фильтр списка лейблов |
| `1` / `2` / `3` / `4` / `5` / `6` | навигация в sidebar |

```toml
[topic_labels.lt01]
"orders" = ["order-service", "prod"]
```

Команды: `:labels`, `:label billing`, `:label-delete billing` (удалить везде).

## Contexts

Список кластеров из `config.toml` и переключение между ними.

| Клавиша | Действие |
|---|---|
| `Enter` | переключиться на кластер (переподключение + Topics) |
| `/` | фильтр списка |
| `4` | экран Contexts (sidebar) |

Команды: `:contexts`, `:context <имя>`.

## ACLs

Просмотр и управление ACL Kafka (нужен включённый authorizer и права администратора у вашего principal).

| Клавиша | Действие |
|---|---|
| `/` | фильтр списка |
| `c` | создать ACL |
| `e` | изменить выбранный ACL (удаление старой записи + создание новой) |
| `d` | удалить (подтверждение `y`) |
| `r` | обновить |
| `5` | экран ACLs (sidebar) |

Команда: `:acls`

Типы ресурсов: `topic`, `group`, `broker`, `cluster` (имя `kafka-cluster`), `transactional_id`. Pattern: `literal`, `prefixed`, `match`. Permission: `allow` / `deny`.

## Schemas (Schema Registry)

Отдельный HTTP-сервис Confluent Schema Registry — не через Kafka API. Для кластера нужна секция `[clusters.<name>.schema_registry]` с `url` (см. [`config.example.toml`](../../config.example.toml)).

| Клавиша | Действие |
|---|---|
| `/` | фильтр по subject |
| `Enter` | детали схемы (последняя версия) |
| `j` / `k` | переключение версии в деталях |
| `u` / `d` | прокрутка JSON схемы |
| `r` | обновить |
| `6` | экран Schemas (sidebar) |

Команды: `:schemas`, `:schema <subject>`, `:sr <subject>`

## Connect (Kafka Connect)

Отдельный REST API Kafka Connect. Для кластера — секция `[clusters.<name>.kafka_connect]` с `url` (см. [`config.example.toml`](../../config.example.toml)).

| Клавиша | Действие |
|---|---|
| `/` | фильтр коннекторов |
| `Enter` | детали (статус, tasks, JSON конфига) |
| `P` / `O` | pause / resume |
| `R` | restart |
| `d` | удалить (подтверждение `y`) |
| `u` / `PageUp` / `PageDown` | прокрутка конфига |
| `r` | обновить |
| `7` | экран Connect (sidebar) |

Команды: `:connect`, `:connectors`

## Messages

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

## Consumer groups

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

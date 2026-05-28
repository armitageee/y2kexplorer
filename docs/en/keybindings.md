# Keybindings

## Global

| Key | Action |
|---|---|
| `j` / `k`, `↑` / `↓` | navigate |
| `Enter` | open selection |
| `Esc` | back / close modal |
| `r` | refresh current view |
| `:` | command palette (`context`, `clusters`, `groups`, `labels`, `acls`, `schemas`, `connect`, `label`, `limit`, `poll`, `help`) |
| `1` / `2` / `3` / `4` / `5` / `6` / `7` | sidebar: Topics / Groups / Labels / Contexts / ACLs / Schemas / Connect |
| `?` | toggle short / full keybinding help |
| `T` | cycle UI theme (`midnight` → `cream` → `mono` → `latte`) |
| `q` | quit |

## Topics

| Key | Action |
|---|---|
| `Space` | mark / unmark topic (k9s-style multi-select) |
| `L` | add label to marked (or current) topic(s) |
| `U` | remove label from marked (or current) topic(s) |
| `D` | clear all marks |
| `/` | text filter |
| `Enter` | open messages for selected topic |
| `n` | produce — open key + payload editor |
| `c` | create topic (with partitions) |
| `d` | delete topic (confirm with `y`) |
| `p` | partition metadata popup |
| `g` | Consumer Groups (sidebar `2`) |

## Labels

Local tags per topic (stored in `config.toml`, not on the broker). Use them to group topics by microservice, env, team, etc.

| Key | Action |
|---|---|
| `Enter` | open Topics filtered by this label |
| `d` | delete label from all topics in cluster (confirm `y`) |
| `/` | filter label list |
| `1` / `2` / `3` / `4` / `5` / `6` | sidebar navigation |

Config example:

```toml
[topic_labels.lt01]
"orders" = ["order-service", "prod"]
```

Commands: `:labels`, `:label billing` (filter topics), `:label-delete billing` (remove everywhere).

## Contexts

Browse and switch Kafka clusters defined in `config.toml`.

| Key | Action |
|---|---|
| `Enter` | switch to selected cluster (reconnect + Topics) |
| `/` | filter context list |
| `4` | open Contexts from anywhere (sidebar) |

Commands: `:contexts`, `:context <name>` (quick switch without the menu).

## ACLs

List and manage Kafka ACLs (requires a cluster with the standard authorizer enabled and admin rights on your principal).

| Key | Action |
|---|---|
| `/` | filter ACL list |
| `c` | create ACL (form: resource type/name, pattern, principal, host, operation, permission) |
| `e` | edit selected ACL (delete old binding + create new — Kafka has no in-place update) |
| `d` | delete selected ACL (confirm `y`) |
| `r` | refresh |
| `5` | open ACLs from anywhere (sidebar) |

Commands: `:acls`

Resource types in the form: `topic`, `group`, `broker`, `cluster` (cluster ACL uses resource name `kafka-cluster`), `transactional_id`. Pattern: `literal`, `prefixed`, `match`. Permission: `allow` / `deny`.

## Schemas (Schema Registry)

Confluent Schema Registry is a separate HTTP service (not the Kafka protocol). Per cluster, add `[clusters.<name>.schema_registry]` with `url` — see [`config.example.toml`](../../config.example.toml).

| Key | Action |
|---|---|
| `/` | filter subjects |
| `Enter` | open schema detail (latest version) |
| `j` / `k` | cycle versions in detail view |
| `u` / `d` | scroll schema JSON |
| `r` | refresh |
| `6` | Schemas screen (sidebar) |

Commands: `:schemas`, `:schema <subject>`, `:sr <subject>`

## Connect (Kafka Connect)

Kafka Connect is a separate REST API (`GET /connectors`, etc.). Per cluster, add `[clusters.<name>.kafka_connect]` with `url` — see [`config.example.toml`](../../config.example.toml).

| Key | Action |
|---|---|
| `/` | filter connectors |
| `Enter` | open connector detail (status, tasks, config JSON) |
| `P` / `O` | pause / resume connector |
| `R` | restart connector |
| `d` | delete connector (confirm `y`) |
| `u` / `PageUp` / `PageDown` | scroll config in detail view |
| `r` | refresh |
| `7` | Connect screen (sidebar) |

Commands: `:connect`, `:connectors`

## Messages

| Key | Action |
|---|---|
| `b` / `t` | tail (from end) / head (from start) |
| `p` | cycle partition (all → 0 → 1 → …) |
| `i` | partition metadata popup |
| `s` | toggle time-sort vs offset-sort |
| `+` / `-` | change message limit ±50 (10–10000) |
| `l` | enter exact message limit |
| `f` | live follow — poll new messages periodically |
| `[` / `]` | live-poll interval ±1s (1–30) |
| `o` | toggle pretty-print JSON |
| `y` | yank selected message to clipboard |
| `u` / `d` | scroll detail pane |
| `PgUp` / `PgDn` | scroll detail pane fast |
| `n` | produce |

## Consumer groups

| Key | Action |
|---|---|
| `/` | filter by id |
| `Enter` | group details (offsets / lag) |
| `R` | reset offsets |
| `d` | delete group (only when state is `Empty` / `Dead`) |

`R` opens a modal with a single **spec** field. Accepted values:

| Spec | Effect |
|---|---|
| `earliest` | move to low watermark of every partition |
| `latest` | move to high watermark (LEO) |
| `offset:N` | absolute N (clamped to `[low, high]` per partition) |
| `timestamp:UNIX_MS` | first offset with `timestamp >= UNIX_MS` (via `offsets_for_times`) |

> **Note:** offset reset only works when the group has **no active members**
> (state ∈ {`Empty`, `Dead`}). Otherwise the broker returns `REBALANCE_IN_PROGRESS`.
> y2kexplorer pre-checks the group state and surfaces a clear error if the group is live.

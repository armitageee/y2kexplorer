# UI themes

Four color palettes tuned for **dark** or **light** terminal backgrounds. Pick one at
startup (`--theme` or `defaults.theme` in config) or cycle in the TUI with **`T`**
(the current theme name appears in the status bar).

| Name | Terminal background | Character |
|---|---|---|
| `midnight` (default) | dark | magenta / blue accents — closest to eilmeldung defaults |
| `cream` | light | warm amber / brown accents |
| `mono` | light | monochrome grays — high contrast on a white background, no bright colors |
| `latte` | light | [Catppuccin Latte](https://catppuccin.com/palette/) — muted pastels |

**Aliases** (backward compatible): `dark` → `midnight`, `light` → `mono`, `paper` → `mono`, `slate` → `midnight`.

```bash
y2k --theme mono
y2k --theme latte
```

```toml
[defaults]
theme = "midnight"   # midnight | cream | mono | latte
```

**Keybindings footer:** hints wrap to multiple lines when the terminal is narrow; if
they still do not fit, a `… +N` suffix shows how many entries are hidden — press **`?`**
for the full list on up to four lines (on taller terminals).

## Установка

### macOS (Apple Silicon, arm64)

```bash
# 1. Скачать и распаковать архив
curl -LO https://github.com/armitageee/y2kexplorer/releases/download/__TAG__/y2kexplorer-__VERSION__-aarch64-apple-darwin.tar.gz
tar -xzf y2kexplorer-__VERSION__-aarch64-apple-darwin.tar.gz
cd y2kexplorer-__VERSION__-aarch64-apple-darwin

# 2. Снять карантин Gatekeeper, если архив скачан через браузер
xattr -dr com.apple.quarantine .

# 3. Запустить
./y2k --help
```

> Если после `./y2k` всё-таки выскакивает `library load disallowed by system policy` — подпишите ad-hoc вручную:
> ```bash
> codesign --force --sign - lib/*.dylib
> codesign --force --sign - y2k y2k-probe
> ```
> Это означает, что в этом релизе CI-подпись по какой-то причине не доехала.

Все нужные `.dylib` (`libsasl2`, `libssl`, `libcrypto`, `libkrb5`, `libcurl`…) лежат в `lib/` рядом с бинарником — устанавливать `brew install cyrus-sasl krb5 openssl@3` **не требуется**.

### Linux (x86_64)

Бинарник собран на Ubuntu 22.04 (glibc 2.35) и совместим с большинством современных дистрибутивов: **Ubuntu 22.04+**, **Debian 12+**, **RHEL/Rocky/Alma 9+**, **Fedora 36+**, **openSUSE Leap 15.5+**, Arch (rolling).

```bash
# 1. Поставить системные зависимости (один раз)
# Debian/Ubuntu:
sudo apt install libsasl2-2 libssl3 libkrb5-3 libcurl4
# Fedora/RHEL:
sudo dnf install cyrus-sasl-lib openssl-libs krb5-libs libcurl

# 2. Скачать и распаковать
curl -LO https://github.com/armitageee/y2kexplorer/releases/download/__TAG__/y2kexplorer-__VERSION__-x86_64-unknown-linux-gnu.tar.gz
tar -xzf y2kexplorer-__VERSION__-x86_64-unknown-linux-gnu.tar.gz
cd y2kexplorer-__VERSION__-x86_64-unknown-linux-gnu

# 3. Запустить
./y2k --help
```

> Не работает на Ubuntu 20.04 / Debian 11 / RHEL 8 / Alpine — там старая glibc или musl. Для них можно собрать локально из исходников, см. README.

### Конфигурация

После установки скопируйте пример конфига и подставьте свои `brokers`/`auth`:

```bash
mkdir -p ~/.config/y2kexplorer
cp config.example.toml ~/.config/y2kexplorer/config.toml
$EDITOR ~/.config/y2kexplorer/config.toml
./y2k                            # стартует с дефолтным кластером
./y2k --cluster <name>           # выбрать кластер из [clusters.<name>]
./y2k-probe --cluster <name>     # smoke-тест подключения без TUI
```

Подробности по конфигу, Kerberos-аутентификации, hotkeys — в [README](https://github.com/armitageee/y2kexplorer#readme).

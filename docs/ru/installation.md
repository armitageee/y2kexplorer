# Установка

## Готовые бинарники (рекомендуется)

Каждый тег `v*` собирает self-contained тарбол под две платформы.

### macOS (Apple Silicon, arm64)

```bash
TAG=v0.0.7-rc.1    # подставить актуальный тег из Releases
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

### Linux (x86_64)

Собран на Ubuntu 22.04 (glibc 2.35); совместим с **Ubuntu 22.04+**, **Debian 12+**,
**RHEL/Rocky/Alma 9+**, **Fedora 36+**, **openSUSE Leap 15.5+**, Arch.

```bash
# 1. системные библиотеки (один раз)
sudo apt install libsasl2-2 libssl3 libkrb5-3 libcurl4         # Debian/Ubuntu
# или
sudo dnf install cyrus-sasl-lib openssl-libs krb5-libs libcurl  # Fedora/RHEL

# 2. скачать и запустить
TAG=v0.0.7-rc.1
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
tar -xzf "y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
cd "y2kexplorer-${VER}-x86_64-unknown-linux-gnu"
./y2k --help
```

> Не запустится на Ubuntu 20.04 / Debian 11 / RHEL 8 / Alpine (старая glibc или musl) —
> для таких систем собирайте из исходников.

## Сборка из исходников

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

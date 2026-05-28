# Installation

## Pre-built binaries (recommended)

Each `v*` tag publishes self-contained tarballs for two platforms.

### macOS (Apple Silicon, arm64)

```bash
TAG=v0.0.7-rc.1    # use the latest tag from Releases
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-aarch64-apple-darwin.tar.gz"
tar -xzf "y2kexplorer-${VER}-aarch64-apple-darwin.tar.gz"
cd "y2kexplorer-${VER}-aarch64-apple-darwin"

# strip Gatekeeper quarantine if downloaded via browser
xattr -dr com.apple.quarantine .

./y2k --help
```

All required `.dylib`s (`libsasl2`, `libssl`, `libcrypto`, `libkrb5`, `libcurl`, …) are bundled
into `lib/` next to the binary and rewritten to `@executable_path/lib/...` via `dylibbundler`.
You **don't need** `brew install cyrus-sasl krb5 openssl@3`.

> If you still see `library load disallowed by system policy`, the CI signing step didn't
> reach the dylibs in this release — re-sign locally:
> ```bash
> codesign --force --sign - lib/*.dylib
> codesign --force --sign - y2k y2k-probe
> ```

### Linux (x86_64)

Built on Ubuntu 22.04 (glibc 2.35); compatible with **Ubuntu 22.04+**, **Debian 12+**,
**RHEL/Rocky/Alma 9+**, **Fedora 36+**, **openSUSE Leap 15.5+**, Arch.

```bash
# 1. system libraries (once)
sudo apt install libsasl2-2 libssl3 libkrb5-3 libcurl4         # Debian/Ubuntu
# or
sudo dnf install cyrus-sasl-lib openssl-libs krb5-libs libcurl  # Fedora/RHEL

# 2. download & run
TAG=v0.0.7-rc.1
VER=${TAG#v}
curl -LO "https://github.com/armitageee/y2kexplorer/releases/download/${TAG}/y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
tar -xzf "y2kexplorer-${VER}-x86_64-unknown-linux-gnu.tar.gz"
cd "y2kexplorer-${VER}-x86_64-unknown-linux-gnu"
./y2k --help
```

> Won't run on Ubuntu 20.04 / Debian 11 / RHEL 8 / Alpine (older glibc or musl) —
> build from source instead.

## Build from source

Requires Rust 1.75+, CMake, pkg-config, OpenSSL, Cyrus SASL, MIT Kerberos and libcurl:

```bash
# macOS
brew install cmake pkg-config openssl@3 cyrus-sasl krb5

# Debian/Ubuntu
sudo apt install cmake pkg-config libsasl2-dev libssl-dev libkrb5-dev libcurl4-openssl-dev
```

Then:

```bash
git clone https://github.com/armitageee/y2kexplorer.git
cd y2kexplorer
cargo build --release --bin y2k --bin y2k-probe --all-features
./target/release/y2k --help
```

If you have [`just`](https://github.com/casey/just), run `just build` instead.

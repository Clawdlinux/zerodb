# AtticDB

[![Release](https://img.shields.io/github/v/release/Clawdlinux/atticdb)](https://github.com/Clawdlinux/atticdb/releases/latest)
[![Downloads](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2FClawdlinux%2Fatticdb%2Fbadges%2Fdownloads.json)](https://github.com/Clawdlinux/atticdb/releases)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![Platforms](https://img.shields.io/badge/platforms-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)](#where-to-run-it)

AtticDB is local, durable memory for coding agents, written in Rust.
It stores turn history per session and keyed facts. A write is acked only after it is synced to disk.
It wires into Copilot CLI, VS Code, Claude Code and Codex with 1 command.

Status: early. Don't run it in production. Built so far: the tape, session engine, disk-full handling, server and facts (S1 to S4).

## Install

On macOS and Linux:

```sh
curl -fsSL https://github.com/Clawdlinux/atticdb/releases/latest/download/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"   # needed only if the installer said it isn't on PATH
```

The installer puts `atticdb` in `~/.local/bin` and checks the download against the release's `SHA256SUMS`. It never uses sudo or edits your shell files.
Releases ship monthly, and the latest is the default.
Set `ATTICDB_VERSION=0.1.0` to pin a release, `ATTICDB_INSTALL_DIR` to install elsewhere, or `ATTICDB_DOWNLOAD_BASE` for a mirror.

On Windows x86_64, download `atticdb-x86_64-pc-windows-msvc.zip` and `SHA256SUMS` from the [latest release](https://github.com/Clawdlinux/atticdb/releases/latest).
Check the zip, then put `atticdb.exe` in a folder on your PATH. In PowerShell:

```powershell
(Get-FileHash .\atticdb-x86_64-pc-windows-msvc.zip -Algorithm SHA256).Hash.ToLower()   # must match SHA256SUMS
Expand-Archive .\atticdb-x86_64-pc-windows-msvc.zip "$env:LOCALAPPDATA\Programs\atticdb"
$env:PATH += ";$env:LOCALAPPDATA\Programs\atticdb"   # this session only. Add it to your user PATH too
atticdb setup
```

`install.sh` doesn't run on Windows.

From source, with Rust installed:

```sh
cargo install --git https://github.com/Clawdlinux/atticdb atticdb --locked   # puts atticdb in ~/.cargo/bin
```

The downloads badge counts release archive downloads. Our own install tests are left out. A daily workflow updates it.

## Quick start

```sh
atticdb setup --dry-run   # shows what it would change, writes nothing
atticdb setup             # asks which apps, then wires them
atticdb doctor            # checks the install; it may start the server
```

`setup` finds Copilot CLI, VS Code, Claude Code and Codex. It adds the MCP entry, a chat-start hook and the guide each app reads. Every edited file is backed up first. `atticdb disconnect <app>` removes exactly what it added. Codex runs a new hook only after you approve it once in Codex. [research/setup_eval/results](research/setup_eval/results/README.md) has the eval results.

`atticdb doctor` prints 1 line per check, with what to do when one fails. [Set up](crates/atticdb/README.md#set-up) and [`atticdb doctor`](crates/atticdb/README.md#atticdb-doctor) give the details.

## Where to run it

macOS, Linux and Windows x86_64. Release binaries cover macOS and Linux on x86_64 and arm64, and Windows on x86_64. The Linux ones are static, so any distro works. Then `atticdb setup` connects it to VS Code, Copilot CLI, Claude Code and Codex.
On Windows, Codex gets only its MCP entry for now ([#142](https://github.com/Clawdlinux/atticdb/issues/142)). On Windows, Claude Code 2.1.139 or later is needed for the hook. Durability there is weaker, as Windows has no directory fsync. [Windows](crates/atticdb/README.md#windows) lists what differs.

Keep the data directory on a local disk. Every ack waits for a sync, so that disk sets your write latency.
[Where to keep the data directory](crates/atticdb/README.md#where-to-keep-the-data-directory) lists the disks we tested and the ones to avoid.

## Tell us what you need

AtticDB does a few things on purpose. We add features when people ask for them.

- Missing an app, a command or an API? [Open a feature request](https://github.com/Clawdlinux/atticdb/issues/new?template=feature.yml). Say what you're trying to do, not just the feature.
- Something broke? [Open a bug](https://github.com/Clawdlinux/atticdb/issues/new?template=bug.yml) with the `atticdb doctor` output.
- Someone asked already? Add a thumbs-up to that issue. We count those. Issues labeled `waiting-for-users` are ideas we parked until enough people want them.

## Docs

- [atticdb](crates/atticdb/README.md): server, HTTP API, MCP, studio, `stats`, `load`, `verify`, `dump` and `recover`. It also has these:
  - [Run a server by hand](crates/atticdb/README.md#run-a-server-by-hand), [Keys](crates/atticdb/README.md#keys) and [Where to keep the data directory](crates/atticdb/README.md#where-to-keep-the-data-directory).
  - [MCP](crates/atticdb/README.md#atticdb-mcp), [Secrets](crates/atticdb/README.md#secrets), [Shell verbs](crates/atticdb/README.md#shell-verbs) and [Connect](crates/atticdb/README.md#atticdb-connect-and-atticdb-disconnect).
  - [Studio](crates/atticdb/README.md#atticdb-studio) and [Windows](crates/atticdb/README.md#windows).
- [atticdb-core](crates/atticdb-core/README.md) and [atticdb-tape](crates/atticdb-tape/README.md): engine, formats, recovery and limits.
- [Overview](docs/overview.md): how it works, benchmark numbers and the build steps.
- [Sandboxed agents](docs/deploy/sandboxes.md): who holds memory, and how sandboxed code can reach it.
- [Python client](clients/python/README.md), [TESTING.md](TESTING.md) and the [decision log](docs/design/0002-decision-log.md).
- Benchmarks: [head-to-head tables](bench/H2H.md) and [macOS engine runs](bench/RESULTS.md). History: [0001 (historical)](docs/design/0001-architecture.md).

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before you open a pull request. It has the build and test commands.
Follow the [code of conduct](CODE_OF_CONDUCT.md), and report vulnerabilities as [SECURITY.md](SECURITY.md) says.

## License

License: MIT OR Apache-2.0, at your option. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

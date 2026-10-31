# Contributing to AtticDB

Thanks for helping. This file says how to propose a change and get it merged.

## Before you code

- AtticDB builds what users ask for. Before you start a feature, open an issue for it, or add a thumbs-up to an existing one.
- Use the [issue forms](https://github.com/Clawdlinux/atticdb/issues/new/choose) for a bug, a feature request or a question.
- Issues labeled [`waiting-for-users`](https://github.com/Clawdlinux/atticdb/labels/waiting-for-users) are ideas we parked until enough people want them. Thumbs-ups count.
- A bug report needs the `atticdb doctor` output. It holds no tokens.

## Setup

- Rust: [rust-toolchain.toml](rust-toolchain.toml) pins the version and adds rustfmt and clippy. Run `rustup toolchain install` in the repo, as CI does.
- Python 3 with pytest runs the Python client, research and installer tests. `python3 -m pip install -r clients/python/requirements-test.txt` installs the pinned pytest. CI uses Python 3.12.
- On Linux, CI also runs `shellcheck install.sh`.
- [ci.yml](.github/workflows/ci.yml) holds the exact commands CI runs. If this file and ci.yml differ, ci.yml wins.
- [TESTING.md](TESTING.md) says what gets a test and how to write it.

### Build and test

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace && cargo test --workspace --release
ATTICDB_SIM_SEEDS=2000 cargo test -p atticdb-tape --release --features sim --test sim
ATTICDB_SIM_SEEDS=2000 cargo test -p atticdb-core --release --features sim --test engine_sim
```

CI then runs the fault-injection, Python, research and installer tests:

```sh
python3 -m pip install -r clients/python/requirements-test.txt
cargo test -p atticdb --release --features fault-inject --test it -- --include-ignored
cargo build -p atticdb --release --features fault-inject
ATTICDB_BIN=target/release/atticdb python3 -m pytest clients/python/tests
python3 -m pytest research/ctxcache/tests
ATTICDB_BIN=target/release/atticdb python3 -m pytest research/setup_eval/tests
python3 -m pytest scripts/tests
shellcheck install.sh
```

Check Windows code from macOS or Linux with:

```sh
rustup target add x86_64-pc-windows-msvc
cargo clippy -p atticdb --target x86_64-pc-windows-msvc --features blake3/pure --all-targets --all-features -- -D warnings
```

`blake3/pure` skips blake3's C and assembly, which need a Windows C compiler. Never commit it.

## Pull requests

- Sign off every commit with `git commit -s`. The sign-off certifies the [Developer Certificate of Origin](https://developercertificate.org).
- Keep each pull request small, and add or update tests.
- CI must pass on Linux, macOS and Windows.
- Open an issue before you add a dependency. AtticDB keeps 6: blake3, crc32c, httparse, rustix, serde_json and windows-sys.
- A design change adds a row to the [decision log](docs/design/0002-decision-log.md).
- Fill in the pull request template, including the commands you ran.
- Docs follow 4 writing rules:
  - Short sentences, 20 words or fewer.
  - No em dashes.
  - Numbers as digits, such as "3 apps".
  - Plain words. No marketing terms.

## License

AtticDB is licensed under MIT OR Apache-2.0, at your option. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

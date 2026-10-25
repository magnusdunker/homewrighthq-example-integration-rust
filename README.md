# Example counter for HomewrightHQ

A small external integration for [HomewrightHQ](https://github.com/magnusdunker/homewrighthq). It is for trying out how integrations are installed from GitHub, and as a starting point for writing your own.

Each setup creates a device with:

- a sensor, **Antal** (count), that counts up by one at a fixed interval
- a switch, **Tæller** (counter), that pauses the counting
- a button, **Nulstil** (reset), that sets the number to 0 and fires the event `example_counter_reset`

It also has the service `example_counter.set`, which sets a counter to a given value, and an options flow where you can change the interval. The number is stored in the integration's data folder, so it survives a restart.

## Installation

In HomewrightHQ: **Settings → Integrations → From GitHub**. Paste

```
https://github.com/magnusdunker/homewrighthq-example-integration-rust
```

and press **Install**. Then add **Eksempel-tæller** under **Add integration**.

## How it works

An external integration is a standalone program. HomewrightHQ starts it and talks to it over JSON-RPC on stdin and stdout. The protocol is described in HomewrightHQ's [docs/custom-integrations.md](https://github.com/magnusdunker/homewrighthq/blob/main/docs/custom-integrations.md), so the integration can be written in any language.

The repo contains:

- `thehome.json`: domain, name and protocol version
- `sdk/`: the crate `thehome-plugin`, which handles the protocol in Rust
- `src/main.rs`: the integration itself
- `.github/workflows/release.yml`: builds a binary for each platform and puts them on a release

## Make your own

1. Copy the repo and change `domain` and `name` in `thehome.json`. The domain may only contain lowercase letters, digits and `_`, and must not be the domain of a built-in integration.
2. Change `BIN` and `DOMAIN` at the top of `release.yml`, and the binary name in `Cargo.toml`.
3. Implement `Integration` from `thehome-plugin`. Only `setup_entry` is required.
4. Push a tag, e.g. `git tag v0.1.0 && git push --tags`. The workflow builds for Windows, Linux (x86_64 and arm64) and macOS and creates the release.

HomewrightHQ always fetches the newest release. The binaries must be named `<domain>-<target>`, e.g. `example_counter-aarch64-unknown-linux-musl`.

Try it locally with `cargo build`. The program waits for messages from HomewrightHQ on stdin, so it does nothing when started on its own.

## License

MIT

# 🎁

A simple presence server.

## Summary

Presents is a simple websocket-based presence server.
Clients connect to the server over a WebSocket, and must set `presents-tag` (who am I) and `presents-topic` (where am I) headers.
On successful connections, the client will be sent a message with the tags of all other clients present for the same topic.
Connected clients are sent a message whenever a client connects or disconnects for the same topic.

## Usage

Presents is written in Rust, and currently must be built from source.
You can get set up for Rust development quickly using https://rustup.rs/.

With a Rust toolchain installed, simply clone the repo and use `cargo run`:

```sh
git clone https://github.com/connec/presents
cd presents
cargo run
```

## Future possibilities

- Binary/docker builds: the server must currently be built from source.
- Multiple topics: currently the `presents-topic` header accepts a single value, but it might be useful to be present in multiple topics at the same time (without needing multiple connections).
- Authentication/authorization: currently the server is entirely unauthenticated, anyone who knows the URL can use the service with arbitrary topics.

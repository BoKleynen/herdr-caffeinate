# Herdr Caffeinate Debug Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `tracing` diagnostics that show where plugin startup or event processing exits.

**Architecture:** Initialize a `tracing_subscriber` filter from `RUST_LOG` at the start of `main`. Add structured logs at socket connections, request writes, snapshot parsing, subscriptions, event handling, caffeinate transitions, EOF, and errors. Keep behavior unchanged.

**Tech Stack:** Rust 2024, `tracing`, `tracing-subscriber` with `env-filter`, existing `anyhow`, Unix sockets.

## Global Constraints

- Do not change the event protocol or caffeinate behavior.
- Preserve `RUST_LOG` filtering.
- Preserve `HERDR_SOCKET_PATH`, defaulting to `/tmp/herdr.sock`.
- Log IDs, pane IDs, event names, status values, and counts.
- Do not log full socket payloads unless needed for an error.
- Do not modify unrelated files or the existing plugin manifest.

---

### Task 1: Add tracing dependencies and initialize logging

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/main.rs`

**Interfaces:**
- `main` initializes `tracing_subscriber` before any fallible startup operation.
- `RUST_LOG=debug` enables debug messages without code changes.

- [ ] **Step 1: Add dependencies**

Add:

```toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

Run `cargo check` to update `Cargo.lock` and confirm dependency resolution.

- [ ] **Step 2: Initialize the subscriber**

At the first line of `main`, add:

```rust
tracing_subscriber::fmt()
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .init();
```

Log the selected socket path with `info!` before connecting.

- [ ] **Step 3: Verify the logging setup**

Run: `RUST_LOG=debug cargo test`

Expected: all tests pass and the test process accepts the logging configuration.

### Task 2: Instrument startup and socket boundaries

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Existing functions keep their signatures and return types.
- Each fallible socket boundary emits context before returning its error.

- [ ] **Step 1: Log event socket startup**

Add logs around the event socket connection, global subscription request, and successful write:

```rust
tracing::debug!(path = %socket_path, "connecting event socket");
tracing::debug!("event socket connected");
tracing::debug!("subscribing to pane lifecycle events");
```

Log errors with `error!(error = %error, ...)` before returning them when the current `?` would otherwise end startup silently.

- [ ] **Step 2: Log snapshot startup and response parsing**

In `read_snapshot`, log connection, request send, each ignored response ID, the matching response, the number of panes, and snapshot socket EOF or errors.

Use fields such as:

```rust
tracing::debug!(request_id, "requesting session snapshot");
tracing::debug!(request_id = %request_id, "received snapshot response");
tracing::info!(pane_count = snapshot.panes.len(), "loaded session snapshot");
```

- [ ] **Step 3: Log pane subscriptions**

In `subscribe_to_pane`, log duplicate subscriptions at debug level and new subscriptions at info or debug level with `pane_id`.

- [ ] **Step 4: Log initial caffeinate state**

Log the number of tracked panes and whether any pane is working before calling `update_caffeinate`.

- [ ] **Step 5: Run the focused checks**

Run: `cargo test`

Expected: all existing tests pass.

### Task 3: Instrument event handling and process lifecycle

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- `handle_event` preserves current status tracking and event filtering.
- `start_caffeinate` and `stop_caffeinate` retain their current process behavior.

- [ ] **Step 1: Log received event categories**

In `handle_event`, log the event name at debug level before dispatch. Log pane creation, closure, and status changes with `pane_id` and status fields.

- [ ] **Step 2: Log ignored and malformed messages**

Log ignored event names at trace or debug level. Keep the existing warning path for malformed messages, but include the parse error and the event-loop context.

- [ ] **Step 3: Log caffeinate transitions**

In `update_caffeinate`, log aggregate working state and child presence. In `start_caffeinate`, log spawn success and failure. In `stop_caffeinate`, log kill and wait actions.

- [ ] **Step 4: Log event-loop termination**

Log normal event socket EOF at info level and read errors at error level before cleanup. Log final shutdown after `caffeinate` cleanup.

- [ ] **Step 5: Run the full checks**

Run:

```text
cargo fmt -- --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
```

Expected: all commands pass. The release build may retain the existing macOS deployment-target linker warning.

### Task 4: Reproduce with runtime diagnostics

**Files:**
- No source changes.

- [ ] **Step 1: Run the requested command with debug logging**

Run:

```bash
HERDR_SOCKET_PATH=/Users/bokleynen/.config/herdr/herdr.sock \
HERDR_PLUGIN_STATE_DIR=/Users/bokleynen/.local/state/herdr-caffeinate \
RUST_LOG=debug cargo run --release
```

- [ ] **Step 2: Identify the exit boundary**

Use the final log line to classify the cause as socket connection, request write, snapshot response, event EOF, parse error, or process failure.

- [ ] **Step 3: Report the evidence**

Return the observed exit boundary and any remaining root cause. Do not add retries or protocol changes until the logs prove they are needed.

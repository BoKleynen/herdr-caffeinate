# Herdr Subscription Handshake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the event socket open by completing the documented subscription acknowledgement before requesting the snapshot.

**Root cause evidence:** The event socket connects and accepts the request. It then emits an RPC response with no `event` field, which the client logs as `event=""`. The client does not recognize or verify this acknowledgement. Herdr’s socket API requires clients to wait for the `events.subscribe` acknowledgement before starting `session.snapshot` on another connection.

**Architecture:** Add a startup handshake on the event socket. Read until the global subscription response arrives, buffer any lifecycle events received before that response, then request the snapshot. Treat RPC responses and pushed events as different message types. Apply buffered events after snapshot state is loaded.

**Files:** Modify `src/main.rs`. Add focused unit tests in its existing test module. Do not change dependencies or the plugin manifest.

## Constraints

- Keep the separate snapshot socket.
- Subscribe globally to `pane.created` and `pane.closed`.
- Preserve all existing status tracking and caffeinate behavior.
- Never discard lifecycle events received during the handshake.
- Log subscription acknowledgements, API errors, event names, and socket closure reasons.

## Task 1: Add failing handshake parsing tests

1. Add a test for a successful subscription response:

```rust
let message = json!({
    "id": "global-subscription",
    "result": { "type": "subscription_started" }
});
assert!(is_subscription_ack(&message, "global-subscription"));
```

2. Add a test proving a lifecycle event is not treated as an acknowledgement.
3. Add a test for an API error response with the requested ID.
4. Run `cargo test subscription_ack` and verify the tests fail because the helper is missing.

## Task 2: Wait for the global subscription acknowledgement

1. Add a helper with this interface:

```rust
fn wait_for_subscription_ack(
    reader: &mut BufReader<UnixStream>,
    request_id: &str,
) -> Result<Vec<String>>
```

2. Read newline-delimited messages until the response has the matching request ID.
3. Return an error when the matching response contains `error`.
4. Return an error when the socket closes before the acknowledgement.
5. Buffer non-matching lines and return them in arrival order.
6. Log the acknowledgement result type and request ID.
7. Log and return API errors with their request ID.
8. Run the focused tests and confirm they pass.

## Task 3: Apply the handshake in startup

1. Send the global subscription on the event socket.
2. Call `wait_for_subscription_ack` before opening the snapshot socket.
3. Load the snapshot on the separate socket as before.
4. Subscribe to the snapshot panes.
5. Process buffered lifecycle events before entering the live event loop.
6. Refactor one event-line handler so buffered lines and live lines use the same code path.
7. Handle RPC responses on the live event socket by logging their request ID and result type, then ignoring them.
8. Handle pushed events only when the top-level `event` field is present.
9. Replace the empty `event=""` debug output with explicit response logs.

## Task 4: Verify the live failure boundary

Run:

```text
cargo fmt -- --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
HERDR_SOCKET_PATH=/Users/bokleynen/.config/herdr/herdr.sock HERDR_PLUGIN_STATE_DIR=/Users/bokleynen/.local/state/herdr-caffeinate RUST_LOG=debug cargo run --release
```

Expected startup logs:

- The global subscription acknowledgement arrives before snapshot connection.
- The snapshot loads without closing the event stream.
- RPC acknowledgements do not appear as empty event names.
- The event loop stays open until Herdr closes the subscription or an error occurs.

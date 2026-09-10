# Herdr Per-Pane Subscription Sockets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans (recommended) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep pane status monitoring alive by using one socket for each `events.subscribe` stream.

**Root cause evidence:** The global subscription is acknowledged. The snapshot succeeds. The server closes the event socket immediately after the client sends a second `events.subscribe` request for `pane.agent_status_changed`. Herdr documents each subscription connection as one acknowledged request followed by pushed events. The current client incorrectly sends multiple subscription requests on one stream.

**Architecture:** Keep one lifecycle socket for `pane.created` and `pane.closed`. Open one status socket per pane for `pane.agent_status_changed`. Move socket readers into threads that send typed lifecycle and status messages to the main loop through `std::sync::mpsc`. The main loop alone owns pane state and caffeinate.

**Files:** Modify `src/main.rs`. Add focused unit tests in its existing test module. Keep dependencies unchanged.

## Constraints

- Send only one `events.subscribe` request on each socket.
- Wait for every subscription acknowledgement before consuming its stream.
- Preserve the snapshot bootstrap and initial caffeinate decision.
- Ignore status events for panes already closed.
- Shut down status sockets when panes close.
- Log subscription failures and reader termination with pane IDs.

## Task 1: Add failing message-routing tests

1. Add tests for a typed lifecycle message containing `pane_created`.
2. Add tests for a typed status message containing `pane_id` and `agent_status`.
3. Add a test proving a status message for an untracked pane does not change the state map.
4. Run `cargo test subscription_message` and verify failure because the new message types are absent.

## Task 2: Add one-shot subscription socket helpers

1. Define an internal channel message enum:

```rust
enum HerdrMessage {
    Lifecycle(Value),
    Status(AgentStatusChanged),
    SocketClosed { pane_id: Option<String> },
}
```

2. Add `subscribe_socket(socket_path, subscription, request_id)` that connects one socket, sends one `events.subscribe` request, waits for the matching `subscription_started` response, and returns a reader stream plus a shutdown handle.
3. Use the existing acknowledgement parser for errors and EOF.
4. Add a reader thread for each returned stream. Parse pushed events and send `HerdrMessage` values to the main loop.
5. Log request IDs, pane IDs, acknowledgement results, parse failures, and EOF.
6. Run the focused tests and `cargo test`.

## Task 3: Separate lifecycle and status streams

1. Use the existing lifecycle socket only for the global `pane.created` and `pane.closed` subscription.
2. After the snapshot, create one status socket and reader thread for each existing pane.
3. Remove the current call that sends pane status subscriptions through the lifecycle socket.
4. On `pane_created`, insert the initial status and create that pane’s status socket.
5. On `pane_closed`, remove the pane state, shut down and join its status reader, and reconcile caffeinate.
6. On `Status` messages, update only tracked panes and reconcile caffeinate.
7. On lifecycle or status socket closure, log the pane ID and keep other subscriptions alive.
8. Have the main loop receive from the channel instead of blocking only on the lifecycle reader.

## Task 4: Verify the live socket behavior

Run:

```text
cargo fmt -- --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
HERDR_SOCKET_PATH=/Users/bokleynen/.config/herdr/herdr.sock HERDR_PLUGIN_STATE_DIR=/Users/bokleynen/.local/state/herdr-caffeinate RUST_LOG=debug cargo run --release
```

Expected logs:

- The lifecycle subscription is acknowledged.
- The snapshot succeeds.
- Each pane status subscription uses its own socket and is acknowledged.
- The lifecycle socket stays open after status subscriptions start.
- Status changes can start and stop caffeinate.

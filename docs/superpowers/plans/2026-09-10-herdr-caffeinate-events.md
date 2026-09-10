# Herdr Caffeinate Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite the plugin to track agent status through the current Herdr event API and control `caffeinate` correctly.

**Architecture:** Use one long-lived Unix socket for event subscriptions and one short-lived Unix socket for `session.snapshot`. Subscribe to global pane creation and closure events, then subscribe to status changes for each known pane. Keep a pane status map and start or stop one `caffeinate` child from its aggregate state.

**Tech Stack:** Rust 2024, `serde`, `serde_json`, `anyhow`, Unix sockets, macOS `caffeinate`.

## Global Constraints

- Use the current output of `herdr api schema --json` as the API contract.
- Keep the plugin macOS-only.
- Do not add dependencies.
- Preserve `HERDR_SOCKET_PATH`, defaulting to `/tmp/herdr.sock`.
- Subscribe to `pane.created` and `pane.closed` globally.
- Subscribe to `pane.agent_status_changed` with a required `pane_id`.
- Start `caffeinate` when any pane is `working`.
- Stop and reap it when no pane is `working`.

---

### Task 1: Add aggregate status logic tests

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Add a small testable state transition function that accepts a pane status map and returns whether any pane is working.
- Keep process spawning outside this pure logic.

- [ ] **Step 1: Write failing tests for aggregate status behavior**

Add a `#[cfg(test)]` module with tests covering:

```rust
#[test]
fn working_status_is_detected() {
    let statuses = HashMap::from([(String::from("pane-1"), AgentStatus::Working)]);
    assert!(has_working_agent(&statuses));
}

#[test]
fn non_working_statuses_are_not_detected() {
    let statuses = HashMap::from([
        (String::from("pane-1"), AgentStatus::Idle),
        (String::from("pane-2"), AgentStatus::Done),
    ]);
    assert!(!has_working_agent(&statuses));
}

#[test]
fn one_working_pane_keeps_the_aggregate_working() {
    let statuses = HashMap::from([
        (String::from("pane-1"), AgentStatus::Idle),
        (String::from("pane-2"), AgentStatus::Working),
    ]);
    assert!(has_working_agent(&statuses));
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `cargo test has_working_agent`

Expected: compilation fails because `has_working_agent` does not exist.

- [ ] **Step 3: Implement the minimal aggregate helper**

Add:

```rust
fn has_working_agent(statuses: &HashMap<String, AgentStatus>) -> bool {
    statuses.values().any(|status| *status == AgentStatus::Working)
}
```

- [ ] **Step 4: Run the focused tests and verify they pass**

Run: `cargo test has_working_agent`

Expected: all aggregate status tests pass.

### Task 2: Replace the socket protocol and event models

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Decode the current success response shape with an envelope containing an `id` and optional `result` or `error`.
- Decode `session.snapshot` results containing `panes`, where each pane has `pane_id` and `agent_status`.
- Decode normal Herdr event envelopes with `event` and tagged `data`.
- Decode subscription envelopes with `event` and untagged status data containing `pane_id` and `agent_status`.

- [ ] **Step 1: Define the schema-backed Rust types**

Replace the old `PaneInfo`, `SessionSnapshot`, and event enums with the minimum fields needed by the schema:

```rust
#[derive(Deserialize, Debug, Clone)]
struct SnapshotPane {
    pane_id: String,
    agent_status: AgentStatus,
}

#[derive(Deserialize, Debug)]
struct SessionSnapshot {
    panes: Vec<SnapshotPane>,
}

#[derive(Deserialize, Debug)]
struct RpcResponse<T> {
    id: String,
    result: Option<T>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HerdrEventData {
    PaneCreated { pane: SnapshotPane },
    PaneClosed { pane_id: String },
    #[serde(other)]
    Other,
}

#[derive(Deserialize, Debug)]
struct HerdrEvent {
    event: String,
    data: HerdrEventData,
}

#[derive(Deserialize, Debug)]
struct AgentStatusChanged {
    pane_id: String,
    agent_status: AgentStatus,
}

#[derive(Deserialize, Debug)]
struct SubscriptionEvent {
    event: String,
    data: AgentStatusChanged,
}
```

Use separate parsing attempts for RPC responses, normal event envelopes, and subscription events. Do not assume every incoming line has the same shape.

- [ ] **Step 2: Add request builders for each supported subscription**

Build JSON requests with unique IDs:

```rust
fn subscribe_request(id: &str, subscriptions: Vec<serde_json::Value>) -> String
fn pane_status_subscription(id: &str, pane_id: &str) -> String
fn snapshot_request(id: &str) -> String
```

The global subscription must contain:

```json
{"type":"pane.created"}
{"type":"pane.closed"}
```

Each pane subscription must contain:

```json
{"type":"pane.agent_status_changed","pane_id":"..."}
```

- [ ] **Step 3: Run compile and unit tests**

Run: `cargo test`

Expected: the new types and request helpers compile, and the aggregate tests pass.

### Task 3: Implement two-socket startup and event tracking

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- `main` connects the event socket first and keeps its reader and writer alive.
- A separate snapshot connection sends `session.snapshot`, reads its response, and may close.
- `subscribe_pane` sends one status subscription per pane ID and returns whether the ID was newly added.

- [ ] **Step 1: Add the event socket setup**

Connect to `HERDR_SOCKET_PATH`, clone the stream, and immediately send one `events.subscribe` request for `pane.created` and `pane.closed`.

- [ ] **Step 2: Add snapshot startup**

Open a second Unix socket, send `session.snapshot`, read lines until the response with the matching request ID arrives, and extract `result.panes`.

Insert every snapshot pane into the status map. Start `caffeinate` immediately when the map contains a working pane. Subscribe to status changes for every snapshot pane on the event socket.

If the snapshot response contains an error or no result, return an `anyhow` error. Ignore EOF after the response because the snapshot socket is disposable.

- [ ] **Step 3: Add pane subscription deduplication**

Keep a `HashSet<String>` of pane IDs whose status subscription has been sent.

When a pane appears in the snapshot or a `pane_created` event, send its status subscription only if the ID is not already in the set.

- [ ] **Step 4: Process the event socket**

Read one line at a time and handle messages in this order:

1. A `pane_created` event inserts the pane’s initial status and subscribes to its status changes.
2. A `pane_closed` event removes the pane status and subscription ID.
3. A `pane.agent_status_changed` event updates the pane status.

After each state change, compare `has_working_agent` with whether `caffeinate_child` exists. Start only for a new working aggregate, and stop plus wait when the aggregate becomes idle.

- [ ] **Step 5: Add shutdown cleanup**

When the event socket reaches EOF or `main` returns, stop and wait for any running `caffeinate` child before exiting.

- [ ] **Step 6: Run the full test suite**

Run: `cargo test`

Expected: all tests pass.

### Task 4: Verify formatting, linting, and release build

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Format the code**

Run: `cargo fmt -- --check`

Expected: no formatting changes are reported.

- [ ] **Step 2: Run Clippy**

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: Clippy completes without warnings.

- [ ] **Step 3: Build the release binary**

Run: `cargo build --release`

Expected: `target/release/herdr-caffeinate` builds successfully for the existing macOS plugin manifest.

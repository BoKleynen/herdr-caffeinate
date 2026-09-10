# Herdr Caffeinate File Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write Herdr Caffeinate logs to `herdr-caffeinate.log` in the plugin state directory with an `info` default level.

**Architecture:** Resolve `HERDR_PLUGIN_STATE_DIR`, falling back to `~/.local/state/herdr-caffeinate`. Create the directory, build a non-rotating `tracing_appender` writer, and keep its guard alive in `main`. Preserve `RUST_LOG` as the level override.

**Tech Stack:** Rust 2024, `tracing`, `tracing-subscriber`, `tracing-appender`, existing `anyhow` and `std::path` APIs.

## Global Constraints

- Use `HERDR_PLUGIN_STATE_DIR` when it is set.
- Use `~/.local/state/herdr-caffeinate` when the variable is not set.
- Write to `herdr-caffeinate.log`.
- Use `info` when `RUST_LOG` is absent.
- Preserve `RUST_LOG` filtering when it is present.
- Use non-blocking logging with no rotation.
- Return contextual errors for missing home, directory creation, and appender initialization failures.
- Do not change socket, event, or caffeinate behavior.

---

### Task 1: Add the file logging dependency

**Files:**
- Modify: `Cargo.toml:9-14`
- Modify: `Cargo.lock` through Cargo dependency resolution

**Interfaces:**
- Produces the `tracing_appender` crate used by the logging initializer.

- [ ] **Step 1: Add the dependency**

Add this entry under the existing tracing dependencies:

```toml
tracing-appender = "0.2"
```

- [ ] **Step 2: Resolve the lockfile**

Run:

```text
cargo check
```

Expected: Cargo resolves `tracing-appender` and updates `Cargo.lock` without source errors.

- [ ] **Step 3: Review the dependency diff**

Run:

```text
jj diff -- Cargo.toml Cargo.lock
```

Expected: only the new direct dependency and its required lockfile entries appear.

### Task 2: Add testable logging path resolution

**Files:**
- Modify: `src/main.rs:1-14` for imports
- Modify: `src/main.rs` near `main` for path helpers
- Test: `src/main.rs` existing `#[cfg(test)]` module

**Interfaces:**
- Produces `fn resolve_state_dir(state_dir: Option<&OsStr>, home_dir: Option<&Path>) -> Result<PathBuf>`.
- Produces `fn init_logging() -> Result<WorkerGuard>` for `main`.

- [ ] **Step 1: Write the path resolution tests**

Add tests that call the pure helper without changing process environment variables:

```rust
#[test]
fn uses_configured_state_directory() {
    let path = resolve_state_dir(Some(OsStr::new("/tmp/herdr-state")), None).unwrap();
    assert_eq!(path, PathBuf::from("/tmp/herdr-state"));
}

#[test]
fn falls_back_to_home_state_directory() {
    let path = resolve_state_dir(None, Some(Path::new("/Users/tester"))).unwrap();
    assert_eq!(path, PathBuf::from("/Users/tester/.local/state/herdr-caffeinate"));
}

#[test]
fn rejects_missing_state_directory_and_home() {
    assert!(resolve_state_dir(None, None).is_err());
}
```

Import `OsStr` and `Path` for the tests and `PathBuf` for the implementation.

- [ ] **Step 2: Run the first focused test to verify failure**

Run:

```text
cargo test uses_configured_state_directory
```

Expected: compilation fails because `resolve_state_dir` does not exist yet.

- [ ] **Step 3: Implement the pure path helper**

Implement this behavior:

```rust
fn resolve_state_dir(state_dir: Option<&OsStr>, home_dir: Option<&Path>) -> Result<PathBuf> {
    state_dir
        .map(PathBuf::from)
        .or_else(|| home_dir.map(|home| home.join(".local/state/herdr-caffeinate")))
        .ok_or_else(|| anyhow!("HERDR_PLUGIN_STATE_DIR is unset and HOME is unavailable"))
}
```

- [ ] **Step 4: Run the focused tests to verify success**

Run:

```text
cargo test resolve_state_dir
```

Expected: all three tests pass.

### Task 3: Initialize the non-blocking file subscriber

**Files:**
- Modify: `src/main.rs` `main` and logging setup area

**Interfaces:**
- Consumes `resolve_state_dir`.
- Produces `init_logging() -> Result<tracing_appender::non_blocking::WorkerGuard>`.
- Keeps the returned guard alive until `main` exits.

- [ ] **Step 1: Implement `init_logging`**

Use `env::var_os("HERDR_PLUGIN_STATE_DIR")` and `env::var_os("HOME")` as inputs to `resolve_state_dir`.

Create the directory with:

```rust
fs::create_dir_all(&state_dir)
    .with_context(|| format!("create plugin state directory {}", state_dir.display()))?;
```

Build the appender with the fallible builder so initialization errors do not panic:

```rust
let appender = tracing_appender::rolling::RollingFileAppender::builder()
    .rotation(tracing_appender::rolling::Rotation::NEVER)
    .filename_prefix("herdr-caffeinate.log")
    .build(&state_dir)
    .with_context(|| format!("open log file in {}", state_dir.display()))?;
```

Create the non-blocking writer and filter:

```rust
let (writer, guard) = tracing_appender::non_blocking(appender);
let filter = tracing_subscriber::EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
tracing_subscriber::fmt().with_env_filter(filter).with_writer(writer).init();
```

Return `guard` after installing the subscriber.

- [ ] **Step 2: Update `main` before socket startup**

Replace the current three-line stderr subscriber setup with:

```rust
let _logging_guard = init_logging()?;
```

Keep this binding in scope for all of `main`. The existing first log record must occur after it.

- [ ] **Step 3: Run all unit tests**

Run:

```text
cargo test
```

Expected: every test passes and no logger initialization runs during unit tests.

### Task 4: Verify file output and project checks

**Files:**
- No additional source files

**Interfaces:**
- Verifies the log file path, default level, `RUST_LOG` override, and existing behavior.

- [ ] **Step 1: Check formatting**

Run:

```text
cargo fmt -- --check
```

Expected: no formatting changes are required.

- [ ] **Step 2: Check lint and tests**

Run:

```text
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Expected: both commands pass.

- [ ] **Step 3: Check the release build**

Run:

```text
cargo build --release
```

Expected: the release build passes.

- [ ] **Step 4: Verify runtime output in a temporary state directory**

Run the plugin with an existing Herdr socket and an explicit state directory:

```text
HERDR_PLUGIN_STATE_DIR=/tmp/herdr-caffeinate-state cargo run --release
```

Expected: `/tmp/herdr-caffeinate-state/herdr-caffeinate.log` exists and contains `info` records.

- [ ] **Step 5: Verify the debug override**

Run the same command with `RUST_LOG=debug`:

```text
HERDR_PLUGIN_STATE_DIR=/tmp/herdr-caffeinate-state RUST_LOG=debug cargo run --release
```

Expected: the log contains debug records such as `connecting event socket`.

# Herdr Caffeinate File Logging Design

## Goal

Write plugin logs to a file inside the plugin state directory.

Use `info` as the default log level. Keep `RUST_LOG` as an override.

## Log Path

Use `HERDR_PLUGIN_STATE_DIR` when it is set.

Use `~/.local/state/herdr-caffeinate` when the variable is not set.

Create the directory before starting the logger.

Write to `herdr-caffeinate.log` in that directory.

## Logging Setup

Add the existing `tracing-appender` crate as a dependency.

Create a `tracing_appender::rolling::never` appender for the log directory and
file name. Wrap it in a non-blocking writer.

Keep the non-blocking writer guard alive for the full lifetime of `main`.

Build the filter from `RUST_LOG` when present. Use `info` when it is absent.

Write the existing tracing records to the file instead of stderr. Do not add
log rotation or change existing log messages.

## Error Handling

Return a contextual error when the fallback home directory is unavailable.

Return a contextual error when directory creation fails.

Return a contextual error when the log appender cannot open the file.

Do not start socket connections before logging is ready.

## Testing

Run the formatter, tests, Clippy, and release build.

Add one focused test for the default log filter if the setup can be tested
without installing a global subscriber.

Verify manually that an unset `HERDR_PLUGIN_STATE_DIR` uses the fallback path,
and that `RUST_LOG=debug` enables debug records.

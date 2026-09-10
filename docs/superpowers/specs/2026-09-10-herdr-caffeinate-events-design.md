# Herdr Caffeinate Event Design

## Goal

Keep macOS awake while at least one Herdr agent has a `working` status.

Use the current Herdr API schema instead of the old pane update model.

## API Flow

The plugin will use two Unix socket connections.

The event socket stays open. It subscribes to `pane.created` and `pane.closed`,
then receives per-pane `pane.agent_status_changed` events.

The snapshot socket requests `session.snapshot`. Herdr may close this socket
after the response, so it has no other responsibility.

Startup will follow this order:

1. Open the event socket.
2. Subscribe to `pane.created` and `pane.closed`.
3. Open the snapshot socket.
4. Request `session.snapshot`.
5. Store the status of every returned pane.
6. Start `caffeinate` if any returned pane is already `working`.
7. Subscribe on the event socket to `pane.agent_status_changed` for every
   returned pane.
8. Process new events.

Subscribing to `pane.created` before requesting the snapshot prevents new pane
events from being lost during startup. Duplicate pane subscriptions will be
ignored.

## State And Caffeinate

Store the latest `agent_status` by pane ID.

For every status event, update that pane’s status and recalculate whether any
pane is `working`.

Start `caffeinate` only when the state changes from no working panes to at
least one working pane.

Stop and reap the child when the state changes to no working panes.

Remove a pane from the map on `pane.closed`.

## Parsing

Decode the current schema’s response envelope, snapshot result, success event
envelopes, and subscription event envelopes.

Handle only these event types:

- `pane_created`, which contains a `pane` object and its ID.
- `pane.agent_status_changed`, which contains a pane ID and status.
- `pane_closed`, which removes a pane from tracking.

The subscription request uses `pane.created` and `pane.closed` names. Herdr
delivers those normal events with underscore names.

Ignore unrelated valid messages and report malformed or failed API responses.

## Testing

Add small unit tests for status aggregation and caffeinate transitions:

- An initial working pane starts caffeinate.
- A working status starts caffeinate once.
- One working pane keeps caffeinate running when another pane stops.
- The last working pane stopping stops caffeinate.
- Closing a working pane can stop caffeinate.

Run the Rust formatter, linter, and test suite after the rewrite.

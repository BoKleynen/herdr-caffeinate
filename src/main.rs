use std::collections::{HashMap, HashSet};
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::process::{Child, Command};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::{debug, error, info, trace, warn};

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AgentStatus {
    Idle,
    Working,
    Blocked,
    Done,
    Unknown,
}

#[derive(Deserialize, Debug)]
struct PaneState {
    pane_id: String,
    agent_status: AgentStatus,
}

#[derive(Deserialize, Debug)]
struct SessionSnapshot {
    panes: Vec<PaneState>,
}

#[derive(Deserialize, Debug)]
struct SnapshotResponseResult {
    snapshot: SessionSnapshot,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HerdrEventData {
    PaneCreated {
        pane: PaneState,
    },
    PaneClosed {
        pane_id: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize, Debug)]
struct HerdrEvent {
    data: HerdrEventData,
}

#[derive(Deserialize, Debug)]
struct AgentStatusChanged {
    pane_id: String,
    agent_status: AgentStatus,
}

#[derive(Deserialize, Debug)]
struct SubscriptionEvent {
    data: AgentStatusChanged,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let socket_path =
        env::var("HERDR_SOCKET_PATH").unwrap_or_else(|_| "/tmp/herdr.sock".to_owned());
    info!(path = %socket_path, "starting herdr-caffeinate");

    debug!(path = %socket_path, "connecting event socket");
    let event_stream = match UnixStream::connect(&socket_path) {
        Ok(stream) => stream,
        Err(error) => {
            error!(%error, path = %socket_path, "event socket connection failed");
            return Err(error).with_context(|| format!("connect event socket at {socket_path}"));
        }
    };
    debug!("event socket connected");
    let mut event_writer = event_stream.try_clone()?;
    let mut event_reader = BufReader::new(event_stream);
    debug!("subscribing to pane lifecycle events");
    send_global_subscription(&mut event_writer)?;
    debug!("pane lifecycle subscription sent");
    let buffered_events = wait_for_subscription_ack(&mut event_reader, "global-subscription")?;

    let snapshot = read_snapshot(&socket_path)?;
    let mut agent_states = HashMap::new();
    for pane in snapshot.panes {
        agent_states.insert(pane.pane_id, pane.agent_status);
    }
    info!(pane_count = agent_states.len(), "loaded session snapshot");

    let mut caffeinate_child = None;
    debug!(
        pane_count = agent_states.len(),
        working = has_working_agent(&agent_states),
        "reconciling initial caffeinate state"
    );
    update_caffeinate(&agent_states, &mut caffeinate_child);

    let mut subscribed_panes = HashSet::new();
    for line in buffered_events {
        if let Err(error) = handle_event(
            &line,
            &mut event_writer,
            &mut subscribed_panes,
            &mut agent_states,
            &mut caffeinate_child,
        ) {
            warn!(error = %error, "ignoring buffered Herdr message");
        }
    }
    for pane_id in agent_states.keys() {
        subscribe_to_pane(&mut event_writer, &mut subscribed_panes, pane_id)?;
    }

    let mut line = String::new();
    let event_result = loop {
        match event_reader.read_line(&mut line) {
            Ok(0) => {
                info!("event socket closed");
                break Ok(());
            }
            Ok(_) => {
                if let Err(error) = handle_event(
                    &line,
                    &mut event_writer,
                    &mut subscribed_panes,
                    &mut agent_states,
                    &mut caffeinate_child,
                ) {
                    warn!(error = %error, "ignoring Herdr message");
                }
                line.clear();
            }
            Err(error) => {
                error!(error = %error, "event socket read failed");
                break Err(error.into());
            }
        }
    };

    stop_caffeinate(&mut caffeinate_child);
    info!("herdr-caffeinate stopped");
    event_result
}

fn send_global_subscription(writer: &mut UnixStream) -> Result<()> {
    let request = json!({
        "id": "global-subscription",
        "method": "events.subscribe",
        "params": {
            "subscriptions": [
                { "type": "pane.created" },
                { "type": "pane.closed" }
            ]
        }
    });
    write_request(writer, &request).context("send global event subscription")
}

fn write_request(writer: &mut UnixStream, request: &Value) -> Result<()> {
    serde_json::to_writer(&mut *writer, request)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn subscribe_to_pane(
    writer: &mut UnixStream,
    subscribed_panes: &mut HashSet<String>,
    pane_id: &str,
) -> Result<()> {
    if !subscribed_panes.insert(pane_id.to_owned()) {
        debug!(pane_id, "pane status subscription already exists");
        return Ok(());
    }

    let request = json!({
        "id": format!("status-subscription-{pane_id}"),
        "method": "events.subscribe",
        "params": {
            "subscriptions": [{
                "type": "pane.agent_status_changed",
                "pane_id": pane_id
            }]
        }
    });
    debug!(pane_id, "subscribing to pane agent status changes");
    write_request(writer, &request)
        .with_context(|| format!("subscribe to pane agent status changes for {pane_id}"))
}

fn wait_for_subscription_ack(
    reader: &mut BufReader<UnixStream>,
    request_id: &str,
) -> Result<Vec<String>> {
    let mut buffered_events = Vec::new();
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            error!(
                request_id,
                "event socket closed before subscription acknowledgement"
            );
            bail!("event socket closed before subscription acknowledgement");
        }

        let raw_line = line.clone();
        let message: Value = serde_json::from_str(&line).context("parse subscription response")?;
        if is_subscription_error(&message, request_id) {
            let error = message.get("error").unwrap_or(&Value::Null);
            error!(request_id, %error, "subscription request failed");
            bail!("subscription request failed: {error}");
        }
        if is_subscription_ack(&message, request_id) {
            let result_type = message
                .get("result")
                .and_then(|result| result.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            info!(request_id, result_type, "subscription acknowledged");
            return Ok(buffered_events);
        }

        buffered_events.push(raw_line);
    }
}

fn read_snapshot(socket_path: &str) -> Result<SessionSnapshot> {
    debug!(path = %socket_path, "connecting snapshot socket");
    let stream = match UnixStream::connect(socket_path) {
        Ok(stream) => stream,
        Err(error) => {
            error!(%error, path = %socket_path, "snapshot socket connection failed");
            return Err(error).with_context(|| format!("connect snapshot socket at {socket_path}"));
        }
    };
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    let request_id = "session-snapshot";
    debug!(request_id, "requesting session snapshot");
    write_request(
        &mut writer,
        &json!({
            "id": request_id,
            "method": "session.snapshot",
            "params": {}
        }),
    )?;

    let mut line = String::new();
    while reader.read_line(&mut line)? > 0 {
        let message: Value =
            serde_json::from_str(&line).with_context(|| "parse session.snapshot response")?;
        line.clear();

        if message.get("id").and_then(Value::as_str) != Some(request_id) {
            trace!("ignoring response for another request");
            continue;
        }
        if let Some(error) = message.get("error") {
            error!(%error, "session snapshot request failed");
            bail!("session.snapshot failed: {error}");
        }
        let result = message
            .get("result")
            .ok_or_else(|| anyhow!("session.snapshot response has no result"))?;
        let snapshot = snapshot_from_result(result)?.snapshot;
        debug!(
            request_id,
            pane_count = snapshot.panes.len(),
            "received session snapshot"
        );
        return Ok(snapshot);
    }

    error!(request_id, "snapshot socket closed before response");
    bail!("session.snapshot socket closed before returning a response")
}

fn handle_event(
    line: &str,
    writer: &mut UnixStream,
    subscribed_panes: &mut HashSet<String>,
    agent_states: &mut HashMap<String, AgentStatus>,
    caffeinate_child: &mut Option<Child>,
) -> Result<()> {
    let message: Value = serde_json::from_str(line).context("parse event message")?;
    if let Some(request_id) = message.get("id").and_then(Value::as_str) {
        if let Some(error) = message.get("error") {
            warn!(request_id, %error, "Herdr request returned an error");
        } else {
            let result_type = message
                .get("result")
                .and_then(|result| result.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            debug!(request_id, result_type, "Herdr request acknowledged");
        }
        return Ok(());
    }

    let Some(event) = message.get("event").and_then(Value::as_str) else {
        trace!("ignored Herdr message without event or request ID");
        return Ok(());
    };
    debug!(event, "received Herdr event");

    match event {
        event if is_pane_lifecycle_event(event) => {
            let event: HerdrEvent = serde_json::from_value(message)?;
            match event.data {
                HerdrEventData::PaneCreated { pane } => {
                    let pane_id = pane.pane_id.clone();
                    info!(pane_id = %pane_id, status = ?pane.agent_status, "pane created");
                    agent_states.insert(pane_id.clone(), pane.agent_status);
                    subscribe_to_pane(writer, subscribed_panes, &pane_id)?;
                }
                HerdrEventData::PaneClosed { pane_id } => {
                    info!(pane_id = %pane_id, "pane closed");
                    agent_states.remove(&pane_id);
                    subscribed_panes.remove(&pane_id);
                }
                HerdrEventData::Other => {}
            }
        }
        "pane.agent_status_changed" => {
            let event: SubscriptionEvent = serde_json::from_value(message)?;
            debug!(
                pane_id = %event.data.pane_id,
                status = ?event.data.agent_status,
                "pane agent status changed"
            );
            update_agent_status(agent_states, &event.data);
        }
        _ => {
            trace!(event, "ignored Herdr event");
            return Ok(());
        }
    }

    update_caffeinate(agent_states, caffeinate_child);
    Ok(())
}

fn snapshot_from_result(result: &Value) -> Result<SnapshotResponseResult> {
    Ok(serde_json::from_value(result.clone())?)
}

fn is_subscription_ack(message: &Value, request_id: &str) -> bool {
    message.get("id").and_then(Value::as_str) == Some(request_id)
        && message
            .get("result")
            .and_then(|result| result.get("type"))
            .and_then(Value::as_str)
            == Some("subscription_started")
}

fn is_subscription_error(message: &Value, request_id: &str) -> bool {
    message.get("id").and_then(Value::as_str) == Some(request_id) && message.get("error").is_some()
}

fn is_pane_lifecycle_event(event: &str) -> bool {
    matches!(event, "pane_created" | "pane_closed")
}

fn update_agent_status(statuses: &mut HashMap<String, AgentStatus>, event: &AgentStatusChanged) {
    if let Some(status) = statuses.get_mut(&event.pane_id) {
        *status = event.agent_status;
    }
}

fn has_working_agent(statuses: &HashMap<String, AgentStatus>) -> bool {
    statuses
        .values()
        .any(|status| *status == AgentStatus::Working)
}

fn update_caffeinate(statuses: &HashMap<String, AgentStatus>, child: &mut Option<Child>) {
    debug!(
        working = has_working_agent(statuses),
        caffeinate_running = child.is_some(),
        "reconciling caffeinate state"
    );
    if child
        .as_mut()
        .is_some_and(|process| process.try_wait().ok().flatten().is_some())
    {
        *child = None;
    }

    if has_working_agent(statuses) && child.is_none() {
        *child = start_caffeinate();
    } else if !has_working_agent(statuses) && child.is_some() {
        stop_caffeinate(child);
    }
}

fn start_caffeinate() -> Option<Child> {
    let result = Command::new("caffeinate").args(["-i", "-s", "-m"]).spawn();
    match result {
        Ok(child) => {
            info!(pid = child.id(), "started caffeinate");
            Some(child)
        }
        Err(error) => {
            error!(%error, "failed to start caffeinate");
            None
        }
    }
}

fn stop_caffeinate(child: &mut Option<Child>) {
    if let Some(mut process) = child.take() {
        info!(pid = process.id(), "stopping caffeinate");
        let _ = process.kill();
        let _ = process.wait();
        debug!("caffeinate stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_subscription_acknowledgement() {
        let message = json!({
            "id": "global-subscription",
            "result": { "type": "subscription_started" }
        });

        assert!(is_subscription_ack(&message, "global-subscription"));
    }

    #[test]
    fn does_not_treat_lifecycle_event_as_subscription_acknowledgement() {
        let message = json!({
            "event": "pane_created",
            "data": { "type": "pane_created", "pane": {} }
        });

        assert!(!is_subscription_ack(&message, "global-subscription"));
    }

    #[test]
    fn recognizes_subscription_error() {
        let message = json!({
            "id": "global-subscription",
            "error": { "code": "invalid_params", "message": "bad subscription" }
        });

        assert!(is_subscription_error(&message, "global-subscription"));
    }

    #[test]
    fn parses_session_snapshot_result_wrapper() {
        let result = json!({
            "type": "session_snapshot",
            "snapshot": {
                "panes": [{
                    "pane_id": "pane-1",
                    "agent_status": "working"
                }]
            }
        });

        let snapshot = snapshot_from_result(&result).unwrap();

        assert_eq!(snapshot.snapshot.panes[0].pane_id, "pane-1");
        assert_eq!(
            snapshot.snapshot.panes[0].agent_status,
            AgentStatus::Working
        );
    }

    #[test]
    fn recognizes_success_event_lifecycle_names() {
        assert!(is_pane_lifecycle_event("pane_created"));
        assert!(is_pane_lifecycle_event("pane_closed"));
        assert!(!is_pane_lifecycle_event("pane.created"));
    }

    #[test]
    fn ignores_status_for_untracked_panes() {
        let mut statuses = HashMap::from([(String::from("pane-1"), AgentStatus::Working)]);

        update_agent_status(
            &mut statuses,
            &AgentStatusChanged {
                pane_id: String::from("closed-pane"),
                agent_status: AgentStatus::Working,
            },
        );

        assert!(!statuses.contains_key("closed-pane"));
    }

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
}

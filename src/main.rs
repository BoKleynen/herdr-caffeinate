use std::collections::HashMap;
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::process::{Child, Command};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

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

enum HerdrMessage {
    Lifecycle(Value),
    Status(AgentStatusChanged),
    SocketClosed { pane_id: Option<String> },
}

struct StatusSubscription {
    control: UnixStream,
    thread: JoinHandle<()>,
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

    let (sender, receiver) = mpsc::channel();
    let lifecycle_thread = spawn_lifecycle_reader(event_reader, sender.clone());
    let mut status_subscriptions = HashMap::new();
    let mut caffeinate_child = None;
    debug!(
        pane_count = agent_states.len(),
        working = has_working_agent(&agent_states),
        "reconciling initial caffeinate state"
    );
    update_caffeinate(&agent_states, &mut caffeinate_child);

    for line in buffered_events {
        if let Some(message) = parse_lifecycle_message(&line)?
            && let Err(error) = handle_message(
                message,
                &socket_path,
                &sender,
                &mut status_subscriptions,
                &mut agent_states,
                &mut caffeinate_child,
            )
        {
            warn!(error = %error, "ignoring buffered Herdr message");
        }
    }
    for pane_id in agent_states.keys().cloned().collect::<Vec<_>>() {
        ensure_status_subscription(&socket_path, &pane_id, &sender, &mut status_subscriptions);
    }

    while let Ok(message) = receiver.recv() {
        if let Err(error) = handle_message(
            message,
            &socket_path,
            &sender,
            &mut status_subscriptions,
            &mut agent_states,
            &mut caffeinate_child,
        ) {
            warn!(error = %error, "ignoring Herdr message");
        }
    }

    for (_, subscription) in status_subscriptions {
        let _ = subscription.control.shutdown(Shutdown::Both);
        let _ = subscription.thread.join();
    }
    let _ = lifecycle_thread.join();
    stop_caffeinate(&mut caffeinate_child);
    info!("herdr-caffeinate stopped");
    Ok(())
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

fn spawn_lifecycle_reader(
    mut reader: BufReader<UnixStream>,
    sender: Sender<HerdrMessage>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => match parse_lifecycle_message(&line) {
                    Ok(Some(message)) => {
                        if sender.send(message).is_err() {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => warn!(%error, "failed to parse lifecycle message"),
                },
                Err(error) => {
                    error!(%error, "lifecycle socket read failed");
                    break;
                }
            }
        }
        info!("lifecycle socket reader stopped");
        let _ = sender.send(HerdrMessage::SocketClosed { pane_id: None });
    })
}

fn spawn_status_subscription(
    socket_path: &str,
    pane_id: &str,
    sender: &Sender<HerdrMessage>,
) -> Result<StatusSubscription> {
    let stream = UnixStream::connect(socket_path)
        .with_context(|| format!("connect status socket for pane {pane_id}"))?;
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    let request_id = format!("status-subscription-{pane_id}");
    write_request(
        &mut writer,
        &json!({
            "id": request_id,
            "method": "events.subscribe",
            "params": {
                "subscriptions": [{
                    "type": "pane.agent_status_changed",
                    "pane_id": pane_id
                }]
            }
        }),
    )?;
    debug!(pane_id, request_id, "status subscription sent");

    let buffered = wait_for_subscription_ack(&mut reader, &request_id)?;
    for line in buffered {
        if let Some(message) = parse_status_message(&line)? {
            sender.send(message)?;
        }
    }

    let thread_sender = sender.clone();
    let thread_pane_id = pane_id.to_owned();
    let thread = thread::spawn(move || {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => match parse_status_message(&line) {
                    Ok(Some(message)) => {
                        if thread_sender.send(message).is_err() {
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(pane_id = %thread_pane_id, %error, "failed to parse status message");
                    }
                },
                Err(error) => {
                    error!(pane_id = %thread_pane_id, %error, "status socket read failed");
                    break;
                }
            }
        }
        info!(pane_id = %thread_pane_id, "status socket reader stopped");
        let _ = thread_sender.send(HerdrMessage::SocketClosed {
            pane_id: Some(thread_pane_id),
        });
    });

    Ok(StatusSubscription {
        control: writer,
        thread,
    })
}

fn ensure_status_subscription(
    socket_path: &str,
    pane_id: &str,
    sender: &Sender<HerdrMessage>,
    subscriptions: &mut HashMap<String, StatusSubscription>,
) {
    if subscriptions.contains_key(pane_id) {
        return;
    }
    match spawn_status_subscription(socket_path, pane_id, sender) {
        Ok(subscription) => {
            subscriptions.insert(pane_id.to_owned(), subscription);
            info!(pane_id, "status subscription active");
        }
        Err(error) => error!(pane_id, %error, "failed to start status subscription"),
    }
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

fn handle_message(
    message: HerdrMessage,
    socket_path: &str,
    sender: &Sender<HerdrMessage>,
    subscriptions: &mut HashMap<String, StatusSubscription>,
    agent_states: &mut HashMap<String, AgentStatus>,
    caffeinate_child: &mut Option<Child>,
) -> Result<()> {
    match message {
        HerdrMessage::Lifecycle(message) => {
            let event: HerdrEvent = serde_json::from_value(message)?;
            match event.data {
                HerdrEventData::PaneCreated { pane } => {
                    let pane_id = pane.pane_id.clone();
                    info!(pane_id = %pane_id, status = ?pane.agent_status, "pane created");
                    agent_states.insert(pane_id.clone(), pane.agent_status);
                    ensure_status_subscription(socket_path, &pane_id, sender, subscriptions);
                }
                HerdrEventData::PaneClosed { pane_id } => {
                    info!(pane_id = %pane_id, "pane closed");
                    agent_states.remove(&pane_id);
                    if let Some(subscription) = subscriptions.remove(&pane_id) {
                        let _ = subscription.control.shutdown(Shutdown::Both);
                        let _ = subscription.thread.join();
                    }
                }
                HerdrEventData::Other => {}
            }
        }
        HerdrMessage::Status(event) => {
            debug!(pane_id = %event.pane_id, status = ?event.agent_status, "pane agent status changed");
            apply_message(HerdrMessage::Status(event), agent_states);
        }
        HerdrMessage::SocketClosed { pane_id: None } => {
            info!("lifecycle socket reader stopped");
            return Ok(());
        }
        HerdrMessage::SocketClosed {
            pane_id: Some(pane_id),
        } => {
            warn!(pane_id, "status socket reader stopped");
            subscriptions.remove(&pane_id);
        }
    }

    update_caffeinate(agent_states, caffeinate_child);
    Ok(())
}

fn parse_lifecycle_message(line: &str) -> Result<Option<HerdrMessage>> {
    let message: Value = serde_json::from_str(line).context("parse lifecycle message")?;
    if message.get("id").is_some() {
        return Ok(None);
    }
    if message
        .get("event")
        .and_then(Value::as_str)
        .is_some_and(is_pane_lifecycle_event)
    {
        return Ok(Some(HerdrMessage::Lifecycle(message)));
    }
    trace!("ignored lifecycle message");
    Ok(None)
}

fn parse_status_message(line: &str) -> Result<Option<HerdrMessage>> {
    let message: Value = serde_json::from_str(line).context("parse status message")?;
    if message.get("id").is_some() {
        return Ok(None);
    }
    if message.get("event").and_then(Value::as_str) == Some("pane.agent_status_changed") {
        let event: SubscriptionEvent = serde_json::from_value(message)?;
        return Ok(Some(HerdrMessage::Status(event.data)));
    }
    Ok(None)
}

fn apply_message(message: HerdrMessage, statuses: &mut HashMap<String, AgentStatus>) {
    if let HerdrMessage::Status(event) = message {
        update_agent_status(statuses, &event);
    }
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
    fn routes_status_message_for_tracked_pane() {
        let mut statuses = HashMap::from([(String::from("pane-1"), AgentStatus::Idle)]);

        apply_message(
            HerdrMessage::Status(AgentStatusChanged {
                pane_id: String::from("pane-1"),
                agent_status: AgentStatus::Working,
            }),
            &mut statuses,
        );

        assert_eq!(statuses["pane-1"], AgentStatus::Working);
    }

    #[test]
    fn ignores_status_message_for_untracked_pane() {
        let mut statuses = HashMap::new();

        apply_message(
            HerdrMessage::Status(AgentStatusChanged {
                pane_id: String::from("closed-pane"),
                agent_status: AgentStatus::Working,
            }),
            &mut statuses,
        );

        assert!(statuses.is_empty());
    }

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

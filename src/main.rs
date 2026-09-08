use std::collections::HashMap;
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::process::{Child, Command};

use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Idle,
    Working,
    Blocked,
    Done,
    Unknown,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PaneInfo {
    pub pane_id: String,
    pub agent_status: AgentStatus,
    pub agent: Option<String>,
    pub workspace_id: String,
    pub tab_id: String,
}

#[derive(Deserialize, Debug)]
pub struct SessionSnapshot {
    pub panes: Vec<PaneInfo>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseResult {
    SessionSnapshot {
        snapshot: SessionSnapshot,
    },
    SubscriptionStarted,
    #[serde(other)]
    Other,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventData {
    PaneUpdated {
        pane: PaneInfo,
    },
    PaneCreated {
        pane: PaneInfo,
    },
    PaneClosed {
        pane_id: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize, Debug)]
pub struct EventEnvelope {
    pub event: String,
    pub data: EventData,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum SocketMessage {
    Event(EventEnvelope),
}

fn main() -> anyhow::Result<()> {
    let socket_path =
        env::var("HERDR_SOCKET_PATH").unwrap_or_else(|_| "/tmp/herdr.sock".to_owned());

    let stream = UnixStream::connect(&socket_path)?;
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    let sub_req = json!({
        "id": "sub",
        "method": "events.subscribe",
        "params": {
            "subscriptions": [
                { "type": "pane.updated" },
                { "type": "pane.created" },
                { "type": "pane.closed" }
            ]
        }
    })
    .to_string()
        + "\n";
    writer.write_all(sub_req.as_bytes())?;

    // TODO: call session.snapshot (ref: https://herdr.dev/docs/socket-api/#raw-methods).

    let mut agent_states: HashMap<String, AgentStatus> = HashMap::new();
    let mut caffeinate_child: Option<Child> = None;
    let mut line = String::new();

    while reader.read_line(&mut line)? > 0 {
        if let Ok(msg) = serde_json::from_str::<SocketMessage>(&line) {
            let SocketMessage::Event(EventEnvelope { data, .. }) = msg;
            match data {
                EventData::PaneUpdated { pane } | EventData::PaneCreated { pane } => {
                    agent_states.insert(pane.pane_id, pane.agent_status);
                }
                EventData::PaneClosed { pane_id } => {
                    agent_states.remove(&pane_id);
                }
                EventData::Other => {}
            }

            // Recalculate working agent count and toggle caffeinate
            let working_count = agent_states
                .values()
                .filter(|status| **status == AgentStatus::Working)
                .count();

            if working_count > 0 && caffeinate_child.is_none() {
                caffeinate_child = start_caffeinate();
            } else if working_count == 0 && caffeinate_child.is_some() {
                stop_caffeinate(&mut caffeinate_child);
            }
        }
        line.clear();
    }

    Ok(())
}

fn start_caffeinate() -> Option<Child> {
    Command::new("caffeinate")
        .args(["-i", "-s", "-m"])
        .spawn()
        .ok()
}

fn stop_caffeinate(child: &mut Option<Child>) {
    if let Some(mut process) = child.take() {
        let _ = process.kill();
        let _ = process.wait();
    }
}

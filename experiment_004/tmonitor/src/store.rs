//! In-memory task store.
//!
//! Accumulates [`TaskEvent`]s into [`TaskRecord`]s that can be
//! queried by the API endpoints. Each record holds the task's full
//! lifecycle: metadata, status, elapsed time, output detail, and a
//! timestamped event log.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use trunner::TaskEvent;

/// A timestamped entry in a task's event log.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineEntry {
    pub kind: String,
    /// Milliseconds since the Unix epoch.
    pub timestamp: u64,
    /// For `finished` events, the status string (e.g. "Success").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

/// A complete record of a task's lifecycle, serialisable as JSON for
/// the API.
#[derive(Debug, Clone, Serialize)]
pub struct TaskRecord {
    pub name: String,
    pub timeout_ms: u64,
    /// `None` while the task is still running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Caller-provided context from the originating task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    pub events: Vec<TimelineEntry>,
}

/// Thread-safe, cloneable handle to the in-memory store.
#[derive(Clone)]
pub struct TaskStore {
    inner: Arc<RwLock<StoreInner>>,
}

struct StoreInner {
    /// Insertion-ordered task records. We use a `Vec` of keys plus a
    /// `HashMap` for lookup so the API returns tasks in the order
    /// they were started.
    order: Vec<String>,
    records: HashMap<String, TaskRecord>,
}

impl TaskStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StoreInner {
                order: Vec::new(),
                records: HashMap::new(),
            })),
        }
    }

    /// Record an event into the store.
    pub fn record(&self, event: &TaskEvent) {
        let mut inner = self.inner.write().unwrap();

        match event {
            TaskEvent::Started {
                name,
                timeout,
                metadata,
            } => {
                let record = TaskRecord {
                    name: name.clone(),
                    timeout_ms: timeout.as_millis() as u64,
                    status: None,
                    elapsed_ms: None,
                    detail: None,
                    metadata: metadata.clone(),
                    events: vec![TimelineEntry {
                        kind: "started".into(),
                        timestamp: now_ms(),
                        status: None,
                    }],
                };
                inner.order.push(name.clone());
                inner.records.insert(name.clone(), record);
            }

            TaskEvent::Finished(result) => {
                let status_str = format!("{:?}", result.status);
                if let Some(record) = inner.records.get_mut(&result.name) {
                    record.status = Some(status_str.clone());
                    record.elapsed_ms = Some(result.elapsed.as_millis() as u64);
                    record.detail = Some(result.detail.clone());
                    // Preserve metadata from the result if the started
                    // event didn't carry one (shouldn't happen, but be
                    // defensive).
                    if record.metadata.is_none() {
                        record.metadata = result.metadata.clone();
                    }
                    record.events.push(TimelineEntry {
                        kind: "finished".into(),
                        timestamp: now_ms(),
                        status: Some(status_str),
                    });
                }
            }

            TaskEvent::TimeoutSent { name } => {
                if let Some(record) = inner.records.get_mut(name) {
                    record.events.push(TimelineEntry {
                        kind: "timeout_sent".into(),
                        timestamp: now_ms(),
                        status: None,
                    });
                }
            }

            // We don't store interrupt/tick/run-completed events per
            // task, but we could add a global event log later.
            _ => {}
        }
    }

    /// Return all task records in insertion order.
    pub fn all_tasks(&self) -> Vec<TaskRecord> {
        let inner = self.inner.read().unwrap();
        inner
            .order
            .iter()
            .filter_map(|name| inner.records.get(name).cloned())
            .collect()
    }

    /// Return a single task record by name.
    pub fn get_task(&self, name: &str) -> Option<TaskRecord> {
        let inner = self.inner.read().unwrap();
        inner.records.get(name).cloned()
    }
}

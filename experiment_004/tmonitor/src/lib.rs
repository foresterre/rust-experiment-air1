//! A lightweight monitoring web service for the task trunner.
//!
//! Subscribes to the trunner's `broadcast::Receiver<TaskEvent>` and
//! accumulates task records in memory. Serves a real-time dashboard
//! over HTTP with SSE for live updates, and a JSON API for
//! historical task inspection.
//!
//! # Endpoints
//!
//!   - `GET /`             — HTML dashboard with live SSE updates.
//!   - `GET /events`       — SSE stream of JSON-encoded task events.
//!   - `GET /api/tasks`    — JSON array of all task records.
//!   - `GET /api/tasks/:name` — JSON detail for a single task.
//!
//! # Usage
//!
//! ```rust,ignore
//! let dispatcher = trunner::dispatcher::dispatcher(config);
//! let event_rx = dispatcher.subscribe();
//!
//! let monitor = monitor::Monitor::new(event_rx);
//! let monitor_handle = tokio::spawn(monitor.serve(([127, 0, 0, 1], 3000)));
//! ```

mod store;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use store::TaskStore;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{info, warn};
use trunner::TaskEvent;

/// A serialisable mirror of [`TaskEvent`] for the SSE stream.
///
/// Kept separate from the trunner's type so the wire format is an
/// explicit contract, not an accident of internal representation.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SseEvent {
    Started {
        name: String,
        timeout_ms: u64,
        metadata: Option<String>,
    },
    Finished {
        name: String,
        status: String,
        elapsed_ms: u64,
        detail: String,
        metadata: Option<String>,
    },
    TimeoutSent {
        name: String,
    },
    Interrupted {
        active_count: usize,
    },
    Tick {
        active_count: usize,
    },
    RunCompleted {
        success: usize,
        failure: usize,
        panicked: usize,
        timed_out: usize,
        skipped: usize,
    },
}

fn task_event_to_sse(event: &TaskEvent) -> SseEvent {
    match event {
        TaskEvent::Started {
            name,
            timeout,
            metadata,
        } => SseEvent::Started {
            name: name.clone(),
            timeout_ms: timeout.as_millis() as u64,
            metadata: metadata.clone(),
        },
        TaskEvent::Finished(result) => SseEvent::Finished {
            name: result.name.clone(),
            status: format!("{:?}", result.status),
            elapsed_ms: result.elapsed.as_millis() as u64,
            detail: result.detail.clone(),
            metadata: result.metadata.clone(),
        },
        TaskEvent::TimeoutSent { name } => SseEvent::TimeoutSent { name: name.clone() },
        TaskEvent::Interrupted { active_count } => SseEvent::Interrupted {
            active_count: *active_count,
        },
        TaskEvent::Tick { active_count } => SseEvent::Tick {
            active_count: *active_count,
        },
        TaskEvent::RunCompleted(stats) => SseEvent::RunCompleted {
            success: stats.success,
            failure: stats.failure,
            panicked: stats.panicked,
            timed_out: stats.timed_out,
            skipped: stats.skipped,
        },
    }
}

/// Shared application state accessible by all route handlers.
type AppState = Arc<AppStateInner>;

struct AppStateInner {
    relay_tx: broadcast::Sender<TaskEvent>,
    store: TaskStore,
}

pub struct Monitor {
    event_rx: broadcast::Receiver<TaskEvent>,
}

impl Monitor {
    pub fn new(event_rx: broadcast::Receiver<TaskEvent>) -> Self {
        Self { event_rx }
    }

    /// Build the Axum router. Exposed for testing — callers who want
    /// a custom listener can use this directly.
    pub fn router(self) -> Router {
        let (relay_tx, _) = broadcast::channel::<TaskEvent>(256);
        let store = TaskStore::new();

        let state: AppState = Arc::new(AppStateInner {
            relay_tx: relay_tx.clone(),
            store: store.clone(),
        });

        let relay_tx_clone = relay_tx.clone();
        let store_clone = store.clone();
        tokio::spawn(async move {
            let mut rx = self.event_rx;
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        store_clone.record(&event);
                        let _ = relay_tx_clone.send(event);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "monitor skipped events (slow consumer)");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("event channel closed, relay task exiting");
                        break;
                    }
                }
            }
        });

        Router::new()
            .route("/", get(dashboard))
            .route("/events", get(sse_handler))
            .route("/api/tasks", get(api_tasks))
            .route("/api/tasks/{name}", get(api_task_detail))
            .with_state(state)
    }

    /// Start serving on the given address.
    pub async fn serve(self, addr: impl Into<SocketAddr>) -> Result<(), std::io::Error> {
        let addr = addr.into();
        let router = self.router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        info!(%addr, "monitor listening");
        axum::serve(listener, router)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}

async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.relay_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(event) => {
            let sse_event = task_event_to_sse(&event);
            match serde_json::to_string(&sse_event) {
                Ok(json) => Some(Ok(Event::default().data(json))),
                Err(_) => None,
            }
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

async fn api_tasks(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.store.all_tasks())
}

async fn api_task_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.store.get_task(&name) {
        Some(record) => Ok(Json(record)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn dashboard() -> impl IntoResponse {
    Html(include_str!("dashboard.html"))
}

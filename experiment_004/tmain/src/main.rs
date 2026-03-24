//! Wires the trunner and monitor together into a single long-lived
//! process that continuously submits random synthetic tasks.
//!
//! Open `http://localhost:3000` to watch the dashboard while the
//! load generator runs. Press Ctrl-C to drain in-flight tasks and
//! shut down.

use rand::{Rng, RngExt};
use std::net::{Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::time::Duration;
use tmonitor::Monitor;
use trunner::dispatcher::{dispatcher, Config, DispatcherHandle};
use trunner::{Task, TaskOutcome};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,trunner=info,monitor=info".parse().unwrap()),
        )
        .init();

    let config = Config {
        max_concurrency: 4,
        tick_interval: Duration::from_secs(2),
    };

    let d = dispatcher(config);
    let event_rx = d.subscribe();
    let handle = d.start();

    let monitor_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 3000));
    let monitor = Monitor::new(event_rx);

    tokio::spawn(async move {
        if let Err(e) = monitor.serve(monitor_addr).await {
            eprintln!("monitor failed to start: {e}");
        }
    });

    // Give the TCP listener a moment to bind so the browser can
    // connect before the first events fire.
    tokio::time::sleep(Duration::from_millis(100)).await;

    eprintln!("monitor at http://{monitor_addr}");
    eprintln!("press ctrl-c to shut down\n");

    // The load generator runs until the handle reports shutdown
    // (which happens when Ctrl-C hits the dispatcher's event loop).
    generate_load(handle.clone()).await;

    let stats = handle.shutdown().await;
    eprintln!("\n{stats}");

    if stats.has_failures() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

// -- load generator -------------------------------------------------------

/// Module prefixes and suffixes used to build realistic-looking task
/// names like `build::frontend::assets` or `test::integration::auth`.
const MODULES: &[&str] = &[
    "build", "test", "lint", "deploy", "check", "bench", "migrate",
];

const SCOPES: &[&str] = &[
    "core",
    "api",
    "auth",
    "db",
    "cache",
    "queue",
    "frontend",
    "payments",
    "search",
    "notifications",
    "analytics",
    "config",
];

const SUFFIXES: &[&str] = &[
    "unit",
    "integration",
    "e2e",
    "snapshot",
    "smoke",
    "regression",
    "assets",
    "schema",
    "indexes",
    "connections",
    "permissions",
];

/// The kind of work a generated task will simulate. The weights
/// control how often each outcome occurs — success is the most
/// common, with occasional failures, rare panics, and occasional
/// timeouts to keep the dashboard interesting.
#[derive(Clone, Copy)]
enum TaskBehaviour {
    /// Completes successfully after sleeping for a random duration.
    Success,
    /// Fails with a message.
    Failure,
    /// Panics (the executor catches it).
    Panic,
    /// Sleeps longer than its timeout, ignoring the cancellation
    /// token.
    Timeout,
    /// Cooperatively cancels when the token fires (still gets
    /// reported as timed out by the dispatcher, since the deadline
    /// passes).
    CooperativeTimeout,
}

fn random_behaviour(rng: &mut impl Rng) -> TaskBehaviour {
    // Rough distribution: 60% success, 15% failure, 5% panic,
    // 10% hard timeout, 10% cooperative timeout.
    let roll: u32 = rng.random_range(0..100);
    match roll {
        0..60 => TaskBehaviour::Success,
        60..75 => TaskBehaviour::Failure,
        75..80 => TaskBehaviour::Panic,
        80..90 => TaskBehaviour::Timeout,
        _ => TaskBehaviour::CooperativeTimeout,
    }
}

fn random_task_name(rng: &mut impl Rng, seq: u64) -> String {
    let module = MODULES[rng.random_range(0..MODULES.len())];
    let scope = SCOPES[rng.random_range(0..SCOPES.len())];
    let suffix = SUFFIXES[rng.random_range(0..SUFFIXES.len())];
    format!("{module}::{scope}::{suffix}#{seq}")
}

const FAILURE_MESSAGES: &[&str] = &[
    "assertion failed: expected 200, got 500",
    "connection refused on port 5432",
    "timeout waiting for lock acquisition",
    "expected Ok, got Err(\"not found\")",
    "snapshot mismatch: 3 lines differ",
    "migration checksum does not match",
    "rate limit exceeded (429)",
    "schema validation failed: missing field `id`",
];

const PANIC_MESSAGES: &[&str] = &[
    "index out of bounds: len is 0 but index is 1",
    "called `Option::unwrap()` on a `None` value",
    "connection pool exhausted",
    "stack overflow in recursive resolver",
    "attempted to divide by zero",
];

fn build_task(rng: &mut impl Rng, seq: u64) -> Task {
    let name = random_task_name(rng, seq);
    let behaviour = random_behaviour(rng);

    // How long the task's work actually takes (for non-timeout cases).
    let work_ms: u64 = rng.random_range(100..2000);

    // The task's timeout budget. For success and failure tasks, the
    // timeout is generous. For timeout tasks, it's deliberately
    // shorter than the work duration.
    let timeout = match behaviour {
        TaskBehaviour::Success | TaskBehaviour::Failure | TaskBehaviour::Panic => {
            Duration::from_secs(10)
        }
        TaskBehaviour::Timeout | TaskBehaviour::CooperativeTimeout => {
            Duration::from_millis(rng.random_range(200..600))
        }
    };

    let fail_msg = FAILURE_MESSAGES[rng.random_range(0..FAILURE_MESSAGES.len())].to_string();
    let panic_msg = PANIC_MESSAGES[rng.random_range(0..PANIC_MESSAGES.len())].to_string();

    let behaviour_label = match behaviour {
        TaskBehaviour::Success => "success",
        TaskBehaviour::Failure => "failure",
        TaskBehaviour::Panic => "panic",
        TaskBehaviour::Timeout => "hard_timeout",
        TaskBehaviour::CooperativeTimeout => "cooperative_timeout",
    };

    let metadata = serde_json::json!({
        "behaviour": behaviour_label,
        "work_ms": work_ms,
        "timeout_ms": timeout.as_millis() as u64,
        "seq": seq,
    })
    .to_string();

    Task::new(name, timeout, move |token| match behaviour {
        TaskBehaviour::Success => {
            std::thread::sleep(Duration::from_millis(work_ms));
            TaskOutcome::Success
        }
        TaskBehaviour::Failure => {
            std::thread::sleep(Duration::from_millis(work_ms));
            TaskOutcome::Failure { message: fail_msg }
        }
        TaskBehaviour::Panic => {
            std::thread::sleep(Duration::from_millis(work_ms / 2));
            panic!("{panic_msg}");
        }
        TaskBehaviour::Timeout => {
            // Ignore the cancellation token entirely.
            std::thread::sleep(Duration::from_secs(4));
            TaskOutcome::Success
        }
        TaskBehaviour::CooperativeTimeout => {
            for _ in 0..40 {
                if token.is_cancelled() {
                    return TaskOutcome::Failure {
                        message: "cancelled by trunner".into(),
                    };
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            TaskOutcome::Success
        }
    })
    .with_metadata(metadata)
}

/// Submits random tasks at random intervals until the dispatcher
/// shuts down (returns `SubmitError`).
async fn generate_load(handle: DispatcherHandle) {
    let mut rng = rand::rng();
    let mut seq: u64 = 0;

    loop {
        let task = build_task(&mut rng, seq);
        seq += 1;

        if let Err(_) = handle.submit(task).await {
            break;
        }

        // Random inter-arrival time: 200ms to 1.5s, so the
        // concurrency slots stay busy but not completely saturated.
        let delay_ms: u64 = rng.random_range(200..1500);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

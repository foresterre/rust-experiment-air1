use crate::{
    CancellationToken, Completion, Task, TaskOutcome, TaskResult, TaskStatus, SUPPRESS_PANIC_OUTPUT,
};
use std::panic;
use std::panic::AssertUnwindSafe;
use std::time::Instant;
use tokio::sync::mpsc;

/// How tasks are actually run. The production implementation spawns a
/// blocking thread; a test implementation can run inline.
pub trait Executor: Send + 'static {
    /// Execute `task` with the given `token` for cancellation, and send
    /// the result through `tx`.
    ///
    /// The implementation decides whether to run on a separate thread,
    /// inline, or in any other way. It must handle panics — either by
    /// catching them or by letting the caller know through the completion.
    fn execute(&self, task: Task, token: CancellationToken, tx: mpsc::Sender<Completion>);
}

/// Run a task function with panic isolation. Shared between executor
/// implementations so the panic-catching logic lives in one place.
pub fn run_with_panic_isolation(task: Task, token: CancellationToken) -> Completion {
    let start = Instant::now();
    let metadata = task.metadata.clone();

    SUPPRESS_PANIC_OUTPUT.set(true);
    let panic_result = panic::catch_unwind(AssertUnwindSafe(|| (task.func)(&token)));
    SUPPRESS_PANIC_OUTPUT.set(false);

    let elapsed = start.elapsed();

    let result = match panic_result {
        Ok(TaskOutcome::Success) => TaskResult {
            name: task.name,
            status: TaskStatus::Success,
            elapsed,
            detail: "ok".into(),
            metadata,
        },
        Ok(TaskOutcome::Failure { message }) => TaskResult {
            name: task.name,
            status: TaskStatus::Failure,
            elapsed,
            detail: message,
            metadata,
        },
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".into());
            TaskResult {
                name: task.name,
                status: TaskStatus::Panicked,
                elapsed,
                detail: message,
                metadata,
            }
        }
    };

    Completion { result, token }
}

/// The production executor. Spawns each task on tokio's blocking thread pool.
pub struct ThreadExecutor;

impl Executor for ThreadExecutor {
    fn execute(&self, task: Task, token: CancellationToken, tx: mpsc::Sender<Completion>) {
        tokio::task::spawn_blocking(move || {
            let completion = run_with_panic_isolation(task, token);
            let _ = tx.blocking_send(completion);
        });
    }
}

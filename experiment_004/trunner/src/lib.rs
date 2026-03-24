//! A task runner inspired in part by actor model from e.g. Erlang, that executes Rust functions
//! with bounded concurrency, cooperative cancellation, and panic isolation.
//!
//! # Architecture
//!
//! Three roles, each behind a trait:
//!
//!   - [`Executor`]: knows *how* to run a task. The production implementation
//!     uses `tokio::task::spawn_blocking`. A test implementation can run tasks
//!     inline for deterministic behaviour.
//!
//!   - [`Reporter`]: knows *how* to present results. The production
//!     implementation prints to stdout/stderr. A test implementation can
//!     collect results into a `Vec`.
//!
//!   - [`Dispatcher`]: owns the event loop. Generic over `Executor` and
//!     `Reporter`. Not a trait itself because there is only one event loop
//!     shape; the variation is in what it delegates to.
//!
//! # Monitoring
//!
//! The dispatcher publishes [`TaskEvent`]s through a `tokio::sync::broadcast`
//! channel. External consumers (such as the `tmonitor` crate's web UI) can
//! subscribe to this channel for real-time progress updates without
//! interfering with task execution.

pub mod dispatcher;
pub mod executor;
pub mod reporter;

use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Thread-local flag: when set, the custom panic hook suppresses output.
// Workers set this before calling the task function, so panics inside
// tasks produce a clean report instead of a full backtrace dump.
thread_local! {
    static SUPPRESS_PANIC_OUTPUT: Cell<bool> = const { Cell::new(false) };
}

/// A cooperative cancellation signal passed to each task.
///
/// Tasks that do long-running work should check `is_cancelled()` at
/// reasonable intervals and return early. Tasks that ignore it will
/// keep running after a timeout — this is the fundamental limitation
/// of in-process execution versus child processes.
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.inner.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.load(Ordering::Acquire)
    }

    /// Pointer-equality test so the dispatcher can match tokens without
    /// requiring `Eq` on the atomic.
    fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// The outcome a task function returns to indicate success or failure.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    Success,
    Failure { message: String },
}

type TaskFn = Box<dyn FnOnce(&CancellationToken) -> TaskOutcome + Send + 'static>;

/// A named unit of work with a timeout.
pub struct Task {
    pub name: String,
    pub timeout: Duration,
    /// Optional caller-provided context (e.g. serialised input
    /// parameters) stored alongside the task for observability.
    /// The runner does not interpret this — it passes it through
    /// to events and results.
    pub metadata: Option<String>,
    func: TaskFn,
}

impl Task {
    pub fn new<F>(name: impl Into<String>, timeout: Duration, func: F) -> Self
    where
        F: FnOnce(&CancellationToken) -> TaskOutcome + Send + 'static,
    {
        Self {
            name: name.into(),
            timeout,
            metadata: None,
            func: Box::new(func),
        }
    }

    /// Attach metadata to this task. Returns `self` for chaining.
    pub fn with_metadata(mut self, metadata: impl Into<String>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Success,
    Failure,
    Panicked,
    TimedOut,
}

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub name: String,
    pub status: TaskStatus,
    pub elapsed: Duration,
    pub detail: String,
    /// Caller-provided context from the originating [`Task`], passed
    /// through for observability.
    pub metadata: Option<String>,
}

/// Events published to the broadcast channel for external observers.
///
/// Each variant carries enough context so a subscriber can maintain
/// a full picture of the run without polling.
#[derive(Debug, Clone)]
pub enum TaskEvent {
    /// A task has been submitted to the executor.
    Started {
        name: String,
        timeout: Duration,
        metadata: Option<String>,
    },
    /// A task completed (success, failure, panic, or timeout).
    Finished(TaskResult),
    /// The cooperative cancellation signal was sent after a timeout.
    TimeoutSent { name: String },
    /// Ctrl-C received; all in-flight tasks are being cancelled.
    Interrupted { active_count: usize },
    /// Periodic heartbeat while tasks are still running.
    Tick { active_count: usize },
    /// The run has completed. No more events will follow.
    RunCompleted(RunStats),
}

#[derive(Debug, Default, Clone)]
pub struct RunStats {
    pub success: usize,
    pub failure: usize,
    pub panicked: usize,
    pub timed_out: usize,
    pub skipped: usize,
}

impl RunStats {
    fn record(&mut self, status: TaskStatus) {
        match status {
            TaskStatus::Success => self.success += 1,
            TaskStatus::Failure => self.failure += 1,
            TaskStatus::Panicked => self.panicked += 1,
            TaskStatus::TimedOut => self.timed_out += 1,
        }
    }

    pub fn total(&self) -> usize {
        self.success + self.failure + self.panicked + self.timed_out + self.skipped
    }

    pub fn has_failures(&self) -> bool {
        self.failure > 0 || self.panicked > 0 || self.timed_out > 0
    }
}

impl fmt::Display for RunStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} passed, {} failed, {} panicked, {} timed out, {} skipped",
            self.success, self.failure, self.panicked, self.timed_out, self.skipped
        )
    }
}

pub struct Completion {
    pub(crate) result: TaskResult,
    pub(crate) token: CancellationToken,
}

/// Tracks an active task's deadline and cancellation token.
struct InFlightTask {
    name: String,
    deadline: Instant,
    token: CancellationToken,
    /// Set to `true` once the deadline has passed and the cancellation
    /// signal has been sent. When the completion arrives, the dispatcher
    /// uses this to override the task's self-reported status with
    /// `TaskStatus::TimedOut`.
    timed_out: bool,
}

/// Result of removing an in-flight task, indicating whether it timed
/// out and whether the timeout was already reported by `cancel_expired`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeoutState {
    /// The task finished before its deadline.
    NotTimedOut,
    /// `cancel_expired` already cancelled the token and the dispatcher
    /// already called `reporter.timeout_sent`.
    AlreadyReported,
    /// The deadline passed but `cancel_expired` never ran for this task
    /// (the completion branch won the `select!` race). The dispatcher
    /// should report the timeout itself.
    NewlyDiscovered,
}

impl TimeoutState {
    pub(crate) fn is_timed_out(self) -> bool {
        matches!(self, Self::AlreadyReported | Self::NewlyDiscovered)
    }
}

/// Manages the set of currently running tasks and their deadlines.
pub(crate) struct InFlightSet {
    tasks: Vec<InFlightTask>,
}

impl InFlightSet {
    pub(crate) fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    pub(crate) fn insert(&mut self, name: String, deadline: Instant, token: CancellationToken) {
        self.tasks.push(InFlightTask {
            name,
            deadline,
            token,
            timed_out: false,
        });
    }

    /// Remove a task by name and return its timeout state. The
    /// dispatcher uses this to decide the final status and whether
    /// to emit a `timeout_sent` report.
    pub(crate) fn remove_by_name(&mut self, name: &str) -> TimeoutState {
        let now = Instant::now();
        let mut state = TimeoutState::NotTimedOut;
        self.tasks.retain(|t| {
            if t.name == name {
                if t.timed_out {
                    // cancel_expired already ran and reported it.
                    state = TimeoutState::AlreadyReported;
                } else if now >= t.deadline {
                    // The completion arrived before cancel_expired ran,
                    // but the deadline has passed. The dispatcher should
                    // report the timeout itself.
                    state = TimeoutState::NewlyDiscovered;
                }
                false
            } else {
                true
            }
        });
        state
    }

    pub(crate) fn remove_by_token(&mut self, token: &CancellationToken) {
        self.tasks.retain(|t| !t.token.same_as(token));
    }

    /// Cancel all tasks whose deadline has passed. Returns the names of
    /// the newly-cancelled tasks for reporting.
    ///
    /// The tasks remain in the set (with `timed_out = true`) so the
    /// event loop keeps waiting for their completions. This avoids a
    /// race where the loop would break before the completion arrives,
    /// causing the result to never be recorded.
    pub(crate) fn cancel_expired(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut expired = Vec::new();

        for task in &mut self.tasks {
            if now >= task.deadline && !task.timed_out {
                task.token.cancel();
                task.timed_out = true;
                expired.push(task.name.clone());
            }
        }

        expired
    }

    /// Cancel all in-flight tasks (used on interrupt).
    pub(crate) fn cancel_all(&self) {
        for task in &self.tasks {
            task.token.cancel();
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns the duration until the earliest *non-timed-out* deadline,
    /// or `None` if there are no pending deadlines. Tasks already marked
    /// as timed out are excluded — they're just waiting for their
    /// completions to arrive.
    pub(crate) fn time_to_next_deadline(&self) -> Option<Duration> {
        self.tasks
            .iter()
            .filter(|t| !t.timed_out)
            .map(|t| t.deadline)
            .min()
            .map(|earliest| {
                let now = Instant::now();
                if earliest <= now {
                    Duration::ZERO
                } else {
                    earliest - now
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::{Config, Dispatcher};
    use crate::executor::{run_with_panic_isolation, Executor};
    use crate::reporter::Reporter;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    // -- Test executor: runs tasks on blocking threads with panic isolation. --

    struct TestExecutor;

    impl Executor for TestExecutor {
        fn execute(&self, task: Task, token: CancellationToken, tx: mpsc::Sender<Completion>) {
            tokio::task::spawn_blocking(move || {
                let completion = run_with_panic_isolation(task, token);
                let _ = tx.blocking_send(completion);
            });
        }
    }

    #[derive(Default)]
    struct CollectedEvents {
        started: Vec<String>,
        finished: Vec<TaskResult>,
        timeouts: Vec<String>,
        interrupts: Vec<usize>,
        ticks: Vec<usize>,
    }

    struct CollectingReporter {
        events: Arc<Mutex<CollectedEvents>>,
    }

    impl CollectingReporter {
        fn new() -> (Self, Arc<Mutex<CollectedEvents>>) {
            let events = Arc::new(Mutex::new(CollectedEvents::default()));
            (
                Self {
                    events: events.clone(),
                },
                events,
            )
        }
    }

    impl Reporter for CollectingReporter {
        fn task_started(&mut self, name: &str) {
            self.events.lock().unwrap().started.push(name.to_string());
        }

        fn task_finished(&mut self, result: &TaskResult) {
            self.events.lock().unwrap().finished.push(result.clone());
        }

        fn timeout_sent(&mut self, name: &str) {
            self.events.lock().unwrap().timeouts.push(name.to_string());
        }

        fn interrupted(&mut self, active_count: usize) {
            self.events.lock().unwrap().interrupts.push(active_count);
        }

        fn tick(&mut self, active_count: usize) {
            self.events.lock().unwrap().ticks.push(active_count);
        }
    }

    fn test_dispatcher(
        max_concurrency: usize,
    ) -> (
        Dispatcher<TestExecutor, CollectingReporter>,
        Arc<Mutex<CollectedEvents>>,
    ) {
        let (reporter, events) = CollectingReporter::new();
        let config = Config {
            max_concurrency,
            tick_interval: Duration::from_secs(60), // Effectively disabled in tests.
        };
        (Dispatcher::new(TestExecutor, reporter, config), events)
    }

    #[tokio::test]
    async fn all_tasks_pass() {
        let tasks = vec![
            Task::new("a", Duration::from_secs(5), |_| TaskOutcome::Success),
            Task::new("b", Duration::from_secs(5), |_| TaskOutcome::Success),
            Task::new("c", Duration::from_secs(5), |_| TaskOutcome::Success),
        ];

        let (dispatcher, events) = test_dispatcher(2);
        let stats = dispatcher.run(tasks).await;

        assert_eq!(stats.success, 3);
        assert!(!stats.has_failures());

        let events = events.lock().unwrap();
        assert_eq!(events.started.len(), 3);
        assert_eq!(events.finished.len(), 3);
    }

    #[tokio::test]
    async fn failure_and_panic_are_isolated() {
        let tasks = vec![
            Task::new("pass", Duration::from_secs(5), |_| TaskOutcome::Success),
            Task::new("fail", Duration::from_secs(5), |_| TaskOutcome::Failure {
                message: "bad".into(),
            }),
            Task::new("panic", Duration::from_secs(5), |_| {
                panic!("boom");
            }),
            Task::new("also_pass", Duration::from_secs(5), |_| {
                TaskOutcome::Success
            }),
        ];

        let (dispatcher, events) = test_dispatcher(4);
        let stats = dispatcher.run(tasks).await;

        assert_eq!(stats.success, 2);
        assert_eq!(stats.failure, 1);
        assert_eq!(stats.panicked, 1);
        assert!(stats.has_failures());

        let events = events.lock().unwrap();
        let panic_result = events
            .finished
            .iter()
            .find(|r| r.status == TaskStatus::Panicked)
            .expect("should have a panicked result");
        assert_eq!(panic_result.detail, "boom");
    }

    #[tokio::test]
    async fn cooperative_cancellation_on_timeout() {
        let tasks = vec![Task::new("slow", Duration::from_millis(50), |token| {
            for _ in 0..500 {
                if token.is_cancelled() {
                    return TaskOutcome::Failure {
                        message: "cancelled".into(),
                    };
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            TaskOutcome::Success
        })];

        let (dispatcher, events) = test_dispatcher(1);
        let stats = dispatcher.run(tasks).await;

        // The dispatcher overrides the status to TimedOut when the
        // deadline fired, regardless of what the task returned.
        assert_eq!(stats.timed_out, 1);

        let events = events.lock().unwrap();
        assert!(
            !events.timeouts.is_empty(),
            "should have sent a timeout signal"
        );

        let result = events
            .finished
            .iter()
            .find(|r| r.name == "slow")
            .expect("should have a finished result for 'slow'");
        assert_eq!(result.status, TaskStatus::TimedOut);
    }

    #[tokio::test]
    async fn concurrency_is_bounded() {
        // Use an atomic flag to detect overlapping execution.
        let occupied = Arc::new(AtomicBool::new(false));
        let violation = Arc::new(AtomicBool::new(false));

        let mut tasks = Vec::new();
        for i in 0..4 {
            let occupied = occupied.clone();
            let violation = violation.clone();
            tasks.push(Task::new(
                format!("task_{i}"),
                Duration::from_secs(5),
                move |_| {
                    // With max_concurrency=1, swapping in `true` should always
                    // return `false`. If it returns `true`, two tasks overlap.
                    if occupied.swap(true, Ordering::SeqCst) {
                        violation.store(true, Ordering::SeqCst);
                    }
                    std::thread::sleep(Duration::from_millis(20));
                    occupied.store(false, Ordering::SeqCst);
                    TaskOutcome::Success
                },
            ));
        }

        let (dispatcher, _) = test_dispatcher(1);
        let stats = dispatcher.run(tasks).await;

        assert_eq!(stats.success, 4);
        assert!(
            !violation.load(Ordering::SeqCst),
            "two tasks ran concurrently with max_concurrency=1"
        );
    }

    #[tokio::test]
    async fn incremental_submit_via_handle() {
        let (dispatcher, events) = test_dispatcher(2);
        let handle = dispatcher.start();

        // Submit tasks one at a time.
        handle
            .submit(Task::new("first", Duration::from_secs(5), |_| {
                std::thread::sleep(Duration::from_millis(30));
                TaskOutcome::Success
            }))
            .await
            .unwrap();

        handle
            .submit(Task::new("second", Duration::from_secs(5), |_| {
                std::thread::sleep(Duration::from_millis(30));
                TaskOutcome::Success
            }))
            .await
            .unwrap();

        // Submit a third after a short delay — it should still get
        // picked up by the running event loop.
        tokio::time::sleep(Duration::from_millis(10)).await;
        handle
            .submit(Task::new("third", Duration::from_secs(5), |_| {
                std::thread::sleep(Duration::from_millis(30));
                TaskOutcome::Success
            }))
            .await
            .unwrap();

        let stats = handle.shutdown().await;

        assert_eq!(stats.success, 3);
        assert!(!stats.has_failures());

        let events = events.lock().unwrap();
        assert_eq!(events.started.len(), 3);
        assert_eq!(events.finished.len(), 3);
    }

    #[tokio::test]
    async fn submit_after_shutdown_returns_error() {
        let (dispatcher, _events) = test_dispatcher(2);
        let handle = dispatcher.start();

        // Clone the handle so we can submit after shutdown.
        let handle2 = handle.clone();
        let _stats = handle.shutdown().await;

        // The event loop has exited, so submit should fail.
        let result = handle2
            .submit(Task::new("late", Duration::from_secs(5), |_| {
                TaskOutcome::Success
            }))
            .await;

        assert!(result.is_err());
    }
}

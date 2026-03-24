use crate::executor::{Executor, ThreadExecutor};
use crate::reporter::{Reporter, StdioReporter};
use crate::{
    CancellationToken, Completion, InFlightSet, RunStats, Task, TaskEvent, TaskResult,
    TaskStatus, TimeoutState, SUPPRESS_PANIC_OUTPUT,
};
use std::panic;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Configuration for a dispatcher run.
pub struct Config {
    pub max_concurrency: usize,
    pub tick_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            tick_interval: Duration::from_secs(2),
        }
    }
}

/// Commands sent from the handle to the event loop.
enum Command {
    /// Submit a new task for execution.
    Submit(Task),
    /// Request a graceful shutdown. The event loop will finish all
    /// in-flight tasks, then send the final stats through the oneshot
    /// and exit.
    Shutdown(oneshot::Sender<RunStats>),
}

/// The event loop. Generic over how tasks are executed and how results
/// are reported, but the scheduling and cancellation logic is fixed.
pub struct Dispatcher<E: Executor, R: Reporter> {
    executor: E,
    reporter: R,
    config: Config,
    event_tx: broadcast::Sender<TaskEvent>,
}

impl<E: Executor, R: Reporter> Dispatcher<E, R> {
    pub fn new(executor: E, reporter: R, config: Config) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            executor,
            reporter,
            config,
            event_tx,
        }
    }

    /// Subscribe to the event stream. Call this before `start()` —
    /// events emitted before subscription are lost (broadcast
    /// semantics).
    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.event_tx.subscribe()
    }

    /// Spawn the event loop and return a handle for submitting tasks.
    ///
    /// The event loop runs until [`DispatcherHandle::shutdown`] is
    /// called (and all in-flight tasks complete), or until Ctrl-C is
    /// received.
    ///
    /// Installs a custom panic hook that suppresses backtrace output
    /// for panics caught by the executor.
    pub fn start(self) -> DispatcherHandle {
        // 32 is enough backpressure for the command channel — the
        // event loop drains it eagerly, and callers that submit
        // faster than the concurrency limit will just park on
        // send().await, which is the desired behaviour.
        let (cmd_tx, cmd_rx) = mpsc::channel(32);

        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            let stats = self.run_loop(cmd_rx).await;
            let _ = event_tx.send(TaskEvent::RunCompleted(stats));
        });

        DispatcherHandle { cmd_tx }
    }

    /// One-shot convenience: submit all tasks, shut down, and return
    /// the final stats. This is the old `run()` API preserved for
    /// callers that have all tasks upfront.
    pub async fn run(self, tasks: Vec<Task>) -> RunStats {
        let handle = self.start();
        handle.run_batch(tasks).await
    }

    async fn run_loop(mut self, mut cmd_rx: mpsc::Receiver<Command>) -> RunStats {
        let previous_hook = panic::take_hook();
        panic::set_hook(Box::new(|info| {
            let suppress = SUPPRESS_PANIC_OUTPUT.with(|flag| flag.get());
            if !suppress {
                eprintln!("{info}");
            }
        }));

        let (completion_tx, mut completion_rx) =
            mpsc::channel::<Completion>(self.config.max_concurrency);

        let mut stats = RunStats::default();
        let mut in_flight = InFlightSet::new();
        // Tasks that arrived while at the concurrency limit.
        let mut pending: Vec<Task> = Vec::new();

        let mut tick = tokio::time::interval(self.config.tick_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut cancelled = false;
        // Set when Shutdown is received. We store the oneshot sender
        // here so we can reply with the final stats once all in-flight
        // tasks have drained.
        let mut shutdown_reply: Option<oneshot::Sender<RunStats>> = None;

        loop {
            // If a shutdown was requested and all work has drained,
            // we're done.
            if shutdown_reply.is_some() && in_flight.is_empty() && pending.is_empty() {
                break;
            }

            // Try to fill concurrency slots from the pending queue.
            while !cancelled && in_flight.len() < self.config.max_concurrency && !pending.is_empty()
            {
                let task = pending.remove(0);
                self.spawn_task(task, &completion_tx, &mut in_flight);
            }

            let deadline_sleep = Self::sleep_until_next_deadline(&in_flight);

            // Only listen for new commands if we haven't shut down yet.
            // After shutdown the command channel is effectively closed
            // from our side: we stop reading, which causes the
            // sender half to report errors to the handle.
            let recv_cmd = async {
                if shutdown_reply.is_some() {
                    // Don't accept new commands after shutdown.
                    std::future::pending::<Option<Command>>().await
                } else {
                    cmd_rx.recv().await
                }
            };

            tokio::select! {
                Some(completion) = completion_rx.recv() => {
                    self.handle_completion(
                        completion,
                        &mut in_flight,
                        &mut stats,
                    );

                    // Fill a newly-opened slot.
                    if !cancelled
                        && in_flight.len() < self.config.max_concurrency
                        && !pending.is_empty()
                    {
                        let task = pending.remove(0);
                        self.spawn_task(task, &completion_tx, &mut in_flight);
                    }
                }

                cmd = recv_cmd => {
                    match cmd {
                        Some(Command::Submit(task)) => {
                            if cancelled {
                                stats.skipped += 1;
                            } else if in_flight.len() < self.config.max_concurrency {
                                self.spawn_task(task, &completion_tx, &mut in_flight);
                            } else {
                                pending.push(task);
                            }
                        }
                        Some(Command::Shutdown(reply)) => {
                            shutdown_reply = Some(reply);
                            // No more tasks will be accepted. Any
                            // pending tasks still get executed unless
                            // we're cancelled.
                        }
                        None => {
                            // All handles dropped — treat as shutdown.
                            shutdown_reply.get_or_insert_with(|| {
                                // No one to reply to, but we still use
                                // the Some-ness as a "shutting down" flag.
                                let (tx, _rx) = oneshot::channel();
                                tx
                            });
                        }
                    }
                }

                () = deadline_sleep => {
                    let expired = in_flight.cancel_expired();
                    for name in &expired {
                        self.reporter.timeout_sent(name);
                        let _ = self.event_tx.send(TaskEvent::TimeoutSent {
                            name: name.clone(),
                        });
                    }
                }

                _ = tokio::signal::ctrl_c() => {
                    self.reporter.interrupted(in_flight.len());
                    let _ = self.event_tx.send(TaskEvent::Interrupted {
                        active_count: in_flight.len(),
                    });
                    cancelled = true;
                    in_flight.cancel_all();
                    stats.skipped += pending.len();
                    pending.clear();
                }

                _ = tick.tick() => {
                    if !in_flight.is_empty() {
                        self.reporter.tick(in_flight.len());
                        let _ = self.event_tx.send(TaskEvent::Tick {
                            active_count: in_flight.len(),
                        });
                    }
                }
            }
        }

        // Send final stats to whoever requested the shutdown.
        if let Some(reply) = shutdown_reply {
            let _ = reply.send(stats.clone());
        }

        panic::set_hook(previous_hook);
        stats
    }

    fn handle_completion(
        &mut self,
        completion: Completion,
        in_flight: &mut InFlightSet,
        stats: &mut RunStats,
    ) {
        let timeout_state = in_flight.remove_by_name(&completion.result.name);
        in_flight.remove_by_token(&completion.token);

        let result = if timeout_state.is_timed_out() {
            if timeout_state == TimeoutState::NewlyDiscovered {
                self.reporter.timeout_sent(&completion.result.name);
                let _ = self.event_tx.send(TaskEvent::TimeoutSent {
                    name: completion.result.name.clone(),
                });
            }

            TaskResult {
                status: TaskStatus::TimedOut,
                ..completion.result
            }
        } else {
            completion.result
        };

        self.reporter.task_finished(&result);
        let _ = self.event_tx.send(TaskEvent::Finished(result.clone()));
        stats.record(result.status);
    }

    fn spawn_task(
        &mut self,
        task: Task,
        tx: &mpsc::Sender<Completion>,
        in_flight: &mut InFlightSet,
    ) {
        let token = CancellationToken::new();
        let deadline = Instant::now() + task.timeout;

        self.reporter.task_started(&task.name);
        let _ = self.event_tx.send(TaskEvent::Started {
            name: task.name.clone(),
            timeout: task.timeout,
            metadata: task.metadata.clone(),
        });
        in_flight.insert(task.name.clone(), deadline, token.clone());
        self.executor.execute(task, token, tx.clone());
    }

    async fn sleep_until_next_deadline(in_flight: &InFlightSet) {
        match in_flight.time_to_next_deadline() {
            Some(duration) => tokio::time::sleep(duration).await,
            None => std::future::pending::<()>().await,
        }
    }
}

/// A handle to a running dispatcher. Cheap to clone — all clones
/// share the same command channel.
#[derive(Clone)]
pub struct DispatcherHandle {
    cmd_tx: mpsc::Sender<Command>,
}

/// Errors that can occur when interacting with the dispatcher through
/// the handle.
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    #[error("the dispatcher has shut down")]
    Shutdown,
}

impl DispatcherHandle {
    /// Submit a task for execution. Returns an error if the dispatcher
    /// has already shut down.
    pub async fn submit(&self, task: Task) -> Result<(), SubmitError> {
        self.cmd_tx
            .send(Command::Submit(task))
            .await
            .map_err(|_| SubmitError::Shutdown)
    }

    /// Request a graceful shutdown and wait for the final stats.
    ///
    /// All already-submitted and in-flight tasks will run to
    /// completion. No new tasks are accepted after this call.
    pub async fn shutdown(self) -> RunStats {
        let (tx, rx) = oneshot::channel();
        // If the send fails the event loop has already exited, the
        // stats are lost, so return a default.
        if self.cmd_tx.send(Command::Shutdown(tx)).await.is_err() {
            return RunStats::default();
        }
        rx.await.unwrap_or_default()
    }

    /// Convenience: submit a batch of tasks, then shut down and wait
    /// for the final stats. This is the equivalent of the old
    /// `Dispatcher::run(tasks)` for callers that have all tasks
    /// upfront.
    pub async fn run_batch(self, tasks: Vec<Task>) -> RunStats {
        for task in tasks {
            if self.submit(task).await.is_err() {
                break;
            }
        }
        self.shutdown().await
    }
}

/// Create a dispatcher with the production executor and stdio reporter.
pub fn dispatcher(config: Config) -> Dispatcher<ThreadExecutor, StdioReporter> {
    Dispatcher::new(ThreadExecutor, StdioReporter, config)
}

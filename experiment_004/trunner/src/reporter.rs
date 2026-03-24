use crate::{TaskResult, TaskStatus};

/// How task lifecycle events are presented. Production prints to
/// stdout/stderr; tests can collect into a buffer.
pub trait Reporter: Send + 'static {
    fn task_started(&mut self, name: &str);
    fn task_finished(&mut self, result: &TaskResult);
    fn timeout_sent(&mut self, name: &str);
    fn interrupted(&mut self, active_count: usize);
    fn tick(&mut self, active_count: usize);
}

/// The production reporter which prints to stdout and stderr.
pub struct StdioReporter;

impl Reporter for StdioReporter {
    fn task_started(&mut self, name: &str) {
        println!("  START  {name}");
    }

    fn task_finished(&mut self, result: &TaskResult) {
        let label = match result.status {
            TaskStatus::Success => "  PASS   ",
            TaskStatus::Failure => "  FAIL   ",
            TaskStatus::Panicked => "  PANIC  ",
            TaskStatus::TimedOut => "  TIMEOUT",
        };
        println!(
            "{label} {} ({:.2?}, {})",
            result.name, result.elapsed, result.detail
        );
    }

    fn timeout_sent(&mut self, name: &str) {
        eprintln!("  TIMEOUT (cooperative cancel sent): {name}");
    }

    fn interrupted(&mut self, active_count: usize) {
        eprintln!("\ninterrupted -- cancelling {active_count} active tasks");
    }

    fn tick(&mut self, active_count: usize) {
        eprintln!("[tick] {active_count} tasks still running");
    }
}

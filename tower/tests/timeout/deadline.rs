//! Controllable-clock tests for the injected deadline entry point.
//!
//! Everything runs in memory and is driven by hand with [`tokio_test::task`],
//! so there are no sleeps and no reliance on wall-clock timing.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio_test::{assert_pending, assert_ready, task};
use tower::timeout::future::ResponseFuture;

/// A controllable stand-in for tokio's timer: it becomes a resolved deadline
/// once [`ManualClock::elapse`] flips the flag.
#[derive(Clone)]
struct ManualClock {
    elapsed: Arc<AtomicBool>,
}

impl ManualClock {
    fn new() -> Self {
        ManualClock {
            elapsed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Move the clock past the deadline.
    fn elapse(&self) {
        self.elapsed.store(true, Ordering::SeqCst);
    }

    fn is_elapsed(&self) -> bool {
        self.elapsed.load(Ordering::SeqCst)
    }
}

impl Future for ManualClock {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.is_elapsed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Shared state between a controllable response future and its handle.
struct State<E> {
    ready: AtomicBool,
    drops: AtomicUsize,
    error: E,
}

/// Handle used to complete the inner future and inspect drop counts.
struct Handle<E> {
    state: Arc<State<E>>,
}

impl<E> Handle<E>
where
    E: Copy,
{
    fn complete(&self) {
        self.state.ready.store(true, Ordering::SeqCst);
    }

    fn drop_count(&self) -> usize {
        self.state.drops.load(Ordering::SeqCst)
    }
}

/// A controllable inner response future. It resolves to `Ok(value)` or to
/// `Err(error)` depending on `fail`, only after the handle completes it.
struct ManualResponse<T, E> {
    state: Arc<State<E>>,
    value: T,
    fail: bool,
}

impl<T, E> Future for ManualResponse<T, E>
where
    T: Clone,
    E: Clone,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.ready.load(Ordering::SeqCst) {
            if self.fail {
                Poll::Ready(Err(self.state.error.clone()))
            } else {
                Poll::Ready(Ok(self.value.clone()))
            }
        } else {
            Poll::Pending
        }
    }
}

impl<T, E> Drop for ManualResponse<T, E> {
    fn drop(&mut self) {
        self.state.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn inner<T, E>(value: T, error: E) -> (ManualResponse<T, E>, Handle<E>)
where
    T: Clone,
    E: Copy,
{
    let state = Arc::new(State {
        ready: AtomicBool::new(false),
        drops: AtomicUsize::new(0),
        error,
    });
    let handle = Handle {
        state: state.clone(),
    };
    let future = ManualResponse {
        state,
        value,
        fail: false,
    };
    (future, handle)
}

fn failing<T, E>(value: T, error: E) -> (ManualResponse<T, E>, Handle<E>)
where
    T: Clone,
    E: Copy,
{
    let (mut future, handle) = inner(value, error);
    future.fail = true;
    (future, handle)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct InnerError;

impl std::fmt::Display for InnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("inner error")
    }
}

impl std::error::Error for InnerError {}

/// When the inner future and the deadline are both resolved before polling
/// (same turn), the inner result is returned: inner completion wins over the
/// timeout.
#[test]
fn inner_completion_wins_same_turn() {
    let clock = ManualClock::new();
    let (response, handle) = inner("world", InnerError);
    clock.elapse();
    handle.complete();

    let mut fut = task::spawn(ResponseFuture::with_deadline(response, clock.clone()));
    let result = assert_ready!(fut.poll());

    assert_eq!(result.unwrap(), "world");
    // Dropping the now-completed wrapper drops the inner future exactly once.
    drop(fut);
    assert_eq!(handle.drop_count(), 1);
}

/// Same turn, but the inner future resolves to an error: the ready error is
/// passed straight through rather than being replaced by the timeout.
#[test]
fn ready_error_passes_through_same_turn() {
    let clock = ManualClock::new();
    let (response, handle) = failing((), InnerError);
    clock.elapse();
    handle.complete();

    let mut fut = task::spawn(ResponseFuture::with_deadline(response, clock));
    let result = assert_ready!(fut.poll());

    let error = result.unwrap_err();
    let passed = error
        .downcast_ref::<InnerError>()
        .expect("ready inner error must be forwarded, not replaced by Elapsed");
    assert_eq!(*passed, InnerError);

    drop(fut);
    assert_eq!(handle.drop_count(), 1);
}

/// The inner future is still pending when the deadline is reached: the future
/// resolves with the timeout error and the inner future is dropped once.
#[test]
fn deadline_drops_pending_inner() {
    let clock = ManualClock::new();
    let (response, handle) = inner("never", InnerError);

    let mut fut = task::spawn(ResponseFuture::with_deadline(response, clock.clone()));
    assert_pending!(fut.poll(), "nothing is ready yet");
    assert_eq!(handle.drop_count(), 0);

    clock.elapse();
    let result = assert_ready!(fut.poll(), "deadline reached => ready");
    let error = result.unwrap_err();
    assert_eq!(error.to_string(), "request timed out");
    assert!(
        error.downcast_ref::<tower::timeout::error::Elapsed>().is_some(),
        "expected the Elapsed timeout error"
    );

    drop(fut);
    assert_eq!(handle.drop_count(), 1, "pending inner future is dropped once");
}

/// Deadline fires first; a later completion of the inner future is ignored
/// because the wrapper already returned the timeout.
#[test]
fn late_inner_completion_ignored() {
    let clock = ManualClock::new();
    let (response, handle) = inner("late", InnerError);

    let mut fut = task::spawn(ResponseFuture::with_deadline(response, clock.clone()));
    assert_pending!(fut.poll());

    clock.elapse();
    let result = assert_ready!(fut.poll(), "timeout");
    assert!(result.is_err());

    // Completing the inner future afterwards cannot change the outcome.
    handle.complete();
    drop(fut);
    assert_eq!(handle.drop_count(), 1);
}

/// A ready inner error that appears *after* the deadline still must not be
/// rewritten when the inner wins (covered for the same-poll case); here the
/// inner stays pending until after the timeout, so Elapsed wins and the inner
/// future is dropped without its error ever surfacing.
#[test]
fn deadline_wins_when_inner_stays_pending() {
    let clock = ManualClock::new();
    let (response, handle) = failing((), InnerError);

    let mut fut = task::spawn(ResponseFuture::with_deadline(response, clock.clone()));
    assert_pending!(fut.poll());

    clock.elapse();
    assert!(assert_ready!(fut.poll()).is_err());
    drop(fut);
    assert_eq!(handle.drop_count(), 1);
}

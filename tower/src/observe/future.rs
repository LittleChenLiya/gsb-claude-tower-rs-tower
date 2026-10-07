//! Future type for the [`ObserveCall`] service.
//!
//! [`ObserveCall`]: crate::observe::ObserveCall

use super::observer::{CallEvent, CallObserver, CallOutcome};

use pin_project_lite::pin_project;
use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pin_project! {
    /// [`Future`] returned by [`ObserveCall`].
    ///
    /// [`ObserveCall`]: crate::observe::ObserveCall
    pub struct ObserveCallFuture<F, O> {
        #[pin]
        inner: F,
        // The guard is never projected as pinned, so its safe `Drop` records
        // the canceled terminal state when the future is dropped unfinished.
        guard: CancelGuard<O>,
    }
}

/// Invokes an observer, monomorphized per concrete observer type `O`.
///
/// Stored as a plain function pointer so that [`CancelGuard`]'s unbounded
/// `Drop` impl can deliver the canceled event without requiring an
/// `O: CallObserver` bound on `Drop` (which Rust does not allow).
fn invoke_observer<O: CallObserver>(observer: &mut O, event: CallEvent) {
    // An observer failure must not override the inner service's result, so
    // its error is deliberately ignored.
    let _ = observer.observe(event);
}

/// Owns the observer and the call event for one in-flight call.
///
/// Its `Drop` implementation delivers the canceled terminal event exactly
/// once if the call had not already ended.
struct CancelGuard<O> {
    observer: O,
    invoke: fn(&mut O, CallEvent),
    event: CallEvent,
    done: bool,
}

impl<O: CallObserver> CancelGuard<O> {
    fn new(observer: O, event: CallEvent) -> Self {
        Self {
            observer,
            invoke: invoke_observer::<O>,
            event,
            done: false,
        }
    }

    fn finish(&mut self, outcome: CallOutcome) {
        // Mark done first, so even a panicking observer cannot trigger a
        // duplicate cancellation record on drop.
        self.done = true;
        self.event.outcome = outcome;
        (self.invoke)(&mut self.observer, self.event);
    }
}

impl<O> Drop for CancelGuard<O> {
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            self.event.outcome = CallOutcome::Canceled;
            (self.invoke)(&mut self.observer, self.event);
        }
    }
}

impl<F, O> ObserveCallFuture<F, O> {
    pub(super) fn new(inner: F, observer: O, event: CallEvent) -> Self
    where
        O: CallObserver,
    {
        Self {
            inner,
            guard: CancelGuard::new(observer, event),
        }
    }
}

impl<F, T, E, O> Future for ObserveCallFuture<F, O>
where
    F: Future<Output = Result<T, E>>,
    O: CallObserver,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(output) => {
                let outcome = if output.is_ok() {
                    CallOutcome::Success
                } else {
                    CallOutcome::InnerError
                };
                this.guard.finish(outcome);
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F, O> fmt::Debug for ObserveCallFuture<F, O>
where
    F: fmt::Debug,
    O: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserveCallFuture")
            .field("inner", &self.inner)
            .field("observer", &self.guard.observer)
            .field("event", &self.guard.event)
            .field("done", &self.guard.done)
            .finish()
    }
}

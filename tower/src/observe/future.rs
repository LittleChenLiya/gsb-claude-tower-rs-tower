//! Future types for the [`Observe`](crate::observe::Observe) middleware.

use super::{CallObserver, CallOutcome, CallState, PendingSet, Shared};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pin_project! {
    /// Response future from the [`Observe`](crate::observe::Observe) service.
    ///
    /// When this future completes, the terminal outcome is reported to the
    /// [`CallObserver`] exactly once. If the future is dropped before
    /// completing, the outcome is [`CallOutcome::Canceled`] instead.
    pub struct ResponseFuture<F, O: CallObserver> {
        #[pin]
        inner: F,
        state: CallState<O>,
    }
}

impl<F, O: CallObserver> ResponseFuture<F, O> {
    pub(super) fn new(
        inner: F,
        id: u64,
        shared: Arc<Shared<O>>,
        pending: PendingSet,
    ) -> Self {
        ResponseFuture {
            inner,
            state: CallState::new(id, shared, pending),
        }
    }
}

impl<F, O> Future for ResponseFuture<F, O>
where
    F: Future,
    F::Output: ObserveResult,
    O: CallObserver,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(output) => {
                this.state.finish(if output.is_ok() {
                    CallOutcome::Success
                } else {
                    CallOutcome::InnerError
                });
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Internal helper that classifies a service output as success or inner error
/// without constraining the concrete error type.
pub(super) trait ObserveResult {
    fn is_ok(&self) -> bool;
}

impl<T, E> ObserveResult for Result<T, E> {
    fn is_ok(&self) -> bool {
        Result::is_ok(self)
    }
}

//! Future types

use super::error::Elapsed;
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time::Sleep;

pin_project! {
    /// [`Timeout`] response future
    ///
    /// By default the deadline is driven by a [`tokio::time::Sleep`], but the
    /// deadline future can be swapped out through
    /// [`ResponseFuture::with_deadline`]. This keeps the production timing path
    /// on tokio while allowing tests to inject a controllable clock.
    ///
    /// [`Timeout`]: crate::timeout::Timeout
    #[derive(Debug)]
    pub struct ResponseFuture<T, D = Sleep> {
        #[pin]
        response: T,
        #[pin]
        deadline: D,
    }
}

impl<T> ResponseFuture<T> {
    pub(crate) fn new(response: T, sleep: Sleep) -> Self {
        ResponseFuture {
            response,
            deadline: sleep,
        }
    }

    /// Wrap an inner response future with an injected deadline future.
    ///
    /// The deadline is considered reached as soon as `deadline` resolves. When
    /// both the inner future and the deadline resolve on the same poll, the
    /// inner future is polled first, so a completed inner response takes
    /// priority over the timeout; a ready inner error is still propagated
    /// unchanged.
    ///
    /// This is primarily a test entry point that makes the clock controllable
    /// without touching the default tokio timing path used by
    /// [`crate::timeout::Timeout`].
    pub fn with_deadline<F>(response: T, deadline: F) -> ResponseFuture<T, F>
    where
        F: Future<Output = ()>,
    {
        ResponseFuture {
            response,
            deadline,
        }
    }
}

impl<F, T, E, D> Future for ResponseFuture<F, D>
where
    F: Future<Output = Result<T, E>>,
    E: Into<crate::BoxError>,
    D: Future<Output = ()>,
{
    type Output = Result<T, crate::BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // First, try polling the future. If it finishes on the same poll the
        // deadline fires, the inner result wins (including a ready error).
        match this.response.poll(cx) {
            Poll::Ready(v) => return Poll::Ready(v.map_err(Into::into)),
            Poll::Pending => {}
        }

        // Now check the deadline
        match this.deadline.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => Poll::Ready(Err(Elapsed(()).into())),
        }
    }
}

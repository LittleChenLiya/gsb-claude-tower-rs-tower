//! [`Future`] types for [`ReadyPermit`].
//!
//! [`Future`]: std::future::Future
//! [`ReadyPermit`]: super::ReadyPermit
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::OwnedSemaphorePermit;

pin_project! {
    /// Future for the [`ReadyPermit`] service.
    ///
    /// The capacity reserved in `poll_ready` is held for the whole duration of
    /// the call: the permit is released only when this future completes or is
    /// dropped.
    ///
    /// [`ReadyPermit`]: super::ReadyPermit
    #[derive(Debug)]
    pub struct ResponseFuture<T> {
        #[pin]
        inner: T,
        // Dropped (and therefore released) when the future finishes.
        _permit: OwnedSemaphorePermit,
    }
}

impl<T> ResponseFuture<T> {
    pub(crate) fn new(inner: T, _permit: OwnedSemaphorePermit) -> ResponseFuture<T> {
        ResponseFuture { inner, _permit }
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

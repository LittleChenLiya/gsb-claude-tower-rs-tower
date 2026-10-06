use super::future::ResponseFuture;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;
use tower_service::Service;

use std::{
    sync::Arc,
    task::{ready, Context, Poll},
};

/// A [`Service`] whose readiness is gated on a capacity permit.
///
/// See the [module-level documentation](crate::ready_permit) for the permit
/// lifecycle.
#[derive(Debug)]
pub struct ReadyPermit<T> {
    inner: T,
    semaphore: PollSemaphore,
    /// The permit acquired in `poll_ready`, if there was spare capacity.
    ///
    /// The permit is settled exactly once: it is either taken by `call` (and
    /// released when the call's future completes or is dropped) or returned
    /// to the semaphore when the service itself is dropped.
    permit: Option<OwnedSemaphorePermit>,
}

impl<T> ReadyPermit<T> {
    /// Create a new readiness-gated service with capacity for `max`
    /// concurrently in-flight requests.
    pub fn new(inner: T, max: usize) -> Self {
        Self::with_semaphore(inner, Arc::new(Semaphore::new(max)))
    }

    /// Create a new readiness-gated service sharing the provided semaphore.
    pub fn with_semaphore(inner: T, semaphore: Arc<Semaphore>) -> Self {
        ReadyPermit {
            inner,
            semaphore: PollSemaphore::new(semaphore),
            permit: None,
        }
    }

    /// Get a reference to the inner service.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Get a mutable reference to the inner service.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Consume `self`, returning the inner service.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<S, Request> Service<Request> for ReadyPermit<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // If we haven't already acquired a permit from the semaphore, try to
        // acquire one first. A second `poll_ready` reuses the held permit
        // rather than acquiring again.
        if self.permit.is_none() {
            self.permit = ready!(self.semaphore.poll_acquire(cx));
            debug_assert!(
                self.permit.is_some(),
                "ReadyPermit semaphore is never closed, so `poll_acquire` \
                 should never fail",
            );
        }

        // Once we've acquired a permit (or if we already had one), poll the
        // inner service, preserving its default readiness behavior.
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // Take the permit; it is now settled by the returned future, which
        // releases it when the call actually ends.
        let permit = self
            .permit
            .take()
            .expect("max requests in-flight; poll_ready must be called first");

        // Call the inner service.
        let future = self.inner.call(request);

        ResponseFuture::new(future, permit)
    }
}

use super::future::ResponseFuture;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;
use tower_service::Service;

use std::{
    sync::Arc,
    task::{ready, Context, Poll},
};

/// Limit concurrent requests driven through a single service handle using a
/// readiness permit.
///
/// See the [module-level documentation](super) for the permit lifecycle.
#[derive(Debug)]
pub struct ReadyPermit<T> {
    inner: T,
    semaphore: PollSemaphore,
    /// The permit reserved by `poll_ready`, if any.
    ///
    /// The reservation is settled exactly once: it is either moved into the
    /// response future by `call`, or it is returned to the semaphore when
    /// this handle (and therefore the reservation) is dropped.
    permit: Option<OwnedSemaphorePermit>,
}

impl<T> ReadyPermit<T> {
    /// Create a new `ReadyPermit` service with an internal permit pool of
    /// `max` permits.
    pub fn new(inner: T, max: usize) -> Self {
        Self::with_semaphore(inner, Arc::new(Semaphore::new(max)))
    }

    /// Create a new `ReadyPermit` service using a provided shared semaphore.
    ///
    /// Each service built with the same semaphore draws from the same pool of
    /// capacity. This does not make the service cloneable; the returned
    /// service is still a single handle.
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
    ///
    /// Any permit reserved through `poll_ready` but not consumed by `call` is
    /// released back to the semaphore.
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
        // Reserve at most one permit: a reservation already held by this
        // handle is reused, so repeated polls do not consume more capacity.
        if self.permit.is_none() {
            self.permit = ready!(self.semaphore.poll_acquire(cx));
            debug_assert!(
                self.permit.is_some(),
                "ReadyPermit semaphore is never closed, so `poll_acquire` \
                 should never fail",
            );
        }

        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // Settle the reservation: the permit is consumed exactly once here and
        // is not released until the call finishes.
        let permit = self
            .permit
            .take()
            .expect("no permit reserved; poll_ready must be called before call");

        let future = self.inner.call(request);

        ResponseFuture::new(future, permit)
    }
}

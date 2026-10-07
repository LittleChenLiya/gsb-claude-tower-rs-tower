//! [`ObserveCall`] service.

use super::{
    future::ObserveCallFuture,
    observer::{CallEvent, CallObserver},
};

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    task::{Context, Poll},
};
use tower_service::Service;

/// A [`Service`] that records the terminal state of every call to an inner
/// service.
///
/// Each [`call`](Service::call) gets a fresh, unique id and the terminal state
/// of that call — success, inner error, or the future being dropped before
/// completion ("canceled") — is reported exactly once to the
/// [`CallObserver`].
///
/// * Errors returned by [`poll_ready`](Service::poll_ready) do not start a
///   call and produce no record.
/// * An error returned by the observer does not override the result produced
///   by the inner service.
/// * Cloning the service shares the id allocator (so ids stay unique across
///   clones) while each clone keeps its own observer and tracks no shared
///   pending-call state; the "canceled" state lives entirely inside the
///   returned future.
#[derive(Clone)]
pub struct ObserveCall<S, O> {
    inner: S,
    observer: O,
    next_id: Arc<AtomicU64>,
}

impl<S, O> ObserveCall<S, O> {
    /// Wraps a service, reporting each call's terminal event to `observer`.
    pub fn new(inner: S, observer: O) -> Self {
        Self {
            inner,
            observer,
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Get a reference to the inner service.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner service.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consume `self`, returning the inner service.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Returns a [`tower_layer::Layer`] that wraps services with
    /// [`ObserveCall`] using a clone of this observer.
    pub fn layer(observer: O) -> super::layer::ObserveCallLayer<O>
    where
        O: Clone,
    {
        super::layer::ObserveCallLayer::new(observer)
    }
}

impl<S, O, Req> Service<Req> for ObserveCall<S, O>
where
    S: Service<Req>,
    O: CallObserver + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ObserveCallFuture<S::Future, O>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // A readiness failure does not start a call, so no id is allocated and
        // no event is recorded.
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let event = CallEvent::started(id);
        let inner = self.inner.call(req);
        ObserveCallFuture::new(inner, self.observer.clone(), event)
    }
}

impl<S, O> fmt::Debug for ObserveCall<S, O>
where
    S: fmt::Debug,
    O: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserveCall")
            .field("inner", &self.inner)
            .field("observer", &self.observer)
            .finish()
    }
}

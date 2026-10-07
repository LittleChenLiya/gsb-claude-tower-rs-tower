//! Middleware that observes the terminal outcome of every [`Service::call`].
//!
//! A [`CallObserver`] receives exactly one notification per successfully issued
//! call, identified by an ID that is unique within the [`Observe`] service
//! (including all of its clones). The outcome is one of:
//!
//! * [`CallOutcome::Success`] — the inner future produced a response,
//! * [`CallOutcome::InnerError`] — the inner future produced an error, or
//! * [`CallOutcome::Canceled`] — the response future was dropped before the
//!   inner future produced an output.
//!
//! A failure of [`Service::poll_ready`] never starts a call, so no
//! notification and no ID is produced for it. If the observer itself returns
//! an error while being notified, that error is counted on the service but is
//! otherwise ignored: the inner result is not replaced, and the outcome is
//! reported exactly once.
//!
//! Cloning the service shares the ID allocator (IDs stay unique across clones)
//! and the observer, while the set of in-flight calls is tracked per clone.

use self::future::ResponseFuture;
use std::{
    collections::HashSet,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tower_service::Service;

pub mod future;

mod layer;
#[doc(inline)]
pub use self::layer::ObserveLayer;

type PendingSet = Arc<Mutex<HashSet<u64>>>;

/// The terminal state of a single [`Service::call`].
///
/// Every call reaches exactly one of these states, and it is reported exactly
/// once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallOutcome {
    /// The response future completed with a response.
    Success,
    /// The response future completed with an error from the inner service.
    InnerError,
    /// The response future was dropped before the inner future completed.
    Canceled,
}

/// Receives the terminal outcome of each observed call.
///
/// An implementation may return an error from [`CallObserver::observe`]. Such
/// an error does not affect the call in any way: the original response or
/// error is still returned to the caller, and the outcome is still reported
/// exactly once. The number of failed notifications is exposed through
/// [`Observe::observer_errors`].
pub trait CallObserver {
    /// Observe that the call identified by `id` reached `outcome`.
    fn observe(&self, id: u64, outcome: CallOutcome) -> Result<(), crate::BoxError>;
}

impl<T: CallObserver + ?Sized> CallObserver for Arc<T> {
    fn observe(&self, id: u64, outcome: CallOutcome) -> Result<(), crate::BoxError> {
        (**self).observe(id, outcome)
    }
}

impl<T: CallObserver + ?Sized> CallObserver for &T {
    fn observe(&self, id: u64, outcome: CallOutcome) -> Result<(), crate::BoxError> {
        (**self).observe(id, outcome)
    }
}

/// State shared between an [`Observe`] service and all of its clones.
struct Shared<O> {
    observer: O,
    /// Monotonic source of call IDs; shared across clones.
    next_id: AtomicU64,
    /// Number of times the observer returned an error.
    observer_errors: AtomicU64,
}

impl<O> Shared<O> {
    fn new(observer: O) -> Self {
        Shared {
            observer,
            next_id: AtomicU64::new(1),
            observer_errors: AtomicU64::new(0),
        }
    }
}

/// A [`Service`] that records the terminal outcome of every call.
///
/// This wraps an inner service and a [`CallObserver`]. The middleware is
/// transparent to requests and responses: their types and values are passed
/// through unchanged.
///
/// [`Service`]: tower_service::Service
pub struct Observe<S, O> {
    inner: S,
    shared: Arc<Shared<O>>,
    /// In-flight calls issued through this service instance only.
    pending: PendingSet,
}

impl<S, O> Observe<S, O> {
    /// Wraps `inner` in [`Observe`] middleware, reporting outcomes to
    /// `observer`.
    pub fn new(inner: S, observer: O) -> Self {
        Self::with_shared(inner, Arc::new(Shared::new(observer)))
    }

    /// Gets a reference to the inner service.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Gets a mutable reference to the inner service.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consumes `self`, returning the inner service.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Returns the IDs of the calls issued through this service instance whose
    /// outcome has not been observed yet.
    ///
    /// Clones keep independent sets: a call is only listed on the service
    /// instance (not its clones) that issued it.
    pub fn pending_calls(&self) -> Vec<u64> {
        let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let mut ids: Vec<u64> = pending.iter().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Number of observer notifications that have returned an error.
    ///
    /// The counter is shared between all clones of the service.
    pub fn observer_errors(&self) -> u64 {
        self.shared.observer_errors.load(Ordering::Relaxed)
    }

    /// Wraps `inner` reusing an existing observer/ID-allocator shared state.
    ///
    /// Used by [`ObserveLayer`] so that every service produced by the same
    /// layer draws IDs from the same sequence.
    fn with_shared(inner: S, shared: Arc<Shared<O>>) -> Self {
        Observe {
            inner,
            shared,
            pending: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl<S, O> fmt::Debug for Observe<S, O>
where
    S: fmt::Debug,
    O: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Observe")
            .field("inner", &self.inner)
            .field("observer", &self.shared.observer)
            .field("pending", &self.pending_calls())
            .finish()
    }
}

impl<S, O> Clone for Observe<S, O>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Observe {
            inner: self.inner.clone(),
            // The ID allocator, observer and error counter are shared ...
            shared: self.shared.clone(),
            // ... while in-flight state is private to each clone.
            pending: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl<S, O, Req> Service<Req> for Observe<S, O>
where
    S: Service<Req>,
    O: CallObserver,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future, O>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // A readiness failure never starts a call, so it produces no record.
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id);

        let inner = self.inner.call(req);
        ResponseFuture::new(inner, id, self.shared.clone(), self.pending.clone())
    }
}

/// Per-call state held by the response future.
///
/// It is a plain, `Unpin` value (the inner future is pinned separately), so a
/// normal [`Drop`] impl is sound. Dropping it while the call is still in
/// flight records the cancellation.
struct CallState<O: CallObserver> {
    id: u64,
    shared: Arc<Shared<O>>,
    pending: PendingSet,
    in_flight: bool,
}

impl<O: CallObserver> CallState<O> {
    fn new(id: u64, shared: Arc<Shared<O>>, pending: PendingSet) -> Self {
        CallState {
            id,
            shared,
            pending,
            in_flight: true,
        }
    }

    /// Reports `outcome` exactly once and removes the call from the in-flight
    /// set of the service that issued it.
    fn finish(&mut self, outcome: CallOutcome) {
        if !self.in_flight {
            return;
        }
        self.in_flight = false;

        let was_tracked = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
        debug_assert!(was_tracked);

        if let Err(error) = self.shared.observer.observe(self.id, outcome) {
            // The observer failing must not change or mask the inner result:
            // count it and keep the original outcome as the single record.
            let _ = error;
            self.shared
                .observer_errors
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl<O: CallObserver> Drop for CallState<O> {
    fn drop(&mut self) {
        if self.in_flight {
            self.finish(CallOutcome::Canceled);
        }
    }
}

//! Readiness-permit based concurrency limiting for a single service handle.
//!
//! [`ReadyPermit`] wraps an inner service and bounds the number of requests
//! that may be processed concurrently with a [`Semaphore`]. Unlike
//! [`ConcurrencyLimit`], it intentionally does not implement `Clone`: each
//! [`ReadyPermit`] is a single handle that accounts for its own readiness
//! permit. Several handles may still share one pool of capacity by being
//! constructed with [`ReadyPermit::with_semaphore`].
//!
//! # Permit lifecycle
//!
//! Capacity obtained through [`poll_ready`](crate::Service::poll_ready) is
//! settled exactly once, either by issuing a call or by abandoning the
//! reservation:
//!
//! 1. `poll_ready` acquires one permit from the semaphore *before* polling
//!    the inner service. Repeated calls while already ready reuse that same
//!    permit; no additional capacity is consumed while a reservation is held.
//! 2. [`call`](crate::Service::call) takes the reserved permit and moves it
//!    into the response future, settling the reservation. After `call` the
//!    service itself holds no reserve, so the next `poll_ready` acquires a
//!    fresh one.
//! 3. The permit is held for as long as the response future is alive and is
//!    released only when the call actually finishes — the response future
//!    completes or is dropped. While a call is in flight, a second request on
//!    an exhausted pool cannot acquire a permit; it becomes ready once the
//!    call releases the capacity.
//! 4. If the handle is dropped after `poll_ready` returns `Ready` but before
//!    `call` (the reservation is abandoned without a request), the held
//!    permit is dropped with the service and returned to the semaphore, so a
//!    later request can become ready again. Dropping the handle while the
//!    inner service is still pending returns capacity in the same way.
//!
//! The default service contract is preserved: readiness must be observed
//! before every `call`, and the inner service is otherwise left untouched.
//!
//! [`Semaphore`]: tokio::sync::Semaphore
//! [`ConcurrencyLimit`]: crate::limit::ConcurrencyLimit

pub mod future;
mod layer;
mod service;

pub use self::{layer::ReadyPermitLayer, service::ReadyPermit};

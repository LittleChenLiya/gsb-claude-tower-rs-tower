//! Middleware that gates service readiness on a capacity permit.
//!
//! [`ReadyPermit`] wraps an inner [`Service`] and acquires a single permit
//! from a semaphore in `poll_ready` before polling the inner service for
//! readiness. This bounds the number of requests that are simultaneously
//! in flight (ready-but-not-yet-called, plus currently executing) for a
//! single service handle, without cloning the service.
//!
//! # Permit lifecycle
//!
//! The capacity acquired by `poll_ready` is settled **exactly once**:
//!
//! 1. `poll_ready` acquires one permit and holds it inside the service. The
//!    inner service's own `poll_ready` is still polled afterwards, so the
//!    default readiness behavior of the wrapped [`Service`] is preserved.
//! 2. `call` takes the held permit and moves it into the returned
//!    [`ResponseFuture`]. The permit is released only when that future
//!    resolves (or is dropped) — i.e. the resource is held for the whole
//!    duration of the actual call, and freed when the call ends.
//! 3. If the service is dropped after becoming ready but before `call` is
//!    invoked, the held permit is returned to the semaphore, so the next
//!    request (on another handle sharing the semaphore) can become ready.
//!
//! Because the permit is an `Option` that is taken by `call`, it can never
//! be settled twice: after `call` takes it, dropping the service returns
//! nothing, and the permit lives exactly as long as the in-flight call.
//!
//! # Scope
//!
//! This middleware currently covers a single service handle only: it does
//! not implement `Clone`-based sharing of an acquired permit, and it does
//! not integrate with `buffer`'s close/wakeup semantics.
//!
//! [`Service`]: tower_service::Service

pub mod future;
mod layer;
mod service;

pub use self::{
    future::ResponseFuture,
    layer::ReadyPermitLayer,
    service::ReadyPermit,
};

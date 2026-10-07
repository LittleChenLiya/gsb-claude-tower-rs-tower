//! Deterministic tests for the per-request timeout using an injected,
//! controllable clock. No real (wall-clock) sleep is used anywhere here:
//!
//! * `deadline.rs` drives [`tower::timeout::ResponseFuture::with_deadline`]
//!   with a tiny in-memory manual clock and checks same-poll priority, drop
//!   counts and ready-error passthrough.
//! * `regression.rs` exercises the original [`Timeout`]/[`TimeoutLayer`]
//!   interface on tokio's paused (virtual) clock and confirms both code paths
//!   agree.
#![cfg(feature = "timeout")]

mod deadline;
mod regression;

//! Observation types for [`ObserveCall`](crate::observe::ObserveCall).

use std::fmt;

/// How a service call ended.
///
/// A [`Service`](tower_service::Service) call always ends in exactly one of
/// these states:
///
/// * [`Success`](CallOutcome::Success): the inner future completed successfully.
/// * [`InnerError`](CallOutcome::InnerError): the inner future completed with
///   an error.
/// * [`Canceled`](CallOutcome::Canceled): the future returned by the inner
///   service was dropped before it completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallOutcome {
    /// The inner future completed successfully.
    Success,
    /// The inner future completed with an error.
    InnerError,
    /// The future returned by the inner service was dropped before it
    /// completed.
    Canceled,
}

/// The terminal event of a single service call.
///
/// Every [`call`](tower_service::Service::call) that reaches the wrapped
/// service is reported through a single, unique [`CallEvent`]. The same event
/// is never delivered twice: once a call ends, its outcome is fixed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallEvent {
    /// The id assigned to the call when it started.
    ///
    /// Ids are unique among the clones of an [`ObserveCall`](crate::observe::ObserveCall)
    /// service: cloning the service shares the id allocator but not any
    /// pending-call state.
    pub id: u64,
    /// How the call ended.
    pub outcome: CallOutcome,
}

impl CallEvent {
    /// Creates the event for a call that just started.
    ///
    /// The outcome is overwritten exactly once, when the call ends.
    pub(crate) fn started(id: u64) -> Self {
        Self {
            id,
            outcome: CallOutcome::Canceled,
        }
    }
}

/// Receives the terminal event of each observed service call.
///
/// Implementations are passed to [`ObserveCall::new`](crate::observe::ObserveCall::new)
/// and [`ObserveCallLayer::new`](crate::observe::ObserveCallLayer::new).
///
/// A terminal event is delivered exactly once per call that reaches the inner
/// service, either when its future completes
/// ([`Success`](CallOutcome::Success) / [`InnerError`](CallOutcome::InnerError))
/// or when that future is dropped while still pending
/// ([`Canceled`](CallOutcome::Canceled)). A [`poll_ready`](tower_service::Service::poll_ready)
/// failure does not start a call and therefore produces no event.
///
/// The error returned by [`observe`](CallObserver::observe) is ignored: an
/// observer failure never replaces or overrides the result produced by the
/// inner service.
pub trait CallObserver {
    /// Observe the terminal event of a call.
    ///
    /// The returned error is ignored and cannot change the inner service's
    /// result.
    fn observe(&mut self, event: CallEvent) -> Result<(), crate::BoxError>;
}

impl<F> CallObserver for F
where
    F: FnMut(CallEvent) -> Result<(), crate::BoxError>,
{
    fn observe(&mut self, event: CallEvent) -> Result<(), crate::BoxError> {
        (self)(event)
    }
}

impl fmt::Display for CallOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallOutcome::Success => f.write_str("success"),
            CallOutcome::InnerError => f.write_str("inner error"),
            CallOutcome::Canceled => f.write_str("canceled"),
        }
    }
}

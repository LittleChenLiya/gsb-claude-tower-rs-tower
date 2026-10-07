//! [`ObserveCallLayer`] for the [`ObserveCall`](super::ObserveCall) service.

use super::ObserveCall;
use std::fmt;
use tower_layer::Layer;

/// A [`Layer`] that wraps services in [`ObserveCall`] middleware.
///
/// Each service produced by the layer gets its own id allocator; cloning the
/// resulting services shares that service's allocator.
#[derive(Clone)]
pub struct ObserveCallLayer<O> {
    observer: O,
}

impl<O> ObserveCallLayer<O> {
    /// Creates a new [`ObserveCallLayer`] with the given [`CallObserver`](super::CallObserver).
    pub const fn new(observer: O) -> Self {
        Self { observer }
    }
}

impl<S, O> Layer<S> for ObserveCallLayer<O>
where
    O: Clone,
{
    type Service = ObserveCall<S, O>;

    fn layer(&self, inner: S) -> Self::Service {
        ObserveCall::new(inner, self.observer.clone())
    }
}

impl<O> fmt::Debug for ObserveCallLayer<O>
where
    O: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserveCallLayer")
            .field("observer", &self.observer)
            .finish()
    }
}

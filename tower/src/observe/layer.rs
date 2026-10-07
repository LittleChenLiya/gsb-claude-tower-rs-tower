use super::{CallObserver, Observe, Shared};
use std::{fmt, sync::Arc};
use tower_layer::Layer;

/// A [`Layer`] that wraps services with the [`Observe`] middleware, sending
/// call outcomes to a shared [`CallObserver`].
///
/// Every service produced by the layer shares the same observer and the same
/// call-ID sequence (so IDs are unique across the produced services and their
/// clones), while each produced service keeps its own in-flight call set.
///
/// [`Layer`]: tower_layer::Layer
pub struct ObserveLayer<O> {
    shared: Arc<Shared<O>>,
}

impl<O> ObserveLayer<O> {
    /// Creates a new layer reporting call outcomes to `observer`.
    pub fn new(observer: O) -> Self {
        ObserveLayer {
            shared: Arc::new(Shared::new(observer)),
        }
    }
}

impl<O> Clone for ObserveLayer<O> {
    fn clone(&self) -> Self {
        ObserveLayer {
            shared: self.shared.clone(),
        }
    }
}

impl<O: fmt::Debug> fmt::Debug for ObserveLayer<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserveLayer")
            .field("observer", &self.shared.observer)
            .finish()
    }
}

impl<S, O> Layer<S> for ObserveLayer<O>
where
    O: CallObserver,
{
    type Service = Observe<S, O>;

    fn layer(&self, inner: S) -> Self::Service {
        // Every service produced by this layer shares the observer and the ID
        // allocator, but keeps its own in-flight call set.
        Observe::with_shared(inner, self.shared.clone())
    }
}

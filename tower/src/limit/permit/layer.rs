use super::ReadyPermit;
use tower_layer::Layer;

/// [`Layer`] that wraps a service in a [`ReadyPermit`].
///
/// See the [module-level documentation](super) for the permit lifecycle.
#[derive(Debug, Clone)]
pub struct ReadyPermitLayer {
    max: usize,
}

impl ReadyPermitLayer {
    /// Create a new `ReadyPermitLayer` with an internal permit pool of `max`
    /// permits.
    pub const fn new(max: usize) -> Self {
        ReadyPermitLayer { max }
    }
}

impl<S> Layer<S> for ReadyPermitLayer {
    type Service = ReadyPermit<S>;

    fn layer(&self, service: S) -> Self::Service {
        ReadyPermit::new(service, self.max)
    }
}

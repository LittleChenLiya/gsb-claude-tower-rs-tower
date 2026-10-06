use std::sync::Arc;

use super::ReadyPermit;
use tokio::sync::Semaphore;
use tower_layer::Layer;

/// A [`Layer`] that wraps services in a readiness permit, limiting the
/// number of concurrently in-flight requests per service handle.
///
/// See the [module-level documentation](crate::ready_permit) for the permit
/// lifecycle.
#[derive(Debug, Clone)]
pub struct ReadyPermitLayer {
    semaphore: Arc<Semaphore>,
}

impl ReadyPermitLayer {
    /// Create a new readiness permit layer allowing `max` concurrently
    /// in-flight requests.
    pub fn new(max: usize) -> Self {
        Self::with_semaphore(Arc::new(Semaphore::new(max)))
    }

    /// Create a new readiness permit layer from a shared `Arc<Semaphore>`.
    pub fn with_semaphore(semaphore: Arc<Semaphore>) -> Self {
        ReadyPermitLayer { semaphore }
    }
}

impl<S> Layer<S> for ReadyPermitLayer {
    type Service = ReadyPermit<S>;

    fn layer(&self, service: S) -> Self::Service {
        ReadyPermit::with_semaphore(service, self.semaphore.clone())
    }
}

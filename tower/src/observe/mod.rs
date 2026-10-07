//! Middleware that records the terminal state of service calls.
//!
//! This module provides [`ObserveCall`], a [`Service`](crate::Service) wrapper
//! that reports how each call ends:
//!
//! * [`CallOutcome::Success`] when the call future completes successfully,
//! * [`CallOutcome::InnerError`] when it completes with the inner service's
//!   error,
//! * [`CallOutcome::Canceled`] when the future is dropped before completing.
//!
//! Every call that reaches the inner service receives a unique id via
//! [`CallEvent::id`] and is reported exactly once. Readiness errors do not
//! start a call and are not recorded. Errors returned by the observer are
//! ignored and never replace the inner result.
//!
//! # Example
//!
//! ```ignore
//! use tower::observe::{CallEvent, CallOutcome, ObserveCall};
//! use tower::ServiceBuilder;
//!
//! # async fn build<S>(my_service: S)
//! # where S: tower::Service<&'static str, Response = &'static str, Error = tower::BoxError>
//! # {
//! let service = ServiceBuilder::new()
//!     .layer(ObserveCall::layer(|event: CallEvent| {
//!         match event.outcome {
//!             CallOutcome::Success => println!("call {} succeeded", event.id),
//!             CallOutcome::InnerError => println!("call {} failed", event.id),
//!             CallOutcome::Canceled => println!("call {} canceled", event.id),
//!         }
//!         Ok(())
//!     }))
//!     .service(my_service);
//! # let _ = service;
//! # }
//! ```

mod future;
mod layer;
mod observer;
mod service;

pub use self::{
    future::ObserveCallFuture,
    layer::ObserveCallLayer,
    observer::{CallEvent, CallObserver, CallOutcome},
    service::ObserveCall,
};

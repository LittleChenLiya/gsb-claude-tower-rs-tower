//! Retry with an explicit *replay identity*.
//!
//! Unlike [`super::Retry`], which trusts any `Some` returned by
//! [`Policy::clone_request`][super::Policy::clone_request], the [`Replay`]
//! middleware seals a [`ReplayIdentity`] — the method, the target and the
//! [`BodyPolicy`] — from the original request and requires every replay
//! candidate to match it. A clone that silently changes the target is
//! rejected *before* the inner service is called, and a streaming body
//! that cannot be cloned terminates explicitly instead of being retried.
//!
//! The identity never contains headers: secret headers such as
//! `authorization` must not leak into it.
//!
//! The default [`Policy`][super::Policy] trait is left untouched; this
//! module is an opt-in alternative with a stricter contract.
//!
//! # Local acceptance
//!
//! The middleware is exercised by a fake `Service` (in
//! `tower/tests/replay/main.rs`) that counts calls, `poll_ready`
//! permits and response drops:
//!
//! ```text
//! cargo test -p tower --features retry --test replay
//! cargo test -p tower --features retry --doc retry::replay
//! ```
//!
//! # Example: retrying errors a bounded number of times
//!
//! ```
//! use tower::retry::replay::{BodyPolicy, ReplayIdentity, ReplayPolicy};
//! use std::future;
//!
//! struct Attempts(usize);
//!
//! impl<E> ReplayPolicy<http::Request<()>, http::Response<()>, E> for Attempts {
//!     type Future = future::Ready<()>;
//!
//!     fn seal(&mut self, req: &http::Request<()>) -> ReplayIdentity {
//!         // The identity is the method, the target and the body policy
//!         // only. Headers — in particular secret headers such as
//!         // `authorization` — are never part of it.
//!         ReplayIdentity::new(req.method().as_str(), req.uri().path(), BodyPolicy::Cloneable)
//!     }
//!
//!     fn retry(
//!         &mut self,
//!         _req: &mut http::Request<()>,
//!         result: &mut Result<http::Response<()>, E>,
//!     ) -> Option<Self::Future> {
//!         if result.is_err() && self.0 > 0 {
//!             self.0 -= 1;
//!             Some(future::ready(()))
//!         } else {
//!             None
//!         }
//!     }
//!
//!     fn clone_request(&mut self, req: &http::Request<()>) -> Option<http::Request<()>> {
//!         // `Some` alone does not make the replay legal: the clone is
//!         // re-sealed and must match the original identity.
//!         Some(req.clone())
//!     }
//! }
//! ```

use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tower_layer::Layer;
use tower_service::Service;

/// Whether a request body can be replayed after a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BodyPolicy {
    /// The body is buffered or otherwise cloneable and may be replayed.
    Cloneable,
    /// The body is a stream that cannot be cloned. A failure terminates
    /// explicitly: no replay is attempted and the inner service is not
    /// called again.
    Streaming,
}

/// The explicit identity a replayed request must match.
///
/// Sealed from the original request by [`ReplayPolicy::seal`]. It
/// contains exactly the method, the target and the [`BodyPolicy`].
/// Headers are *not* part of the identity — in particular secret
/// headers such as `authorization` never leak into it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReplayIdentity {
    method: String,
    target: String,
    body: BodyPolicy,
}

impl ReplayIdentity {
    /// Seals an identity from its three parts.
    pub fn new(method: impl Into<String>, target: impl Into<String>, body: BodyPolicy) -> Self {
        ReplayIdentity {
            method: method.into(),
            target: target.into(),
            body,
        }
    }

    /// The sealed method.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The sealed target.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// The sealed body policy.
    pub fn body_policy(&self) -> BodyPolicy {
        self.body
    }

    /// Returns `true` if `secret` appears anywhere in the identity.
    ///
    /// The identity is meant to be safe to log and compare; this is a
    /// convenience for asserting that a secret header value did not
    /// leak into it.
    pub fn leaks(&self, secret: &str) -> bool {
        !secret.is_empty() && (self.method.contains(secret) || self.target.contains(secret))
    }
}

/// A retry policy that seals an explicit [`ReplayIdentity`] for every
/// request and only replays clones that still match it.
///
/// # Example: only idempotent requests with cloneable bodies replay
///
/// ```
/// use tower::retry::replay::{BodyPolicy, ReplayIdentity, ReplayPolicy};
/// use std::future;
///
/// struct Req {
///     method: &'static str,
///     target: String,
///     streaming: bool,
/// }
///
/// struct IdempotentGet;
///
/// impl<E> ReplayPolicy<Req, String, E> for IdempotentGet {
///     type Future = future::Ready<()>;
///
///     fn seal(&mut self, req: &Req) -> ReplayIdentity {
///         let body = if req.streaming {
///             BodyPolicy::Streaming
///         } else {
///             BodyPolicy::Cloneable
///         };
///         ReplayIdentity::new(req.method, req.target.as_str(), body)
///     }
///
///     fn retry(&mut self, _req: &mut Req, result: &mut Result<String, E>) -> Option<Self::Future> {
///         if result.is_err() {
///             Some(future::ready(()))
///         } else {
///             None
///         }
///     }
///
///     fn clone_request(&mut self, req: &Req) -> Option<Req> {
///         if req.streaming || req.method != "GET" {
///             // Streaming bodies cannot be cloned and non-GET requests
///             // are not idempotent: terminate instead of replaying.
///             None
///         } else {
///             Some(Req {
///                 method: req.method,
///                 target: req.target.clone(),
///                 streaming: false,
///             })
///         }
///     }
/// }
/// ```
pub trait ReplayPolicy<Req, Res, E> {
    /// The [`Future`] returned by [`ReplayPolicy::retry`].
    type Future: Future<Output = ()>;

    /// Seals the [`ReplayIdentity`] of the original request.
    ///
    /// The identity must contain only the method, the target and the
    /// [`BodyPolicy`]; secret headers must not be included.
    fn seal(&mut self, req: &Req) -> ReplayIdentity;

    /// Checks whether a failed attempt should be retried.
    ///
    /// Same contract as [`Policy::retry`][super::Policy::retry]: return
    /// `None` to stop, or `Some` future to delay the next attempt.
    fn retry(&mut self, req: &mut Req, result: &mut Result<Res, E>) -> Option<Self::Future>;

    /// Tries to clone a request before it is passed to the inner service.
    ///
    /// Returning `None` terminates the retry session. Returning `Some`
    /// is *not* by itself a license to replay: the clone is re-sealed
    /// and must match the identity sealed from the original request,
    /// otherwise the replay is rejected before the inner service is
    /// called.
    fn clone_request(&mut self, req: &Req) -> Option<Req>;
}

// Ensure `ReplayPolicy` is object safe
#[cfg(test)]
fn _obj_safe(_: Box<dyn ReplayPolicy<(), (), (), Future = std::future::Ready<()>>>) {}

pin_project! {
    /// Retries requests only when the replay candidate matches the
    /// [`ReplayIdentity`] sealed from the original request.
    ///
    /// Like [`super::Retry`], both the [`ReplayPolicy`] and the inner
    /// `Service` must implement [`Clone`]; a fresh clone of the policy
    /// is used per request session.
    #[derive(Clone, Debug)]
    pub struct Replay<P, S> {
        policy: P,
        service: S,
    }
}

impl<P, S> Replay<P, S> {
    /// Replays requests through `service` according to `policy`.
    pub const fn new(policy: P, service: S) -> Self {
        Replay { policy, service }
    }

    /// Get a reference to the inner service.
    pub fn get_ref(&self) -> &S {
        &self.service
    }

    /// Get a mutable reference to the inner service.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.service
    }

    /// Consume `self`, returning the inner service.
    pub fn into_inner(self) -> S {
        self.service
    }
}

impl<P, S, Request> Service<Request> for Replay<P, S>
where
    P: ReplayPolicy<Request, S::Response, S::Error> + Clone,
    S: Service<Request> + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<P, S, Request>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let identity = self.policy.seal(&request);
        let cloned = match identity.body_policy() {
            BodyPolicy::Cloneable => self.policy.clone_request(&request),
            // A streaming body cannot be cloned: terminate explicitly
            // instead of attempting a replay.
            BodyPolicy::Streaming => None,
        };
        let future = self.service.call(request);

        ResponseFuture::new(cloned, identity, self.clone(), future)
    }
}

/// Retry requests based on a [`ReplayPolicy`].
#[derive(Debug, Clone)]
pub struct ReplayLayer<P> {
    policy: P,
}

impl<P> ReplayLayer<P> {
    /// Creates a new [`ReplayLayer`] from a replay policy.
    pub const fn new(policy: P) -> Self {
        ReplayLayer { policy }
    }
}

impl<P, S> Layer<S> for ReplayLayer<P>
where
    P: Clone,
{
    type Service = Replay<P, S>;

    fn layer(&self, service: S) -> Self::Service {
        let policy = self.policy.clone();
        Replay::new(policy, service)
    }
}

pin_project! {
    /// The [`Future`] returned by a [`Replay`] service.
    #[derive(Debug)]
    pub struct ResponseFuture<P, S, Request>
    where
        P: ReplayPolicy<Request, S::Response, S::Error>,
        S: Service<Request>,
    {
        request: Option<Request>,
        identity: ReplayIdentity,
        // The result of the previous attempt, owned by this future
        // until it is returned or reclaimed exactly once.
        last: Option<Result<S::Response, S::Error>>,
        #[pin]
        retry: Replay<P, S>,
        #[pin]
        state: State<S::Future, P::Future>,
    }
}

pin_project! {
    #[project = StateProj]
    #[derive(Debug)]
    enum State<F, P> {
        // Polling the future from [`Service::call`]
        Called {
            #[pin]
            future: F
        },
        // Polling the future from [`ReplayPolicy::retry`]
        Waiting {
            #[pin]
            waiting: P
        },
        // Checking the replay identity, then polling
        // [`Service::poll_ready`] after [`Waiting`] was OK.
        Retrying,
    }
}

impl<P, S, Request> ResponseFuture<P, S, Request>
where
    P: ReplayPolicy<Request, S::Response, S::Error>,
    S: Service<Request>,
{
    fn new(
        request: Option<Request>,
        identity: ReplayIdentity,
        retry: Replay<P, S>,
        future: S::Future,
    ) -> ResponseFuture<P, S, Request> {
        ResponseFuture {
            request,
            identity,
            last: None,
            retry,
            state: State::Called { future },
        }
    }
}

impl<P, S, Request> Future for ResponseFuture<P, S, Request>
where
    P: ReplayPolicy<Request, S::Response, S::Error>,
    S: Service<Request>,
{
    type Output = Result<S::Response, S::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProj::Called { future } => {
                    let mut result = ready!(future.poll(cx));
                    // The previous attempt's result is owned by this
                    // future and is reclaimed exactly once, here, before
                    // it is replaced.
                    drop(this.last.take());
                    if let Some(req) = &mut this.request {
                        match this.retry.policy.retry(req, &mut result) {
                            Some(waiting) => {
                                *this.last = Some(result);
                                this.state.set(State::Waiting { waiting });
                            }
                            None => return Poll::Ready(result),
                        }
                    } else {
                        // The request wasn't cloned (or has a streaming
                        // body): terminate explicitly with this result.
                        return Poll::Ready(result);
                    }
                }
                StateProj::Waiting { waiting } => {
                    ready!(waiting.poll(cx));

                    this.state.set(State::Retrying);
                }
                StateProj::Retrying => {
                    // `Some` from `clone_request` is not, by itself, a
                    // license to replay: the candidate must match the
                    // identity sealed from the original request. This is
                    // checked *before* acquiring a permit or calling the
                    // inner service, so a rejected replay never reaches
                    // the lower layer.
                    let replay_identity = {
                        let req = this
                            .request
                            .as_ref()
                            .expect("retrying requires cloned request");
                        this.retry.policy.seal(req)
                    };
                    if replay_identity != *this.identity {
                        let result = this
                            .last
                            .take()
                            .expect("rejected replay returns the last result");
                        return Poll::Ready(result);
                    }
                    ready!(this.retry.as_mut().project().service.poll_ready(cx))?;
                    let req = this
                        .request
                        .take()
                        .expect("retrying requires cloned request");
                    *this.request = this.retry.policy.clone_request(&req);
                    this.state.set(State::Called {
                        future: this.retry.as_mut().project().service.call(req),
                    });
                }
            }
        }
    }
}

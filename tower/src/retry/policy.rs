//! Retry [`Policy`] and the sealed [`ReplayId`] that every replay must match.
//!
//! # Scoped verification commands
//!
//! Run from the repository root with the build cache redirected to the local
//! work drive:
//!
//! ```text
//! # Windows cmd (use `$env:CARGO_TARGET_DIR=...` in PowerShell)
//! set CARGO_TARGET_DIR=D:\Codex\gsb-trials\claude-20261007\tasks\1007-008\evidence\A-003\target
//!
//! # fake-service acceptance tests plus the permit-ownership tests
//! cargo test -p tower --features "retry limit" --test retry
//!
//! # acceptance tests without the `limit` feature (permit tests are gated)
//! cargo test -p tower --features retry --test retry
//!
//! # the two policy examples (idempotent HTTP and one-shot stream)
//! cargo test -p tower --features retry --doc retry::policy
//! ```

use std::fmt;
use std::future::Future;

/// The sealed replay identity of a request.
///
/// Before the first [`Service::call`], [`Retry`] asks the [`Policy`] for the
/// [`Policy::replay_id`] of the request it is about to send and seals that
/// identity. Every later replay is produced by [`Policy::clone_request`] and
/// is only sent to the inner service if its identity is equal to the sealed
/// one. This makes the replay target explicit: a clone that silently changes
/// the method or target is rejected *before* it reaches the inner service,
/// even though `clone_request` returned `Some`.
///
/// The identity intentionally covers only the parts a replay must preserve:
/// the HTTP-like `method`, the `target` (path/authority) and the `body`
/// strategy. It deliberately does **not** include headers such as
/// authorization or other secret headers, which are allowed to differ between
/// attempts (for example a rotated bearer token).
///
/// [`Retry`]: crate::retry::Retry
/// [`Service::call`]: crate::Service::call
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReplayId {
    method: String,
    target: String,
    body: String,
}

impl ReplayId {
    /// Seal a replay identity from its method, target and body strategy.
    ///
    /// `body` describes how the body is replayed, e.g. `"buffered"` for a body
    /// that was fully buffered and can be replayed byte-for-byte, or
    /// `"stream"` for a one-shot stream. Secret headers must never be folded
    /// into any of these fields.
    pub fn new(
        method: impl Into<String>,
        target: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        ReplayId {
            method: method.into(),
            target: target.into(),
            body: body.into(),
        }
    }

    /// The method preserved across replays.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The target (path/authority) preserved across replays.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// The body replay strategy preserved across replays.
    pub fn body(&self) -> &str {
        &self.body
    }
}

impl fmt::Debug for ReplayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplayId")
            .field("method", &self.method)
            .field("target", &self.target)
            .field("body", &self.body)
            .finish()
    }
}

/// A "retry policy" to classify if a request should be retried.
///
/// # Example
///
/// ```
/// use tower::retry::Policy;
/// use std::future;
///
/// type Req = String;
/// type Res = String;
///
/// struct Attempts(usize);
///
/// impl<E> Policy<Req, Res, E> for Attempts {
///     type Future = future::Ready<()>;
///
///     fn retry(&mut self, req: &mut Req, result: &mut Result<Res, E>) -> Option<Self::Future> {
///         match result {
///             Ok(_) => {
///                 // Treat all `Response`s as success,
///                 // so don't retry...
///                 None
///             },
///             Err(_) => {
///                 // Treat all errors as failures...
///                 // But we limit the number of attempts...
///                 if self.0 > 0 {
///                     // Try again!
///                     self.0 -= 1;
///                     Some(future::ready(()))
///                 } else {
///                     // Used all our attempts, no retry...
///                     None
///                 }
///             }
///         }
///     }
///
///     fn clone_request(&mut self, req: &Req) -> Option<Req> {
///         Some(req.clone())
///     }
/// }
/// ```
///
/// # Sealed replay identity
///
/// Returning `Some` from [`clone_request`] is not, on its own, enough to
/// replay a request: the middleware also seals the [`replay_id`] of the
/// request before the first call and compares it against the identity of
/// every cloned request. A replay whose identity does not match the sealed
/// method, target and body strategy is terminated before the inner service is
/// called again; the previous response (usually the last error) is returned.
///
/// [`clone_request`]: Policy::clone_request
/// [`replay_id`]: Policy::replay_id
pub trait Policy<Req, Res, E> {
    /// The [`Future`] type returned by [`Policy::retry`].
    type Future: Future<Output = ()>;

    /// Check the policy if a certain request should be retried.
    ///
    /// This method is passed a reference to the original request, and either
    /// the [`Service::Response`] or [`Service::Error`] from the inner service.
    ///
    /// If the request should **not** be retried, return `None`.
    ///
    /// If the request *should* be retried, return `Some` future that will delay
    /// the next retry of the request. This can be used to sleep for a certain
    /// duration, to wait for some external condition to be met before retrying,
    /// or resolve right away, if the request should be retried immediately.
    ///
    /// ## Mutating Requests
    ///
    /// The policy MAY chose to mutate the `req`: if the request is mutated, the
    /// mutated request will be sent to the inner service in the next retry.
    /// This can be helpful for use cases like tracking the retry count in a
    /// header.
    ///
    /// ## Mutating Results
    ///
    /// The policy MAY chose to mutate the result. This enables the retry
    /// policy to convert a failure into a success and vice versa. For example,
    /// if the policy is used to poll while waiting for a state change, the
    /// policy can switch the result to emit a specific error when retries are
    /// exhausted.
    ///
    /// The policy can also record metadata on the request to include
    /// information about the number of retries required or to record that a
    /// failure failed after exhausting all retries.
    ///
    /// [`Service::Response`]: crate::Service::Response
    /// [`Service::Error`]: crate::Service::Error
    fn retry(&mut self, req: &mut Req, result: &mut Result<Res, E>) -> Option<Self::Future>;

    /// Tries to clone a request before being passed to the inner service.
    ///
    /// If the request cannot be cloned, return [`None`]. Moreover, the retry
    /// function will not be called if the [`None`] is returned.
    ///
    /// Note that `Some` means *"a candidate replay exists"*; it is not itself a
    /// permission to replay. The cloned request is only sent if its
    /// [`Policy::replay_id`] matches the identity sealed before the original
    /// call. A request with an un-cloneable streaming body therefore returns
    /// `None` and terminates the retry, and a clone that changes the method or
    /// target is rejected before reaching the inner service.
    fn clone_request(&mut self, req: &Req) -> Option<Req>;

    /// Seals the replay identity of a request.
    ///
    /// This is called with the original request immediately before it is sent
    /// to the inner service. The returned [`ReplayId`] is sealed for the whole
    /// request session and is compared with the identity of every request
    /// produced by [`Policy::clone_request`]. A replay whose identity differs
    /// is terminated before the inner service is called.
    ///
    /// The identity must describe only the parts a safe replay must preserve:
    /// the method, the target and the body strategy. Rotating or per-attempt
    /// data such as a secret `Authorization` header is not part of the
    /// identity.
    ///
    /// The default returns a wildcard identity, so existing policies that
    /// rely solely on [`Policy::clone_request`] keep their current behavior.
    ///
    /// [`Policy::clone_request`]: Policy::clone_request
    fn replay_id(&mut self, req: &Req) -> ReplayId {
        let _ = req;
        ReplayId::new("*", "*", "*")
    }
}

/// This example policy seals an idempotent HTTP request identity. The secret
/// header is carried by the request and re-attached per attempt, but it is
/// deliberately absent from the sealed identity, so rotating it does not
/// invalidate a replay while changing the method or target does.
///
/// ```
/// use std::future;
/// use tower::retry::{Policy, ReplayId};
///
/// #[derive(Clone)]
/// struct HttpRequest {
///     method: &'static str,
///     target: &'static str,
///     body: &'static str,
///     secret_header: String,
/// }
///
/// #[derive(Clone)]
/// struct IdempotentHttp;
///
/// impl Policy<HttpRequest, (), &'static str> for IdempotentHttp {
///     type Future = future::Ready<()>;
///
///     fn retry(&mut self, _: &mut HttpRequest, _: &mut Result<(), &'static str>)
///         -> Option<Self::Future>
///     {
///         Some(future::ready(()))
///     }
///
///     fn clone_request(&mut self, req: &HttpRequest) -> Option<HttpRequest> {
///         Some(req.clone())
///     }
///
///     fn replay_id(&mut self, req: &HttpRequest) -> ReplayId {
///         // No secret_header: rotated credentials must not break replays.
///         ReplayId::new(req.method, req.target, "buffered")
///     }
/// }
/// ```
#[cfg(doctest)]
pub struct _IdempotentHttpPolicyDoc;

/// This example policy refuses to replay a request whose body is a one-shot
/// stream. Returning `None` from `clone_request` terminates the retry
/// explicitly, so the middleware never replays a half-consumed stream.
///
/// ```
/// use std::future;
/// use tower::retry::Policy;
///
/// #[derive(Clone)]
/// struct StreamRequest {
///     body_buffered: bool,
/// }
///
/// #[derive(Clone)]
/// struct OnceOnlyStream;
///
/// impl Policy<StreamRequest, (), &'static str> for OnceOnlyStream {
///     type Future = future::Ready<()>;
///
///     fn retry(&mut self, _: &mut StreamRequest, _: &mut Result<(), &'static str>)
///         -> Option<Self::Future>
///     {
///         Some(future::ready(()))
///     }
///
///     fn clone_request(&mut self, req: &StreamRequest) -> Option<StreamRequest> {
///         // A live stream cannot be cloned; end the retries instead of
///         // replaying a consumed body.
///         req.body_buffered.then(|| req.clone())
///     }
/// }
/// ```
#[cfg(doctest)]
pub struct _StreamPolicyDoc;

// Ensure `Policy` is object safe for policies that do not override
// `replay_id`; the default method keeps it object safe.
#[cfg(test)]
fn _obj_safe(_: Box<dyn Policy<(), (), (), Future = std::future::Ready<()>>>) {}

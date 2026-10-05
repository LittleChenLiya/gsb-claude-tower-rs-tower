//! Future types

use super::{Policy, ReplayId, Retry};
use pin_project_lite::pin_project;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tower_service::Service;

pin_project! {
    /// The [`Future`] returned by a [`Retry`] service.
    pub struct ResponseFuture<P, S, Request>
    where
        P: Policy<Request, S::Response, S::Error>,
        S: Service<Request>,
    {
        // The request stashed for a possible *next* replay. This is `None`
        // when no further replay is possible (the initial clone failed) and
        // after the last permitted replay has been committed.
        request: Option<Request>,
        // The response produced by the previous inner call. It is carried
        // across the policy backoff so it can be returned when a replay is
        // refused, and dropped exactly once when a replay is committed.
        last: Option<Result<S::Response, S::Error>>,
        // Identity sealed from the original request before the first call.
        sealed: ReplayId,
        #[pin]
        retry: Retry<P, S>,
        #[pin]
        state: State<S::Future, P::Future>,
    }
}

pin_project! {
    #[project = StateProj]
    enum State<F, P> {
        // Polling the future from [`Service::call`]
        Called {
            #[pin]
            future: F
        },
        // Polling the future from [`Policy::retry`]
        Waiting {
            #[pin]
            waiting: P
        },
        // Polling [`Service::poll_ready`] after [`Waiting`] was OK.
        Retrying,
    }
}

// The inner futures need not be `Debug`: render only the variant name so that
// `ResponseFuture` stays `Debug` without adding bounds on `S::Future` or
// `P::Future`.
impl<F, P> fmt::Debug for State<F, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Called { .. } => f.write_str("Called"),
            State::Waiting { .. } => f.write_str("Waiting"),
            State::Retrying => f.write_str("Retrying"),
        }
    }
}

impl<P, S, Request> fmt::Debug for ResponseFuture<P, S, Request>
where
    P: Policy<Request, S::Response, S::Error>,
    S: Service<Request>,
    P: fmt::Debug,
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseFuture")
            .field("request", &self.request.is_some())
            .field("last", &self.last.is_some())
            .field("sealed", &self.sealed)
            .field("retry", &self.retry)
            .field("state", &self.state)
            .finish()
    }
}

impl<P, S, Request> ResponseFuture<P, S, Request>
where
    P: Policy<Request, S::Response, S::Error>,
    S: Service<Request>,
{
    pub(crate) fn new(
        request: Option<Request>,
        retry: Retry<P, S>,
        future: S::Future,
        sealed: ReplayId,
    ) -> ResponseFuture<P, S, Request> {
        ResponseFuture {
            request,
            last: None,
            sealed,
            retry,
            state: State::Called { future },
        }
    }
}

impl<P, S, Request> Future for ResponseFuture<P, S, Request>
where
    P: Policy<Request, S::Response, S::Error>,
    S: Service<Request>,
{
    type Output = Result<S::Response, S::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProj::Called { future } => {
                    let mut result = ready!(future.poll(cx));
                    if let Some(req) = &mut this.request {
                        match this.retry.policy.retry(req, &mut result) {
                            Some(waiting) => {
                                // Carry the previous response across the
                                // backoff. It is owned by this future until a
                                // replay is actually committed, at which point
                                // it is dropped exactly once.
                                *this.last = Some(result);
                                this.state.set(State::Waiting { waiting });
                            }
                            None => return Poll::Ready(result),
                        }
                    } else {
                        // request wasn't cloned, so no way to retry it
                        return Poll::Ready(result);
                    }
                }
                StateProj::Waiting { waiting } => {
                    ready!(waiting.poll(cx));

                    this.state.set(State::Retrying);
                }
                StateProj::Retrying => {
                    // NOTE: we assume here that
                    //
                    //   this.retry.poll_ready()
                    //
                    // is equivalent to
                    //
                    //   this.retry.service.poll_ready()
                    //
                    // we need to make that assumption to avoid adding an Unpin bound to the Policy
                    // in Ready to make it Unpin so that we can get &mut Ready as needed to call
                    // poll_ready on it.

                    // The stashed request is the replay the policy voted for.
                    let replay = this
                        .request
                        .take()
                        .expect("retrying requires cloned request");

                    // Clone the *next* attempt and validate this replay's
                    // identity *before* acquiring a permit or calling the
                    // inner service. A clone that changed the sealed method,
                    // target or body strategy, or a non-cloneable streaming
                    // body, terminates the retry here and never reaches the
                    // lower layer.
                    let next = this.retry.policy.clone_request(&replay);
                    let replay_id = this.retry.policy.replay_id(&replay);
                    let admitted = replay_id == *this.sealed;

                    if !admitted {
                        // Terminate the retry with the response from the
                        // previous (failed) attempt. The rejected candidate is
                        // dropped without ever calling the inner service, and
                        // no permit was acquired. `last` is consumed here
                        // exactly once; a replay can only be considered after
                        // a response has been stored.
                        return Poll::Ready(
                            this.last.take().expect("replay follows a stored response"),
                        );
                    }

                    let service = this.retry.as_mut().project().service;
                    ready!(service.poll_ready(cx))?;

                    // The replay is committed to a real lower-layer call:
                    // release ownership of the previous response exactly once
                    // (its permit/body travels no further).
                    *this.last = None;
                    // Stash the clone for the following attempt; `None`
                    // terminates the chain after this replay.
                    *this.request = next;
                    let future = service.call(replay);
                    this.state.set(State::Called { future });
                }
            }
        }
    }
}

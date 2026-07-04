//
// Copyright (c) 2026 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
//! Async timing primitives that work on both native and `wasm32-unknown-unknown`.
//!
//! On native targets these are thin re-exports of `tokio::time`. On wasm, where
//! Tokio's time driver (and `std::time::Instant`) panic, they are backed by the
//! browser clock (`web-time`) and `setTimeout` so Zenoh's transport can run in a
//! browser.

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub use tokio::time::{error::Elapsed, sleep, sleep_until, timeout, Instant};

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub use wasm::{sleep, sleep_until, timeout, Elapsed, Instant};

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod wasm {
    use std::{
        fmt,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use wasm_bindgen::{JsCast, JsValue};
    pub use web_time::Instant;

    /// Asserts a future is `Send`. Sound on `wasm32-unknown-unknown`, which is
    /// single-threaded, but lets browser-bound (`!Send`) futures satisfy the
    /// `Send` bounds that Zenoh's (Tokio-oriented) transport code requires.
    struct AssertSend<F>(F);
    // SAFETY: wasm32-unknown-unknown has no threads, so nothing is ever actually
    // sent across threads.
    unsafe impl<F> Send for AssertSend<F> {}

    impl<F: Future> Future for AssertSend<F> {
        type Output = F::Output;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            // SAFETY: we never move `self.0` out of the pinned `self`.
            unsafe { self.map_unchecked_mut(|s| &mut s.0) }.poll(cx)
        }
    }

    /// Returned when a [`timeout`] elapses before the wrapped future completes.
    #[derive(Debug, Clone, Copy)]
    pub struct Elapsed;

    impl fmt::Display for Elapsed {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "deadline has elapsed")
        }
    }

    impl std::error::Error for Elapsed {}

    async fn sleep_inner(duration: Duration) {
        let millis = duration.as_millis().min(i32::MAX as u128) as i32;
        let promise = js_sys::Promise::new(&mut |resolve, _reject| {
            let global = js_sys::global();
            let handler: &js_sys::Function = resolve.unchecked_ref();
            if let Some(window) = web_sys::window() {
                let _ =
                    window.set_timeout_with_callback_and_timeout_and_arguments_0(handler, millis);
            } else if let Some(worker) = global.dyn_ref::<web_sys::WorkerGlobalScope>() {
                let _ =
                    worker.set_timeout_with_callback_and_timeout_and_arguments_0(handler, millis);
            } else {
                // No scheduler available; resolve immediately to avoid hanging.
                let _ = resolve.call0(&JsValue::NULL);
            }
        });
        let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
    }

    /// Resolve after `duration`, driven by the browser's `setTimeout`.
    pub fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
        AssertSend(sleep_inner(duration))
    }

    /// Sleep until the given [`Instant`].
    pub fn sleep_until(deadline: Instant) -> impl Future<Output = ()> + Send {
        AssertSend(async move {
            sleep_inner(deadline.saturating_duration_since(Instant::now())).await;
        })
    }

    /// Run `future`, cancelling it (returning [`Elapsed`]) if `duration` passes first.
    pub fn timeout<F: Future>(
        duration: Duration,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Elapsed>> + Send {
        AssertSend(async move {
            use futures_util::future::{select, Either};

            let future = std::pin::pin!(future);
            let deadline = std::pin::pin!(sleep_inner(duration));
            match select(future, deadline).await {
                Either::Left((output, _)) => Ok(output),
                Either::Right(((), _)) => Err(Elapsed),
            }
        })
    }
}

//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! ⚠️ WARNING ⚠️
//!
//! This crate is intended for Zenoh's internal use.
//!
//! [Click here for Zenoh's documentation](https://docs.rs/zenoh/latest/zenoh)
use core::panic;
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
use std::sync::atomic::Ordering;
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
use std::time::Duration;
use std::{
    borrow::Borrow,
    collections::HashMap,
    env, fmt,
    future::Future,
    ops::Deref,
    sync::{atomic::AtomicUsize, OnceLock},
};

use lazy_static::lazy_static;
use serde::Deserialize;
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
use tokio::runtime::RuntimeFlavor;
use tokio::{
    runtime::{Handle, Runtime},
    task::JoinHandle,
};
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
use wasm_bindgen::closure::Closure;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
use wasm_bindgen::JsCast;
use zenoh_macros::{GenericRuntimeParam, RegisterParam};
use zenoh_result::ZResult as Result;

pub mod time;

pub const ZENOH_RUNTIME_ENV: &str = "ZENOH_RUNTIME";

/// Available parameters to configure the ZRuntime.
#[derive(Deserialize, Debug, GenericRuntimeParam)]
#[serde(deny_unknown_fields, default)]
pub struct RuntimeParam {
    /// Number of async worker threads. At least one.
    pub worker_threads: usize,
    /// Number of maximal worker threads for blocking tasks. At least one.
    pub max_blocking_threads: usize,
    /// Hand over one ZRuntime to another one.
    pub handover: Option<ZRuntime>,
}

impl Default for RuntimeParam {
    fn default() -> Self {
        Self {
            worker_threads: 1,
            max_blocking_threads: 50,
            handover: None,
        }
    }
}

impl RuntimeParam {
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    pub fn build(&self, zrt: ZRuntime) -> Result<Runtime> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.worker_threads)
            .max_blocking_threads(self.max_blocking_threads)
            .enable_io()
            .enable_time()
            .thread_name_fn(move || {
                let id = ZRUNTIME_INDEX
                    .get(&zrt)
                    .unwrap()
                    .fetch_add(1, Ordering::SeqCst);
                format!("{zrt}-{id}")
            })
            .build()?;
        Ok(rt)
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    pub fn build(&self, _zrt: ZRuntime) -> Result<Runtime> {
        // NOTE: `.enable_time()` makes Tokio construct its time driver, whose
        // `Driver::new` calls `std::time::Instant::now()` — unsupported on
        // wasm32-unknown-unknown, so it panics/traps at runtime build. Tokio's
        // time driver can't work on this target anyway (it doesn't use the
        // browser clock), so we build without it.
        let rt = tokio::runtime::Builder::new_current_thread().build()?;
        Ok(rt)
    }
}

/// [`ZRuntime`], the access point of manipulate runtimes within zenoh.
/// The runtime parameter can be configured by setting the environmental variable [`ZENOH_RUNTIME_ENV`].
/// The parsing syntax use [RON](https://github.com/ron-rs/ron). An example configuration looks
/// like
///
/// ```console
/// ZENOH_RUNTIME='(
///   rx: (handover: app),
///   acc: (handover: app),
///   app: (worker_threads: 2),
///   tx: (max_blocking_threads: 1)
/// )'
/// ```
/// Note: The runtime parameter takes effect at the beginning of the zenoh process and no longer be
/// changed after the initialization.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug, RegisterParam, Deserialize)]
#[param(RuntimeParam)]
pub enum ZRuntime {
    /// Renamed to app. Default param: worker_threads = 1.
    #[serde(rename = "app")]
    #[param(worker_threads = 1)]
    Application,

    /// Renamed to acc. Default param: worker_threads = 1.
    #[serde(rename = "acc")]
    #[param(worker_threads = 1)]
    Acceptor,

    /// Renamed to tx. Default param: worker_threads = 1.
    #[serde(rename = "tx")]
    #[param(worker_threads = 1)]
    TX,

    /// Renamed to rx. Default param: worker_threads = 2.
    #[serde(rename = "rx")]
    #[param(worker_threads = 2)]
    RX,

    /// Renamed to net. Default param: worker_threads = 1.
    #[serde(rename = "net")]
    #[param(worker_threads = 1)]
    Net,
}

impl ZRuntime {
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        #[cfg(feature = "tracing-instrument")]
        let future = tracing::Instrument::instrument(future, tracing::Span::current());

        self.deref().spawn(future)
    }

    pub fn block_in_place<F, R>(&self, f: F) -> R
    where
        F: Future<Output = R>,
    {
        #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
        {
            match Handle::try_current() {
                Ok(handle) => {
                    if handle.runtime_flavor() == RuntimeFlavor::CurrentThread {
                        panic!("Zenoh runtime doesn't support Tokio's current thread scheduler. Please use multi thread scheduler instead, e.g. a multi thread scheduler with one worker thread: `#[tokio::main(flavor = \"multi_thread\", worker_threads = 1)]`");
                    }
                }
                Err(e) => {
                    if e.is_thread_local_destroyed() {
                        panic!("The Thread Local Storage inside Tokio is destroyed. This might happen when Zenoh API is called at process exit, e.g. in the atexit handler. Calling the Zenoh API at process exit is not supported and should be avoided.");
                    }
                }
            }

            #[cfg(feature = "tracing-instrument")]
            let f = tracing::Instrument::instrument(f, tracing::Span::current());

            tokio::task::block_in_place(move || self.block_on(f))
        }

        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        {
            // wasm32-unknown-unknown is single-threaded and cannot truly block.
            // Many of Zenoh's "blocking" waits are futures that resolve in a
            // single poll (e.g. reads of uncontended async locks). Poll once and
            // return the value if it is ready; only panic if the future would
            // genuinely have to await external progress.
            use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

            const VTABLE: RawWakerVTable = RawWakerVTable::new(
                |_| RawWaker::new(std::ptr::null(), &VTABLE),
                |_| {},
                |_| {},
                |_| {},
            );
            // SAFETY: the no-op waker never dereferences its data pointer.
            let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
            let mut cx = Context::from_waker(&waker);
            let mut f = std::pin::pin!(f);
            match f.as_mut().poll(&mut cx) {
                Poll::Ready(r) => r,
                Poll::Pending => panic!(
                    "Blocking Zenoh waits are not supported on wasm32-unknown-unknown: \
                     this future did not resolve synchronously"
                ),
            }
        }
    }
}

impl Deref for ZRuntime {
    type Target = Handle;
    fn deref(&self) -> &Self::Target {
        ZRUNTIME_POOL.get(self)
    }
}

lazy_static! {
    pub static ref ZRUNTIME_POOL: ZRuntimePool = ZRuntimePool::new();
    pub static ref ZRUNTIME_INDEX: HashMap<ZRuntime, AtomicUsize> = ZRuntime::iter()
        .map(|zrt| (zrt, AtomicUsize::new(0)))
        .collect();
}

// A runtime guard used to explicitly drop the static variables that Rust doesn't drop by default
#[derive(Debug)]
pub struct ZRuntimePoolGuard;

impl Drop for ZRuntimePoolGuard {
    fn drop(&mut self) {
        unsafe {
            std::mem::drop((ZRUNTIME_POOL.deref() as *const ZRuntimePool).read());
            std::mem::drop(
                (ZRUNTIME_INDEX.deref() as *const HashMap<ZRuntime, AtomicUsize>).read(),
            );
        }
    }
}

struct ZRuntimeCell {
    runtime: OnceLock<Runtime>,
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    driver: OnceLock<()>,
}

impl ZRuntimeCell {
    fn new() -> Self {
        Self {
            runtime: OnceLock::new(),
            #[cfg(all(target_family = "wasm", target_os = "unknown"))]
            driver: OnceLock::new(),
        }
    }
}

pub struct ZRuntimePool(HashMap<ZRuntime, ZRuntimeCell>);

impl fmt::Debug for ZRuntimePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let initialized = self
            .0
            .iter()
            .filter_map(|(runtime, cell)| cell.runtime.get().map(|_| runtime))
            .collect::<Vec<_>>();
        f.debug_struct("ZRuntimePool")
            .field("initialized", &initialized)
            .finish_non_exhaustive()
    }
}

impl ZRuntimePool {
    fn new() -> Self {
        Self(
            ZRuntime::iter()
                .map(|zrt| (zrt, ZRuntimeCell::new()))
                .collect(),
        )
    }

    pub fn get(&self, zrt: &ZRuntime) -> &Handle {
        // Although the ZRuntime is called to use `zrt`, it may be handed over to another one
        // specified via the environmental variable.
        let param: &RuntimeParam = zrt.borrow();
        let zrt = match param.handover {
            Some(handover) => handover,
            None => *zrt,
        };

        let cell = self
            .0
            .get(&zrt)
            .unwrap_or_else(|| panic!("The hashmap should contains {zrt} after initialization"));
        let runtime = cell.runtime.get_or_init(|| {
            zrt.init()
                .unwrap_or_else(|_| panic!("Failed to init {zrt}"))
        });

        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        cell.driver
            .get_or_init(|| start_wasm_runtime_driver(runtime as *const Runtime));

        runtime.handle()
    }
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
fn start_wasm_runtime_driver(runtime: *const Runtime) {
    use std::{cell::RefCell, rc::Rc};

    fn schedule(callback: &Closure<dyn FnMut()>) {
        let callback = callback.as_ref().unchecked_ref();
        let global = js_sys::global();

        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(callback, 1);
        } else if let Some(worker) = global.dyn_ref::<web_sys::WorkerGlobalScope>() {
            let _ = worker.set_timeout_with_callback_and_timeout_and_arguments_0(callback, 1);
        } else {
            tracing::warn!(
                "Unable to drive Zenoh's wasm Tokio runtime: no Window or WorkerGlobalScope is available"
            );
        }
    }

    let callback = Rc::new(RefCell::new(None::<Closure<dyn FnMut()>>));
    let callback_for_closure = callback.clone();
    *callback.borrow_mut() = Some(Closure::wrap(Box::new(move || {
        // The runtime is stored in a static OnceLock and is not dropped on wasm.
        let runtime = unsafe { &*runtime };
        // Only drive when this thread isn't already inside the runtime. Otherwise
        // a re-entrant `block_on` panics with "Cannot start a runtime from within
        // a runtime" — the outer `block_on` is already pumping tasks this turn.
        if tokio::runtime::Handle::try_current().is_err() {
            runtime.block_on(async {
                tokio::task::yield_now().await;
            });
        }

        if let Some(callback) = callback_for_closure.as_ref().borrow().as_ref() {
            schedule(callback);
        }
    }) as Box<dyn FnMut()>));

    {
        let callback = callback.as_ref().borrow();
        if let Some(callback) = callback.as_ref() {
            schedule(callback);
        }
    }
}

// If there are any blocking tasks spawned by ZRuntimes, the function will block until they return.
impl Drop for ZRuntimePool {
    fn drop(&mut self) {
        #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
        {
            let handles: Vec<_> = self
                .0
                .drain()
                .filter_map(|(_name, mut cell)| {
                    cell.runtime.take().map(|r| {
                        std::thread::spawn(move || r.shutdown_timeout(Duration::from_secs(1)))
                    })
                })
                .collect();

            for hd in handles {
                let _ = hd.join();
            }
        }
    }
}

#[should_panic(expected = "Zenoh runtime doesn't support")]
#[tokio::test]
async fn block_in_place_fail_test() {
    use crate::ZRuntime;
    ZRuntime::TX.block_in_place(async { println!("Done") });
}

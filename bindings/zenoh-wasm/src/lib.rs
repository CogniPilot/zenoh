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
use std::fmt::Display;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use zenoh::{
    bytes::Encoding, handlers::FifoChannelHandler, pubsub::Subscriber, sample::Sample, Config,
    Session,
};

#[wasm_bindgen(js_name = initPanicHook)]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

/// Route Zenoh's `tracing` output to the browser console (call once). Off by
/// default; enable from JS when diagnosing connection issues in DevTools.
#[cfg(feature = "console-log")]
#[wasm_bindgen(js_name = initLog)]
pub fn init_log() {
    static START: std::sync::Once = std::sync::Once::new();
    START.call_once(|| {
        tracing_wasm::set_as_global_default();
    });
}

#[wasm_bindgen]
pub fn version() -> String {
    option_env!("ZENOH_WASM_NPM_VERSION")
        .unwrap_or(env!("CARGO_PKG_VERSION"))
        .to_owned()
}

#[wasm_bindgen]
pub struct ZenohSession {
    inner: Session,
}

#[wasm_bindgen]
impl ZenohSession {
    #[wasm_bindgen(js_name = putBytes)]
    pub async fn put_bytes(&self, key_expr: String, payload: Vec<u8>) -> Result<(), JsValue> {
        self.inner.put(key_expr, payload).await.map_err(js_error)
    }

    #[wasm_bindgen(js_name = putString)]
    pub async fn put_string(&self, key_expr: String, payload: String) -> Result<(), JsValue> {
        self.inner
            .put(key_expr, payload)
            .encoding(Encoding::TEXT_PLAIN)
            .await
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = isClosed)]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub async fn close(&self) -> Result<(), JsValue> {
        self.inner.close().await.map_err(js_error)
    }

    /// Declare a subscriber on `key_expr`. Every matching sample invokes
    /// `callback(keyExpr: string, payload: Uint8Array, encoding: string)`.
    /// The returned handle keeps the subscription alive until `undeclare()`
    /// is called (or the handle is dropped).
    #[wasm_bindgen(js_name = declareSubscriber)]
    pub async fn declare_subscriber(
        &self,
        key_expr: String,
        callback: js_sys::Function,
    ) -> Result<WasmSubscriber, JsValue> {
        let subscriber = self
            .inner
            .declare_subscriber(key_expr)
            .await
            .map_err(js_error)?;
        let receiver: FifoChannelHandler<Sample> = subscriber.handler().clone();

        spawn_local(async move {
            while let Ok(sample) = receiver.recv_async().await {
                let key = sample.key_expr().as_str().to_string();
                let bytes = sample.payload().to_bytes();
                let payload = js_sys::Uint8Array::from(bytes.as_ref());
                let encoding = sample.encoding().to_string();
                // Stop forwarding once the JS side detaches the callback.
                if callback
                    .call3(
                        &JsValue::NULL,
                        &JsValue::from_str(&key),
                        &payload.into(),
                        &JsValue::from_str(&encoding),
                    )
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(WasmSubscriber {
            inner: Some(subscriber),
        })
    }
}

/// Handle to a live subscription. Dropping it (or calling `undeclare`)
/// tears down the subscription and ends sample delivery.
#[wasm_bindgen]
pub struct WasmSubscriber {
    inner: Option<Subscriber<FifoChannelHandler<Sample>>>,
}

#[wasm_bindgen]
impl WasmSubscriber {
    pub async fn undeclare(&mut self) -> Result<(), JsValue> {
        if let Some(subscriber) = self.inner.take() {
            subscriber.undeclare().await.map_err(js_error)?;
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = keyExpr)]
    pub fn key_expr(&self) -> Option<String> {
        self.inner
            .as_ref()
            .map(|subscriber| subscriber.key_expr().as_str().to_string())
    }
}

#[wasm_bindgen]
pub async fn open(endpoint: String) -> Result<ZenohSession, JsValue> {
    let mut config = Config::default();
    config
        .insert_json5("mode", r#""client""#)
        .map_err(js_error)?;
    config
        .insert_json5(
            "connect/endpoints",
            &format!("[{}]", serde_json::to_string(&endpoint).map_err(js_error)?),
        )
        .map_err(js_error)?;
    open_with_config_inner(config).await
}

#[wasm_bindgen(js_name = openWithConfig)]
pub async fn open_with_config(config_json5: String) -> Result<ZenohSession, JsValue> {
    let config = Config::from_json5(&config_json5).map_err(js_error)?;
    open_with_config_inner(config).await
}

async fn open_with_config_inner(config: Config) -> Result<ZenohSession, JsValue> {
    let inner = zenoh::open(config).await.map_err(js_error)?;
    Ok(ZenohSession { inner })
}

fn js_error(error: impl Display) -> JsValue {
    js_sys::Error::new(&error.to_string()).into()
}

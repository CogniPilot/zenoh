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
use zenoh::{bytes::Encoding, Config, Session};

#[wasm_bindgen(js_name = initPanicHook)]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_owned()
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

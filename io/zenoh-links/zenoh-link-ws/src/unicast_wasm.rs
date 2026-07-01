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
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{fmt, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use web_sys::{BinaryType, CloseEvent, ErrorEvent, Event, MessageEvent, WebSocket};
use zenoh_core::zasynclock;
use zenoh_link_commons::{
    LinkAuthId, LinkManagerUnicastTrait, LinkUnicast, LinkUnicastTrait, NewLinkChannelSender,
};
use zenoh_protocol::{
    core::{EndPoint, Locator, Priority},
    transport::BatchSize,
};
use zenoh_result::{bail, zerror, ZResult};

use super::{WS_DEFAULT_MTU, WS_LOCATOR_PREFIX};

enum WsEvent {
    Binary(Vec<u8>),
    Error(String),
}

pub struct LinkUnicastWs {
    ws: WebSocket,
    url: String,
    src_locator: Locator,
    dst_locator: Locator,
    auth_id: LinkAuthId,
    events: AsyncMutex<flume::Receiver<WsEvent>>,
    opened: flume::Receiver<Result<(), String>>,
    leftovers: AsyncMutex<Option<(Vec<u8>, usize, usize)>>,
    _onopen: Closure<dyn FnMut(Event)>,
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
    _onerror: Closure<dyn FnMut(ErrorEvent)>,
    _onclose: Closure<dyn FnMut(CloseEvent)>,
}

// web_sys handles are bound to the browser event loop. zenoh's link traits require
// Send + Sync, and wasm32-unknown-unknown runs this code on the browser thread.
unsafe impl Send for LinkUnicastWs {}
unsafe impl Sync for LinkUnicastWs {}

impl LinkUnicastWs {
    fn new(endpoint: EndPoint) -> ZResult<Self> {
        let url = endpoint_to_url(&endpoint);
        let ws = WebSocket::new(&url)
            .map_err(|e| zerror!("Can not create WebSocket link to {}: {}", url, js_error(e)))?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let (event_tx, event_rx) = flume::unbounded();
        let (open_tx, open_rx) = flume::bounded(1);

        let onopen_tx = open_tx.clone();
        let onopen = Closure::wrap(Box::new(move |_event: Event| {
            let _ = onopen_tx.try_send(Ok(()));
        }) as Box<dyn FnMut(_)>);
        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));

        let onmessage_tx = event_tx.clone();
        let onmessage = Closure::wrap(Box::new(move |event: MessageEvent| {
            let data = event.data();
            if let Ok(buffer) = data.dyn_into::<js_sys::ArrayBuffer>() {
                let array = js_sys::Uint8Array::new(&buffer);
                let mut bytes = vec![0; array.length() as usize];
                array.copy_to(&mut bytes);
                let _ = onmessage_tx.send(WsEvent::Binary(bytes));
            } else {
                tracing::debug!("Ignoring non-binary message from WebSocket link");
            }
        }) as Box<dyn FnMut(_)>);
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

        let onerror_tx = event_tx.clone();
        let onerror_open_tx = open_tx.clone();
        let onerror = Closure::wrap(Box::new(move |event: ErrorEvent| {
            let message = format!("WebSocket error: {}", event.message());
            let _ = onerror_open_tx.try_send(Err(message.clone()));
            let _ = onerror_tx.send(WsEvent::Error(message));
        }) as Box<dyn FnMut(_)>);
        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        let onclose_tx = event_tx;
        let onclose_open_tx = open_tx;
        let onclose = Closure::wrap(Box::new(move |event: CloseEvent| {
            let message = if event.reason().is_empty() {
                format!("WebSocket closed with code {}", event.code())
            } else {
                format!(
                    "WebSocket closed with code {}: {}",
                    event.code(),
                    event.reason()
                )
            };
            let _ = onclose_open_tx.try_send(Err(message.clone()));
            let _ = onclose_tx.send(WsEvent::Error(message));
        }) as Box<dyn FnMut(_)>);
        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        Ok(Self {
            ws,
            url,
            src_locator: Locator::new(WS_LOCATOR_PREFIX, "0.0.0.0:0", "")?,
            dst_locator: endpoint.to_locator(),
            auth_id: LinkAuthId::Ws,
            events: AsyncMutex::new(event_rx),
            opened: open_rx,
            leftovers: AsyncMutex::new(None),
            _onopen: onopen,
            _onmessage: onmessage,
            _onerror: onerror,
            _onclose: onclose,
        })
    }

    async fn wait_open(&self) -> ZResult<()> {
        match self.ws.ready_state() {
            WebSocket::OPEN => Ok(()),
            WebSocket::CONNECTING => match self.opened.recv_async().await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => bail!("{}", e),
                Err(e) => bail!("WebSocket open wait failed for {}: {}", self.url, e),
            },
            WebSocket::CLOSING => bail!("WebSocket link is closing: {}", self.url),
            WebSocket::CLOSED => bail!("WebSocket link is closed: {}", self.url),
            state => bail!(
                "WebSocket link is in unexpected state {}: {}",
                state,
                self.url
            ),
        }
    }

    async fn recv(&self) -> ZResult<Vec<u8>> {
        let guard = zasynclock!(self.events);
        match guard.recv_async().await {
            Ok(WsEvent::Binary(bytes)) => Ok(bytes),
            Ok(WsEvent::Error(e)) => bail!("{}", e),
            Err(e) => bail!("Error when receiving from WebSocket link {}: {}", self, e),
        }
    }
}

#[async_trait]
impl LinkUnicastTrait for LinkUnicastWs {
    async fn close(&self) -> ZResult<()> {
        self.ws
            .close()
            .map_err(|e| zerror!("WebSocket link shutdown {}: {}", self, js_error(e)).into())
    }

    async fn write(&self, buffer: &[u8], _priority: Option<Priority>) -> ZResult<usize> {
        self.wait_open().await?;
        self.ws
            .send_with_u8_array(buffer)
            .map_err(|e| zerror!("Write error on WebSocket link {}: {}", self, js_error(e)))?;
        Ok(buffer.len())
    }

    async fn write_all(&self, buffer: &[u8], priority: Option<Priority>) -> ZResult<()> {
        let mut written = 0;
        while written < buffer.len() {
            written += self.write(&buffer[written..], priority).await?;
        }
        Ok(())
    }

    async fn read(&self, buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<usize> {
        let mut leftovers_guard = zasynclock!(self.leftovers);

        let (slice, start, len) = loop {
            if let Some(tuple) = leftovers_guard.take() {
                break tuple;
            } else {
                let ws_bytes = self.recv().await?;
                let ws_size = ws_bytes.len();
                if ws_size == 0 {
                    tracing::debug!("Ignoring empty binary message from WebSocket link {}", self);
                    continue;
                }
                break (ws_bytes, 0, ws_size);
            }
        };

        let len_min = (len - start).min(buffer.len());
        let end = start + len_min;
        buffer[..len_min].copy_from_slice(&slice[start..end]);
        if end < len {
            *leftovers_guard = Some((slice, end, len));
        }
        Ok(len_min)
    }

    async fn read_exact(&self, buffer: &mut [u8], priority: Option<Priority>) -> ZResult<()> {
        let mut read = 0;
        while read < buffer.len() {
            let n = self.read(&mut buffer[read..], priority).await?;
            if n == 0 {
                bail!(
                    "WebSocket link returned no bytes while reading from {}",
                    self
                );
            }
            read += n;
        }
        Ok(())
    }

    fn get_mtu(&self) -> BatchSize {
        *WS_DEFAULT_MTU
    }

    fn get_src(&self) -> &Locator {
        &self.src_locator
    }

    fn get_dst(&self) -> &Locator {
        &self.dst_locator
    }

    fn is_reliable(&self) -> bool {
        super::IS_RELIABLE
    }

    fn is_streamed(&self) -> bool {
        false
    }

    fn get_interface_names(&self) -> Vec<String> {
        vec![]
    }

    fn get_auth_id(&self) -> &LinkAuthId {
        &self.auth_id
    }
}

impl Drop for LinkUnicastWs {
    fn drop(&mut self) {
        self.ws.set_onopen(None);
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        let _ = self.ws.close();
    }
}

impl fmt::Display for LinkUnicastWs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {}", self.src_locator, self.dst_locator)
    }
}

impl fmt::Debug for LinkUnicastWs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocket")
            .field("url", &self.url)
            .field("src", &self.src_locator)
            .field("dst", &self.dst_locator)
            .finish()
    }
}

#[derive(Debug)]
pub struct LinkManagerUnicastWs {
    _manager: NewLinkChannelSender,
}

impl LinkManagerUnicastWs {
    pub fn new(manager: NewLinkChannelSender) -> Self {
        Self { _manager: manager }
    }
}

#[async_trait]
impl LinkManagerUnicastTrait for LinkManagerUnicastWs {
    async fn new_link(&self, endpoint: EndPoint) -> ZResult<LinkUnicast> {
        let link = Arc::new(LinkUnicastWs::new(endpoint)?);
        link.wait_open().await?;
        Ok(LinkUnicast::from(link as Arc<dyn LinkUnicastTrait>))
    }

    async fn new_listener(&self, endpoint: EndPoint) -> ZResult<Locator> {
        bail!(
            "WebSocket listeners are not available in browser wasm targets: {}",
            endpoint
        )
    }

    async fn del_listener(&self, endpoint: &EndPoint) -> ZResult<()> {
        bail!(
            "WebSocket listeners are not available in browser wasm targets: {}",
            endpoint
        )
    }

    async fn get_listeners(&self) -> Vec<EndPoint> {
        vec![]
    }

    async fn get_locators(&self) -> Vec<Locator> {
        vec![]
    }
}

fn endpoint_to_url(endpoint: &EndPoint) -> String {
    let address = endpoint.address().as_str();
    if address.starts_with("ws://") || address.starts_with("wss://") {
        address.to_string()
    } else {
        format!("{}://{}", WS_LOCATOR_PREFIX, address)
    }
}

fn js_error(value: JsValue) -> String {
    value.as_string().unwrap_or_else(|| format!("{value:?}"))
}

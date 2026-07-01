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
use std::{collections::HashMap, ops::DerefMut, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;
use zenoh_config::{
    get_global_connect_timeout, unwrap_or_default, ConnectionRetryPeriod, ModeDependent,
};
use zenoh_link::{EndPoint, Locator};
use zenoh_protocol::core::{EndPoints, LocatorsStrategy, WhatAmI, ZenohIdProto};
use zenoh_result::{bail, zerror, ZResult};

use super::{Runtime, RuntimeSession};
use crate::net::protocol::linkstate::LinkInfo;

#[derive(Default, Debug)]
pub(crate) struct PeerConnector {
    zid: Option<ZenohIdProto>,
    terminated: bool,
}

#[derive(Default, Debug)]
pub(crate) struct StartConditions {
    notify: Notify,
    peer_connectors: Mutex<Vec<PeerConnector>>,
}

impl StartConditions {
    pub(crate) async fn add_peer_connector(&self) -> usize {
        let mut peer_connectors = self.peer_connectors.lock().await;
        peer_connectors.push(PeerConnector::default());
        peer_connectors.len() - 1
    }

    pub(crate) async fn add_peer_connector_zid(&self, zid: ZenohIdProto) {
        let mut peer_connectors = self.peer_connectors.lock().await;
        if !peer_connectors.iter().any(|pc| pc.zid == Some(zid)) {
            peer_connectors.push(PeerConnector {
                zid: Some(zid),
                terminated: false,
            })
        }
    }

    pub(crate) async fn set_peer_connector_zid(&self, idx: usize, zid: ZenohIdProto) {
        let mut peer_connectors = self.peer_connectors.lock().await;
        if let Some(peer_connector) = peer_connectors.get_mut(idx) {
            peer_connector.zid = Some(zid);
        }
    }

    pub(crate) async fn terminate_peer_connector(&self, idx: usize) {
        let mut peer_connectors = self.peer_connectors.lock().await;
        if let Some(peer_connector) = peer_connectors.get_mut(idx) {
            peer_connector.terminated = true;
        }
        if peer_connectors.iter().all(|pc| pc.terminated) {
            self.notify.notify_one()
        }
    }

    pub(crate) async fn terminate_peer_connector_zid(&self, zid: ZenohIdProto) {
        let mut peer_connectors = self.peer_connectors.lock().await;
        if let Some(peer_connector) = peer_connectors.iter_mut().find(|pc| pc.zid == Some(zid)) {
            peer_connector.terminated = true;
        } else {
            peer_connectors.push(PeerConnector {
                zid: Some(zid),
                terminated: true,
            })
        }
        if peer_connectors.iter().all(|pc| pc.terminated) {
            self.notify.notify_one()
        }
    }
}

impl Runtime {
    fn warn_if_oneof(peer_group: &EndPoints) {
        if let EndPoints::Locators(group) = peer_group {
            if matches!(group.strategy, LocatorsStrategy::OneOf) {
                tracing::warn!(
                    "connect.endpoints locator groups with strategy=oneOf are not implemented yet; \
                     falling back to current allOf behavior"
                );
            }
        }
    }

    pub async fn start(&mut self) -> ZResult<()> {
        match self.whatami() {
            WhatAmI::Client => self.start_client().await,
            WhatAmI::Peer => self.start_peer().await,
            WhatAmI::Router => self.start_router().await,
        }
    }

    async fn start_client(&self) -> ZResult<()> {
        let (listeners, peers, scouting) = {
            let guard = &self.state.config.lock();
            (
                guard
                    .listen()
                    .endpoints()
                    .client()
                    .unwrap_or(&vec![])
                    .clone(),
                guard
                    .connect()
                    .endpoints()
                    .client()
                    .unwrap_or(&vec![])
                    .clone(),
                unwrap_or_default!(guard.scouting().multicast().enabled()),
            )
        };

        Self::warn_listeners(&listeners);
        Self::warn_scouting(scouting);

        if peers.is_empty() {
            bail!(
                "No peer specified and multicast scouting is not available on wasm32-unknown-unknown"
            );
        }
        self.connect_peers(&peers, true).await
    }

    async fn start_peer(&self) -> ZResult<()> {
        let (listeners, peers, scouting, wait_scouting, delay) = {
            let guard = &self.state.config.lock();
            (
                guard.listen().endpoints().peer().unwrap_or(&vec![]).clone(),
                guard
                    .connect()
                    .endpoints()
                    .peer()
                    .unwrap_or(&vec![])
                    .clone(),
                unwrap_or_default!(guard.scouting().multicast().enabled()),
                unwrap_or_default!(guard.open().return_conditions().connect_scouted()),
                std::time::Duration::from_millis(unwrap_or_default!(guard.scouting().delay())),
            )
        };

        Self::warn_listeners(&listeners);
        Self::warn_scouting(scouting);
        self.connect_peers(&peers, false).await?;

        if wait_scouting && !peers.is_empty() {
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    async fn start_router(&self) -> ZResult<()> {
        let (listeners, peers, scouting, delay) = {
            let guard = &self.state.config.lock();
            (
                guard
                    .listen()
                    .endpoints()
                    .router()
                    .unwrap_or(&vec![])
                    .clone(),
                guard
                    .connect()
                    .endpoints()
                    .router()
                    .unwrap_or(&vec![])
                    .clone(),
                unwrap_or_default!(guard.scouting().multicast().enabled()),
                std::time::Duration::from_millis(unwrap_or_default!(guard.scouting().delay())),
            )
        };

        Self::warn_listeners(&listeners);
        Self::warn_scouting(scouting);
        self.connect_peers(&peers, false).await?;

        tokio::time::sleep(delay).await;
        Ok(())
    }

    fn warn_listeners(listeners: &[EndPoint]) {
        if !listeners.is_empty() {
            tracing::warn!(
                "listen endpoints are not available on wasm32-unknown-unknown and will be ignored: {:?}",
                listeners
            );
        }
    }

    fn warn_scouting(scouting: bool) {
        if scouting {
            tracing::warn!(
                "multicast scouting is not available on wasm32-unknown-unknown; configure explicit ws connect endpoints"
            );
        }
    }

    async fn connect_peers(&self, peers: &[EndPoints], single_link: bool) -> ZResult<()> {
        let timeout = self.get_global_connect_timeout();
        if timeout.is_zero() {
            self.connect_peers_impl(peers, single_link).await
        } else {
            match tokio::time::timeout(timeout, self.connect_peers_impl(peers, single_link)).await {
                Ok(r) => r,
                Err(_) => {
                    let e = zerror!("Unable to connect to any of {:?}. Timeout!", peers);
                    tracing::warn!("{}", &e);
                    Err(e.into())
                }
            }
        }
    }

    async fn connect_peers_impl(&self, peers: &[EndPoints], single_link: bool) -> ZResult<()> {
        if single_link {
            self.connect_peers_single_link(peers).await
        } else {
            self.connect_peers_multiply_links(peers).await
        }
    }

    async fn connect_peers_single_link(&self, peers: &[EndPoints]) -> ZResult<()> {
        let mut success_flag = false;
        for peer_group in peers {
            Self::warn_if_oneof(peer_group);
            let mut peers_to_retry = Vec::new();
            for peer in peer_group.as_vec() {
                let endpoint = peer.clone();
                let retry_config = self.get_connect_retry_config(&endpoint);
                tracing::debug!(
                    "Try to connect: {:?}: global timeout: {:?}, retry: {:?}",
                    endpoint,
                    self.get_global_connect_timeout(),
                    retry_config
                );
                if retry_config.timeout().is_zero() || self.get_global_connect_timeout().is_zero() {
                    if self.peer_connector(endpoint).await.is_ok() {
                        success_flag = true;
                    }
                } else {
                    peers_to_retry.push(endpoint);
                }
            }
            if self
                .peers_connector_retry(peers_to_retry, false)
                .await
                .is_ok()
            {
                success_flag = true;
            }
            if success_flag {
                break;
            }
        }

        if success_flag {
            Ok(())
        } else {
            let e = zerror!("Unable to connect to any of {:?}! ", peers);
            tracing::warn!("{}", &e);
            Err(e.into())
        }
    }

    async fn connect_peers_multiply_links(&self, peers: &[EndPoints]) -> ZResult<()> {
        for peer_group in peers {
            Self::warn_if_oneof(peer_group);
            for peer in peer_group.as_vec() {
                let endpoint = peer.clone();
                let retry_config = self.get_connect_retry_config(&endpoint);
                tracing::debug!(
                    "Try to connect: {:?}: global timeout: {:?}, retry: {:?}",
                    endpoint,
                    self.get_global_connect_timeout(),
                    retry_config
                );
                if retry_config.timeout().is_zero() || self.get_global_connect_timeout().is_zero() {
                    if let Err(e) = self.peer_connector(endpoint).await {
                        if retry_config.exit_on_failure {
                            return Err(e);
                        }
                    }
                } else if retry_config.exit_on_failure {
                    let _ = self.peer_connector_retry(endpoint).await?;
                } else if let Err(e) = self.spawn_peer_connector(endpoint.clone()).await {
                    tracing::warn!("Error connecting to {}: {}", endpoint, e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn peer_connector(&self, peer: EndPoint) -> ZResult<()> {
        let result = self
            .manager()
            .open_transport_unicast(peer.clone())
            .await
            .and_then(|transport| -> ZResult<_> {
                let cb = transport
                    .get_callback()?
                    .ok_or_else(|| zerror!("Transport closed immediately"))?;
                let session = cb
                    .as_any()
                    .downcast_ref::<RuntimeSession>()
                    .ok_or_else(|| zerror!("Unexpected callback type"))?;
                zwrite!(session.endpoints).insert(peer.clone());
                Ok(())
            });

        if let Err(e) = &result {
            tracing::warn!("Unable to connect to {}! {}", peer, e);
        }
        result
    }

    fn get_connect_retry_config(&self, endpoint: &EndPoint) -> zenoh_config::ConnectionRetryConf {
        let guard = &self.state.config.lock();
        zenoh_config::get_retry_config(guard, Some(endpoint), false)
    }

    fn get_global_connect_timeout(&self) -> std::time::Duration {
        let guard = &self.state.config.lock();
        get_global_connect_timeout(guard)
    }

    async fn spawn_peer_connector(&self, peer: EndPoint) -> ZResult<()> {
        let this = self.clone();
        let idx = self.state.start_conditions.add_peer_connector().await;
        let config_guard = this.config().lock();
        let config = &config_guard;
        let gossip = unwrap_or_default!(config.scouting().gossip().enabled());
        let wait_declares = unwrap_or_default!(config.open().return_conditions().declares());
        drop(config_guard);
        self.spawn(async move {
            if let Ok(zid) = this.peer_connector_retry(peer).await {
                this.state
                    .start_conditions
                    .set_peer_connector_zid(idx, zid)
                    .await;
            }
            if !gossip && (!wait_declares || this.whatami() != WhatAmI::Peer) {
                this.state
                    .start_conditions
                    .terminate_peer_connector(idx)
                    .await;
            }
        });
        Ok(())
    }

    async fn peers_connector_retry(
        &self,
        peers: Vec<EndPoint>,
        stop_after_first_connection: bool,
    ) -> ZResult<Vec<ZenohIdProto>> {
        async fn wait_next_peer_retry(
            peer: EndPoint,
            period: ConnectionRetryPeriod,
            wait_time: Duration,
            cancellation_token: CancellationToken,
        ) -> Option<(EndPoint, ConnectionRetryPeriod)> {
            tokio::select! {
                _ = tokio::time::sleep(wait_time) => {
                    Some((peer, period))
                }
                _ = cancellation_token.cancelled() => {
                    None
                }
            }
        }

        let mut connected_peers = Vec::new();
        let mut tasks = FuturesUnordered::new();
        let cancellation_token = self.get_cancellation_token();

        for peer in peers {
            let retry_config = self.get_connect_retry_config(&peer);
            let period = retry_config.period();
            tasks.push(wait_next_peer_retry(
                peer,
                period,
                Duration::ZERO,
                cancellation_token.clone(),
            ));
        }

        while let Some(task) = tasks.next().await {
            if let Some((peer, mut period)) = task {
                tracing::debug!(
                    "Try to connect: {:?}: global timeout: {:?}, retry: {:?}",
                    peer,
                    self.get_global_connect_timeout(),
                    self.get_connect_retry_config(&peer)
                );
                let result = self
                    .manager()
                    .open_transport_unicast(peer.clone())
                    .await
                    .and_then(|transport| -> ZResult<_> {
                        let zid = transport.get_zid()?;
                        let cb = transport
                            .get_callback()?
                            .ok_or_else(|| zerror!("Transport closed immediately"))?;
                        let session = cb
                            .as_any()
                            .downcast_ref::<RuntimeSession>()
                            .ok_or_else(|| zerror!("Unexpected callback type"))?;
                        zwrite!(session.endpoints).insert(peer.clone());
                        Ok(zid)
                    });

                match result {
                    Ok(zid) => {
                        tracing::debug!("Successfully connected to configured peer {}", peer);
                        connected_peers.push(zid);
                        if stop_after_first_connection {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Unable to connect to configured peer {}! {}. Retry in {:?}.",
                            peer,
                            e,
                            period.duration()
                        );
                        let wait_time = period.next_duration();
                        tasks.push(wait_next_peer_retry(
                            peer,
                            period,
                            wait_time,
                            cancellation_token.clone(),
                        ));
                    }
                }
            }
        }

        if connected_peers.is_empty() {
            bail!("Peer connector terminated without connecting to any endpoint")
        } else {
            Ok(connected_peers)
        }
    }

    async fn peer_connector_retry(&self, peer: EndPoint) -> ZResult<ZenohIdProto> {
        self.peers_connector_retry(vec![peer], true)
            .await
            .map(|peers| peers[0])
    }

    pub async fn connect_peer(&self, zid: &ZenohIdProto, locators: &[Locator]) -> bool {
        let manager = self.manager();
        if zid == &manager.zid() || manager.get_transport_unicast(zid).await.is_some() {
            return true;
        }

        tracing::debug!("Try to connect to peer {} via any of {:?}", zid, locators);
        for locator in locators {
            let endpoint = locator.to_owned().into();
            match manager.open_transport_unicast_with_zid(endpoint, zid).await {
                Ok(transport) => {
                    tracing::debug!(
                        "Successfully connected to newly discovered peer: {:?}",
                        transport
                    );
                    return true;
                }
                Err(e) => {
                    tracing::trace!("Unable to connect to peer {} on {}: {}", zid, locator, e)
                }
            }
        }

        tracing::warn!(
            "Unable to connect to any locator of discovered peer {}: {:?}",
            zid,
            locators
        );
        false
    }

    pub(super) fn closed_session(session: &RuntimeSession) {
        if session.runtime.is_closed() {
            return;
        }

        if zread!(session.endpoints).is_empty() {
            return;
        }
        let endpoints = session
            .runtime
            .state
            .config
            .lock()
            .connect()
            .endpoints()
            .get(session.runtime.state.whatami)
            .unwrap_or(&vec![])
            .clone();
        let mut peers = vec![];
        for peer in endpoints {
            peers.extend(peer.flatten());
        }

        if session.runtime.whatami() != WhatAmI::Client {
            let endpoints = std::mem::take(zwrite!(session.endpoints).deref_mut());
            peers.retain(|p| endpoints.contains(p));
        }

        if !peers.is_empty() {
            let runtime = session.runtime.clone();
            session.runtime.spawn(async move {
                runtime
                    .peers_connector_retry(peers, runtime.whatami() == WhatAmI::Client)
                    .await
            });
        }
    }

    pub(super) fn closed_link(session: &RuntimeSession, endpoint: EndPoint) {
        if session.runtime.whatami() == WhatAmI::Client {
            return;
        }
        if session.runtime.is_closed() {
            return;
        }
        let endpoints = session
            .runtime
            .state
            .config
            .lock()
            .connect()
            .endpoints()
            .get(session.runtime.state.whatami)
            .unwrap_or(&vec![])
            .clone();
        let mut peers = vec![];
        for peer in endpoints {
            peers.extend(peer.flatten());
        }

        if peers.contains(&endpoint) && zwrite!(session.endpoints).remove(&endpoint) {
            let runtime = session.runtime.clone();
            session.runtime.spawn(async move {
                let _ = runtime.peer_connector_retry(endpoint).await;
            });
        }
    }

    pub(crate) fn get_links_info(&self) -> HashMap<ZenohIdProto, LinkInfo> {
        let _ = self;
        HashMap::new()
    }
}

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
use zenoh_result::{bail, ZResult};
use zenoh_util::LibLoader;

use crate::*;

/// This enum contains information where to load the plugin from.
pub enum DynamicPluginSource {
    /// Load plugin with the name in LibLoader's search paths.
    ByName((LibLoader, String)),
    /// Load first available plugin from the list of paths to plugin files.
    ByPaths(Vec<String>),
}

pub struct DynamicPlugin<StartArgs, Instance> {
    name: String,
    id: String,
    required: bool,
    report: PluginReport,
    source: DynamicPluginSource,
    _marker: std::marker::PhantomData<fn() -> (StartArgs, Instance)>,
}

impl<StartArgs, Instance> DynamicPlugin<StartArgs, Instance> {
    pub fn new(name: String, id: String, source: DynamicPluginSource, required: bool) -> Self {
        Self {
            name,
            id,
            required,
            report: PluginReport::new(),
            source,
            _marker: std::marker::PhantomData,
        }
    }

    fn source_name(&self) -> &str {
        match &self.source {
            DynamicPluginSource::ByName((_loader, name)) => name,
            DynamicPluginSource::ByPaths(paths) => paths.first().map(String::as_str).unwrap_or(""),
        }
    }
}

impl<StartArgs: PluginStartArgs, Instance: PluginInstance> PluginStatus
    for DynamicPlugin<StartArgs, Instance>
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn id(&self) -> &str {
        self.id.as_str()
    }

    fn version(&self) -> Option<&str> {
        None
    }

    fn long_version(&self) -> Option<&str> {
        None
    }

    fn path(&self) -> &str {
        "__dynamic_loading_unsupported__"
    }

    fn state(&self) -> PluginState {
        PluginState::Declared
    }

    fn report(&self) -> PluginReport {
        self.report.clone()
    }
}

impl<StartArgs: PluginStartArgs, Instance: PluginInstance> DeclaredPlugin<StartArgs, Instance>
    for DynamicPlugin<StartArgs, Instance>
{
    fn as_status(&self) -> &dyn PluginStatus {
        self
    }

    fn load(&mut self) -> ZResult<Option<&mut dyn LoadedPlugin<StartArgs, Instance>>> {
        let msg = format!(
            "Dynamic plugin loading is not available on wasm32-unknown-unknown: {}",
            self.source_name()
        );
        self.report.add_error(msg.clone());
        if self.required {
            bail!("{msg}");
        }
        tracing::warn!("{msg}");
        Ok(None)
    }

    fn loaded(&self) -> Option<&dyn LoadedPlugin<StartArgs, Instance>> {
        None
    }

    fn loaded_mut(&mut self) -> Option<&mut dyn LoadedPlugin<StartArgs, Instance>> {
        None
    }
}

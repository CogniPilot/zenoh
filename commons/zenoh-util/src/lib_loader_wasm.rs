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
use std::path::PathBuf;

use crate::LibSearchDirs;

/// Browser wasm cannot load native dynamic libraries.
#[derive(Clone, Debug)]
pub struct LibLoader {
    search_paths: Option<Vec<PathBuf>>,
}

impl LibLoader {
    pub fn empty() -> LibLoader {
        LibLoader { search_paths: None }
    }

    pub fn new(dirs: LibSearchDirs) -> LibLoader {
        let search_paths = dirs
            .into_iter()
            .filter_map(|path| match path {
                Ok(path) => Some(path),
                Err(err) => {
                    tracing::error!("{err}");
                    None
                }
            })
            .collect();

        LibLoader {
            search_paths: Some(search_paths),
        }
    }

    pub fn search_paths(&self) -> Option<&[PathBuf]> {
        self.search_paths.as_deref()
    }

    pub fn _plugin_name(_path: &std::path::Path) -> Option<&str> {
        None
    }

    pub fn plugin_name<P>(_path: &P) -> Option<&str>
    where
        P: AsRef<std::path::Path>,
    {
        None
    }
}

impl Default for LibLoader {
    fn default() -> Self {
        LibLoader::new(LibSearchDirs::default())
    }
}

// Copyright 2020 <盏一 w@hidva.com>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use crate::guc::{self, GucState};
use crate::utils::{SessionState, WorkerState};
use lru::LruCache;
use std::cell::RefCell;
use std::fs::{File, OpenOptions};

type FDCacheT = LruCache<String, File>;

thread_local! {
    static FDCACHE: RefCell<FDCacheT> = RefCell::new(LruCache::new(32));
}

fn do_use_file<T>(
    cache: &mut FDCacheT,
    path: &String,
    act: impl FnOnce(&File) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    if let Some(file) = cache.get(path) {
        return act(file);
    }
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let ret = act(&file);
    cache.put(path.clone(), file);
    return ret;
}

pub fn use_file<T>(
    path: &String,
    act: impl FnOnce(&File) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    FDCACHE.with(|fdcache| {
        let cache = &mut fdcache.borrow_mut();
        return do_use_file(cache, path, act);
    })
}

fn do_resize_fdcache(gucstate: &GucState) {
    // max_files_per_process
    let newsize = guc::get_int(gucstate, guc::MaxFilesPerProcess) as usize;
    FDCACHE.with(|fdcache| {
        let cache = &mut fdcache.borrow_mut();
        cache.resize(newsize);
    })
}

pub trait SessionExt {
    fn resize_fdcache(&self);
}

impl SessionExt for SessionState {
    fn resize_fdcache(&self) {
        do_resize_fdcache(&self.gucstate);
    }
}

pub trait WorkerExt {
    fn resize_fdcache(&self);
}

impl WorkerExt for WorkerState {
    fn resize_fdcache(&self) {
        do_resize_fdcache(&self.gucstate);
    }
}

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
use crate::access::slru;
use crate::guc;
use crate::guc::GucState;
use crate::utils::Xid;
use crate::utils::{SessionState, WorkerState};
use crate::KB_BLCKSZ;
use anyhow;
use lru::LruCache;
use std::cell::RefCell;

pub struct GlobalStateExt {
    d: slru::Slru,
}

impl GlobalStateExt {
    fn new(l2cache_size: usize) -> GlobalStateExt {
        GlobalStateExt {
            d: slru::Slru::new(l2cache_size, "kb_xact"),
        }
    }
}

pub fn init(gucstate: &GucState) -> GlobalStateExt {
    let clog_l2cache_size = guc::get_int(gucstate, guc::ClogL2cacheSize) as usize;
    GlobalStateExt::new(clog_l2cache_size)
}

#[derive(Copy, Clone)]
pub struct WorkerStateExt {
    g: &'static GlobalStateExt,
}

type L1CacheT = LruCache<Xid, XidStatus>;

thread_local! {
    static L1CACHE: RefCell<L1CacheT> = RefCell::new(LruCache::new(32));
}

fn resize_l1cache(gucstate: &GucState) {
    let newsize = guc::get_int(gucstate, guc::ClogL1cacheSize) as usize;
    L1CACHE.with(|l1cache| {
        let cache = &mut l1cache.borrow_mut();
        cache.resize(newsize);
    })
}

fn u8_get_xid_status(byteval: u8, bidx: u64) -> XidStatus {
    let bshift = bidx * BITS_PER_XACT;
    ((byteval >> bshift) & XACT_BITMASK).into()
}

fn get_xid_status(buff: &[u8], byteno: usize, bidx: u64) -> XidStatus {
    u8_get_xid_status(buff[byteno], bidx)
}

impl WorkerStateExt {
    pub fn new(g: &'static GlobalStateExt) -> WorkerStateExt {
        WorkerStateExt { g }
    }

    pub fn set_xid_status(&self, xid: Xid, status: XidStatus) -> anyhow::Result<()> {
        let xid = xid.get();
        let byteno = xid_to_byte(xid);
        let bidx = xid_to_bidx(xid);
        let bshift = bidx * BITS_PER_XACT;
        let andbits = !(XACT_BITMASK << bshift);
        let orbits = (status as u8) << bshift;
        // Add group commit like TransactionGroupUpdateXidStatus
        self.g.d.writable_load(xid_to_pageno(xid), |buff| {
            buff[byteno] = (buff[byteno] & andbits) | orbits;
        })
    }

    fn get_xid_status(&self, cache: &mut L1CacheT, xid: Xid) -> anyhow::Result<XidStatus> {
        if let Some(&v) = cache.get(&xid) {
            return Ok(v);
        }
        let pageno = xid_to_pageno(xid.get());
        let xidstatus = self.g.d.try_readonly_load(pageno, |buff| -> XidStatus {
            let byteno = xid_to_byte(xid.get());
            return get_xid_status(buff, byteno, xid_to_bidx(xid.get()));
        })?;
        if xidstatus != XidStatus::InProgress {
            cache.put(xid, xidstatus);
        }
        return Ok(xidstatus);
    }

    pub fn xid_status(&self, xid: Xid) -> anyhow::Result<XidStatus> {
        L1CACHE.with(|l1cache| -> anyhow::Result<XidStatus> {
            let cache = &mut l1cache.borrow_mut();
            self.get_xid_status(cache, xid)
        })
    }
}

#[repr(u8)]
#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum XidStatus {
    InProgress = 0x00,
    Committed,
    Aborted,
}

impl std::convert::From<u8> for XidStatus {
    fn from(val: u8) -> Self {
        match val {
            0 => XidStatus::InProgress,
            1 => XidStatus::Committed,
            2 => XidStatus::Aborted,
            _ => panic!("u8 -> XidStatus failed. val={}", val),
        }
    }
}

const BITS_PER_XACT: u64 = 2;
pub const XACTS_PER_BYTE: u64 = 4;
const XACTS_PER_PAGE: u64 = KB_BLCKSZ as u64 * XACTS_PER_BYTE;
const XACT_BITMASK: u8 = (1 << BITS_PER_XACT) - 1;

const fn xid_to_pageno(xid: u64) -> slru::Pageno {
    xid / XACTS_PER_PAGE
}

const fn xid_to_pgidx(xid: u64) -> u64 {
    xid % XACTS_PER_PAGE
}

const fn xid_to_byte(xid: u64) -> usize {
    (xid_to_pgidx(xid) / XACTS_PER_BYTE) as usize
}

const fn xid_to_bidx(xid: u64) -> u64 {
    xid % XACTS_PER_BYTE
}

pub trait SessionExt {
    fn resize_clog_l1cache(&self);
}

impl SessionExt for SessionState {
    fn resize_clog_l1cache(&self) {
        resize_l1cache(&self.gucstate);
    }
}

pub trait WorkerExt {
    fn resize_clog_l1cache(&self);
}

impl WorkerExt for WorkerState {
    fn resize_clog_l1cache(&self) {
        resize_l1cache(&self.gucstate);
    }
}

use crate::access::slru;
use crate::guc;
use crate::guc::GucState;
use crate::utils::{Worker, Xid};
use anyhow;
use kuiba::{SelectedSliceIter, KB_BLCKSZ};
use lru::LruCache;
use static_assertions::const_assert;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

const PACKED_SIZE: usize = 8;
const_assert!((PACKED_SIZE & (PACKED_SIZE - 1)) == 0); // PACKED_SIZE should be 2^n!
const_assert!(KB_BLCKSZ % PACKED_SIZE == 0);
const PACKED_XIDS: u64 = PACKED_SIZE as u64 * XACTS_PER_BYTE;
const PACKED_MASK: u64 = PACKED_XIDS - 1;
const PACKED_KEY_MASK: u64 = !PACKED_MASK;
const_assert!((PACKED_KEY_MASK | PACKED_MASK) == 0xFFFFFFFFFFFFFFFF);

#[derive(Copy, Clone)]
pub struct PackedXidStatus([u8; PACKED_SIZE]);

impl PackedXidStatus {
    fn get_xid_status(&self, xid: Xid) -> XidStatus {
        let xid = (xid.get()) & PACKED_MASK;
        let byteno = xid / XACTS_PER_BYTE;
        let bidx = xid_to_bidx(xid);
        get_xid_status(&self.0, byteno as usize, bidx)
    }

    fn new(d: &[u8]) -> PackedXidStatus {
        PackedXidStatus(d.try_into().unwrap())
    }
}

fn set_xid_status(buff: &mut [u8], byteno: usize, bidx: u64, status: XidStatus) {
    let bshift = bidx * BITS_PER_XACT;
    let mut byteval = buff[byteno];
    byteval &= !(((1 << BITS_PER_XACT) - 1) << bshift);
    byteval |= (status as u8) << bshift;
    buff[byteno] = byteval;
}

fn u8_get_xid_status(byteval: u8, bidx: u64) -> XidStatus {
    let bshift = bidx * BITS_PER_XACT;
    ((byteval >> bshift) & XACT_BITMASK).into()
}

fn get_xid_status(buff: &[u8], byteno: usize, bidx: u64) -> XidStatus {
    u8_get_xid_status(buff[byteno], bidx)
}

pub struct VecXidStatus(Vec<u8>);

impl VecXidStatus {
    fn set_xid_status(&mut self, idx: usize, status: XidStatus) {
        let byteno = idx / XACTS_PER_BYTE as usize;
        let bidx = xid_to_bidx(idx as u64);
        set_xid_status(&mut self.0, byteno, bidx, status);
    }

    #[cfg(test)]
    pub fn new(n: usize) -> VecXidStatus {
        let x = XACTS_PER_BYTE as usize;
        let s = (n + (x - 1)) / x;
        VecXidStatus(vec![0; s]) // use set_len to avoid zeroing.
    }

    #[cfg(test)]
    pub fn split(val: u8) -> [XidStatus; XACTS_PER_BYTE as usize] {
        [
            u8_get_xid_status(val, 0),
            u8_get_xid_status(val, 1),
            u8_get_xid_status(val, 2),
            u8_get_xid_status(val, 3),
        ]
    }

    #[cfg(test)]
    pub fn data(&self) -> &Vec<u8> {
        &self.0
    }
}

struct L1Cache(LruCache<u64, PackedXidStatus>);

impl L1Cache {
    fn key_of(xid: Xid) -> u64 {
        xid.get() & PACKED_KEY_MASK
    }

    fn value(&mut self, xidkey: u64) -> Option<PackedXidStatus> {
        self.0.get(&xidkey).map(|v| *v)
    }
}

pub struct WorkerCacheExt {
    l1cache: L1Cache,
}

impl WorkerCacheExt {
    pub fn new(gucstate: &GucState) -> WorkerCacheExt {
        let l1cache_size = guc::get_int(&gucstate, guc::ClogL1cacheSize) as usize;
        WorkerCacheExt {
            l1cache: L1Cache(LruCache::new(l1cache_size)),
        }
    }
}

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
    let clog_l2cache_size = guc::get_int(&gucstate, guc::ClogL2cacheSize) as usize;
    GlobalStateExt::new(clog_l2cache_size)
}

#[derive(Copy, Clone)]
pub struct WorkerStateExt {
    d: &'static slru::Slru,
}

impl WorkerStateExt {
    pub fn new(d: &'static GlobalStateExt) -> WorkerStateExt {
        WorkerStateExt { d: &d.d }
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

pub trait WorkerExt {
    fn set_xid_status(&self, xid: Xid, status: XidStatus) -> anyhow::Result<()>;
    fn get_xid_status(
        &self,
        xids: &[Xid],
        idx: &[usize],
        ret: &mut VecXidStatus,
    ) -> anyhow::Result<()>;
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

type PagenoIdxMap = HashMap<slru::Pageno, Vec<usize>>;
fn insert_pageno_idx(xidmap: &mut PagenoIdxMap, pageno: slru::Pageno, idx: usize) {
    match xidmap.get_mut(&pageno) {
        None => {
            xidmap.insert(pageno, vec![idx]);
        }
        Some(idxes) => {
            idxes.push(idx);
        }
    }
}

impl WorkerExt for Worker {
    fn set_xid_status(&self, xid: Xid, status: XidStatus) -> anyhow::Result<()> {
        let slru = self.state.clog.d;
        let xid = xid.get();
        let byteno = xid_to_byte(xid);
        let bidx = xid_to_bidx(xid);
        let bshift = bidx * BITS_PER_XACT;
        let andbits = !(((1 << BITS_PER_XACT) - 1) << bshift);
        let orbits = (status as u8) << bshift;
        // Add group commit like TransactionGroupUpdateXidStatus
        slru.writable_load(xid_to_pageno(xid), |buff| {
            buff[byteno] = (buff[byteno] & andbits) | orbits;
        })
    }

    fn get_xid_status(
        &self,
        xids: &[Xid],
        idxes: &[usize],
        ret: &mut VecXidStatus,
    ) -> anyhow::Result<()> {
        let mut xidmap: PagenoIdxMap = HashMap::new();
        let mut cache = self.cache.borrow_mut();
        for (&xid, idx) in SelectedSliceIter::new(xids, idxes.iter().map(|v| *v)) {
            let xid_cachekey = L1Cache::key_of(xid);
            match cache.clog.l1cache.value(xid_cachekey) {
                None => {
                    insert_pageno_idx(&mut xidmap, xid_to_pageno(xid.get()), idx);
                }
                Some(packed_status) => {
                    let xidstatus = packed_status.get_xid_status(xid);
                    if xidstatus == XidStatus::InProgress {
                        insert_pageno_idx(&mut xidmap, xid_to_pageno(xid.get()), idx);
                    } else {
                        ret.set_xid_status(idx, xidstatus);
                    }
                }
            }
        }
        if xidmap.is_empty() {
            return Ok(());
        }
        for (&pageno, idxes) in xidmap.iter() {
            let mut cachekey_set =
                HashSet::<u64>::with_capacity(idxes.len() / PACKED_XIDS as usize);
            self.state.clog.d.try_readonly_load(pageno, |buff| {
                for (&xid, idx) in SelectedSliceIter::new(xids, idxes.iter().map(|v| *v)) {
                    let byteno = xid_to_byte(xid.get());
                    let xidstatus = get_xid_status(buff, byteno, xid_to_bidx(xid.get()));
                    ret.set_xid_status(idx, xidstatus);

                    let cachekey = L1Cache::key_of(xid);
                    cachekey_set.insert(cachekey);
                }
                for &cachekey in cachekey_set.iter() {
                    debug_assert_eq!(cachekey / XACTS_PER_PAGE, pageno);
                    let byteno = xid_to_byte(cachekey);
                    let packed_status = PackedXidStatus::new(&buff[byteno..byteno + PACKED_SIZE]);
                    cache.clog.l1cache.0.put(cachekey, packed_status);
                }
            })?;
        }
        Ok(())
    }
}

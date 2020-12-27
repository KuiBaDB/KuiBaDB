use crate::access::slru;
use crate::utils::{WorkerState, Xid};
use anyhow;
use kuiba::KB_BLCKSZ;

pub struct GlobalStateExt {
    d: slru::Slru,
}

impl GlobalStateExt {
    pub fn new() -> GlobalStateExt {
        GlobalStateExt {
            d: slru::Slru::new(128, "kb_xact"),
        }
    }
}

#[derive(Copy, Clone)]
pub struct WorkerStateExt {
    d: &'static GlobalStateExt,
}

impl WorkerStateExt {
    pub fn new(d: &'static GlobalStateExt) -> WorkerStateExt {
        WorkerStateExt { d }
    }
}

#[repr(u8)]
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
    fn get_xid_status(&self, xid: Xid) -> anyhow::Result<XidStatus>;
}

const BITS_PER_XACT: u64 = 2;
const XACTS_PER_BYTE: usize = 4;
const XACTS_PER_PAGE: u64 = (KB_BLCKSZ * XACTS_PER_BYTE) as u64;
const XACT_BITMASK: u8 = (1 << BITS_PER_XACT) - 1;

const fn xid_to_pageno(xid: Xid) -> slru::Pageno {
    xid.get() / XACTS_PER_PAGE
}

const fn xid_to_pgidx(xid: Xid) -> u64 {
    xid.get() % XACTS_PER_PAGE
}

const fn xid_to_byte(xid: Xid) -> usize {
    xid_to_pgidx(xid) as usize / XACTS_PER_BYTE
}

const fn xid_to_bidx(xid: Xid) -> u64 {
    xid.get() % XACTS_PER_BYTE as u64
}

impl WorkerExt for WorkerState {
    fn set_xid_status(&self, xid: Xid, status: XidStatus) -> anyhow::Result<()> {
        let slru = &self.clog.d.d;
        // Add group commit like TransactionGroupUpdateXidStatus
        slru.writable_load(xid_to_pageno(xid), |buff| {
            let byteno = xid_to_byte(xid);
            let bshift = xid_to_bidx(xid) * BITS_PER_XACT;
            let mut byteval = buff[byteno];
            byteval &= !(((1 << BITS_PER_XACT) - 1) << bshift);
            byteval |= (status as u8) << bshift as u8;
            buff[byteno] = byteval;
        })
    }

    fn get_xid_status(&self, xid: Xid) -> anyhow::Result<XidStatus> {
        let slru = &self.clog.d.d;
        slru.try_readonly_load(xid_to_pageno(xid), |buff| {
            let byteno = xid_to_byte(xid);
            let bshift = xid_to_bidx(xid) * BITS_PER_XACT;
            let byteval = buff[byteno];
            ((byteval >> bshift) & XACT_BITMASK).into()
        })
    }
}

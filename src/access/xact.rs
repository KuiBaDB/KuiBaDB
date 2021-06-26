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
use super::clog::XidStatus;
use super::redo::RedoState;
use super::wal::{self, Lsn, RecordHdr, Rmgr, RmgrId, XlogInfo};
use crate::access::lmgr::SessionExt as LMGRSessionExt;
use crate::kbbail;
use crate::protocol::XactStatus;
use crate::utils::{dec_xid, inc_xid, KBSystemTime, SessionState, WorkerState, Xid};
use crate::Oid;
use anyhow::{anyhow, bail};
use log;
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write;
use std::sync::{atomic::AtomicU32, atomic::Ordering::Relaxed, RwLock};

struct BTreeMultiSet<T: Ord> {
    d: BTreeMap<T, u32>,
}

impl<T: Ord> BTreeMultiSet<T> {
    fn new() -> Self {
        Self { d: BTreeMap::new() }
    }

    fn insert(&mut self, value: T) {
        if let Some(cnt) = self.d.get_mut(&value) {
            *cnt += 1;
        } else {
            self.d.insert(value, 1);
        }
    }

    fn first(&self) -> Option<&T> {
        self.d.iter().next().map(|kv| kv.0)
    }

    fn remove<Q: ?Sized>(&mut self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Ord,
    {
        if let Some(cnt) = self.d.get_mut(&value) {
            *cnt -= 1;
            if *cnt <= 0 {
                self.d.remove(value);
            }
            return true;
        }
        return false;
    }
}

struct RunningXactState {
    xids: BTreeSet<Xid>,
    last_completed: Xid,
    nextxid: Xid,
    xid_stop_limit: u64,
}

pub struct GlobalStateExt {
    running: RwLock<RunningXactState>,
    xmins: RwLock<BTreeMultiSet<Xid>>,
    ckpt_delay_num: AtomicU32,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    xmin: Xid,
    xmax: Xid,
    xidset: HashSet<Xid>,
}

impl Snapshot {
    pub fn is_running(&self, xid: Xid) -> bool {
        if xid > self.xmax {
            return true;
        }
        if xid < self.xmin {
            return false;
        }
        if xid == self.xmax {
            return false;
        }
        if xid == self.xmin {
            return true;
        }
        return self.xidset.contains(&xid);
    }
}

fn bsetfirst<T: Copy>(f: &BTreeSet<T>) -> Option<T> {
    f.iter().next().map(|v| *v)
}

impl GlobalStateExt {
    pub fn new(nextxid: Xid, xid_stop_limit: i32) -> GlobalStateExt {
        GlobalStateExt {
            running: RwLock::new(RunningXactState {
                xids: BTreeSet::new(),
                last_completed: dec_xid(nextxid),
                nextxid,
                xid_stop_limit: xid_stop_limit as u64,
            }),
            xmins: RwLock::new(BTreeMultiSet::new()),
            ckpt_delay_num: AtomicU32::new(0),
        }
    }

    // GetNewTransactionId
    fn start_xid(&self) -> anyhow::Result<Xid> {
        const STOP: u64 = u64::MAX - 333;
        let mut state = self.running.write().unwrap();
        if state.nextxid.get() >= STOP - state.xid_stop_limit {
            bail!("database is not accepting commands to avoid wraparound data loss in database");
        }
        let xid = state.nextxid;
        state.nextxid = inc_xid(xid);
        let v = state.xids.insert(xid);
        debug_assert!(v);
        return Ok(xid);
    }

    fn end_xid(&self, xid: Option<Xid>, xmin: Option<Xid>) {
        if let Some(xid) = xid {
            let mut state = self.running.write().unwrap();
            let v = state.xids.remove(&xid);
            debug_assert!(v);
            debug_assert_ne!(state.last_completed, xid);
            if state.last_completed < xid {
                state.last_completed = xid;
            }
        }
        if let Some(xmin) = xmin {
            let mut xmins = self.xmins.write().unwrap();
            let v = xmins.remove(&xmin);
            debug_assert!(v);
        }
        return;
    }

    fn get_snap(&self) -> Snapshot {
        let (xids, last_xid, xmin) = {
            let state = self.running.read().unwrap();
            let mut xiditer = state.xids.iter();
            let (xmin, xidvec) = if let Some(&xmin) = xiditer.next() {
                debug_assert!(xmin < state.last_completed || xmin == inc_xid(state.last_completed));
                let mut xidvec = Vec::with_capacity(state.xids.len() - 1);
                for &xid in xiditer {
                    xidvec.push(xid);
                }
                (xmin, xidvec)
            } else {
                (inc_xid(state.last_completed), Vec::new())
            };
            {
                let mut xmins = self.xmins.write().unwrap();
                xmins.insert(xmin);
            }
            (xidvec, state.last_completed, xmin)
        };
        let mut xidset = HashSet::<Xid>::with_capacity(xids.len());
        for &xid in xids.iter() {
            xidset.insert(xid);
        }
        Snapshot {
            xmin,
            xmax: last_xid,
            xidset,
        }
    }

    pub fn global_xmin(&self) -> Xid {
        let (min_running_xid, last_comp) = {
            let state = self.running.read().unwrap();
            (bsetfirst(&state.xids), state.last_completed)
        };
        let min_xmin = { self.xmins.read().unwrap().first().map(|v| *v) };
        let mut xmin = inc_xid(last_comp);
        if let Some(xid) = min_running_xid {
            if xid < xmin {
                xmin = xid;
            }
        }
        if let Some(xid) = min_xmin {
            if xid < xmin {
                xmin = xid;
            }
        }
        return xmin;
    }

    fn start_delay_ckpt(&self) {
        self.ckpt_delay_num.fetch_add(1, Relaxed);
    }

    fn stop_delay_ckpt(&self) {
        self.ckpt_delay_num.fetch_sub(1, Relaxed);
    }

    pub fn ckpt_is_delayed(&self) -> bool {
        self.ckpt_delay_num.load(Relaxed) != 0
    }
}

#[derive(PartialEq, Debug)]
enum TranState {
    Default,
    Start,
    Inprogress,
    Commit,
    Abort,
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum TBlockState {
    Default,
    Started,
    Begin,
    Inprogress,
    End,
    Abort,
    AbortEnd,
    AbortPending,
}

struct TranCtx {
    xid: Option<Xid>,
    state: TranState,
    block_state: TBlockState,
    startts: KBSystemTime,
}

pub struct SessionStateExt {
    xact: Option<&'static GlobalStateExt>,
    tranctx: TranCtx,
    snap: Option<Snapshot>,
    last_rec_end: Option<Lsn>,
}

impl SessionStateExt {
    pub fn new(xact: Option<&'static GlobalStateExt>, startts: KBSystemTime) -> Self {
        Self {
            xact,
            tranctx: TranCtx {
                startts,
                xid: None,
                state: TranState::Default,
                block_state: TBlockState::Default,
            },
            snap: None,
            last_rec_end: None,
        }
    }
}

#[derive(Debug)]
struct XactRec {
    xact_endts: KBSystemTime,
}

#[repr(C, packed(1))]
struct XactRecSer {
    xact_endts: u64,
}

impl std::convert::From<&XactRec> for XactRecSer {
    fn from(v: &XactRec) -> Self {
        Self {
            xact_endts: v.xact_endts.into(),
        }
    }
}

impl std::convert::From<&XactRecSer> for XactRec {
    fn from(v: &XactRecSer) -> Self {
        Self {
            xact_endts: v.xact_endts.into(),
        }
    }
}

fn get_xact_rec(d: &[u8]) -> XactRec {
    unsafe { (&*(d.as_ptr() as *const XactRecSer)).into() }
}

#[repr(u8)]
enum XactInfo {
    Commit = 0x00,
    Abort = 0x20,
}

impl From<u8> for XactInfo {
    fn from(value: u8) -> Self {
        if value == XactInfo::Commit as u8 {
            XactInfo::Commit
        } else if value == XactInfo::Abort as u8 {
            XactInfo::Abort
        } else {
            panic!("try from u8 to XactInfo failed. value={}", value)
        }
    }
}

// session context
fn sctx(sess: &mut SessionState) -> &mut SessionStateExt {
    &mut sess.xact
}

// global context
fn gctx(sess: &mut SessionState) -> &'static GlobalStateExt {
    sctx(sess).xact.unwrap()
}

// transaction context
fn tctx(sess: &mut SessionState) -> &mut TranCtx {
    &mut sctx(sess).tranctx
}

// immutable tctx
fn itctx(sess: &SessionState) -> &TranCtx {
    &sess.xact.tranctx
}

fn log_xact_rec(sess: &mut SessionState, xact_endts: KBSystemTime, info: XactInfo) {
    let commit_rec = XactRec { xact_endts };
    let commit_rec_ser: XactRecSer = (&commit_rec).into();
    let rec = wal::start_record(&commit_rec_ser);
    sess.insert_record(RmgrId::Xact, info as u8, rec);
    return;
}

fn log_commit_rec(sess: &mut SessionState, commit_time: KBSystemTime) {
    log_xact_rec(sess, commit_time, XactInfo::Commit);
    return;
}

fn record_tran_commit(sess: &mut SessionState) {
    if tctx(sess).xid.is_some() {
        // stop_delay_ckpt() must be called!
        gctx(sess).start_delay_ckpt();
        log_commit_rec(sess, KBSystemTime::now());
    }
    if let Some(lsn) = sctx(sess).last_rec_end {
        sess.wal.unwrap().fsync(lsn);
        sctx(sess).last_rec_end = None;
    }
    if let Some(xid) = tctx(sess).xid {
        sess.clog.set_xid_status(xid, XidStatus::Committed).unwrap();
        gctx(sess).stop_delay_ckpt();
    }
    return;
}

fn record_tran_abort(sess: &mut SessionState) -> anyhow::Result<()> {
    if let Some(xid) = tctx(sess).xid {
        if sess.clog.xid_status(xid)? == XidStatus::Committed {
            panic!("cannot abort transaction {}, it was already committed", xid);
        }
        log_xact_rec(sess, KBSystemTime::now(), XactInfo::Abort);
        sess.clog.set_xid_status(xid, XidStatus::Aborted).unwrap();
    }
    sctx(sess).last_rec_end = None;
    return Ok(());
}

fn end_xid(sess: &mut SessionState) {
    let xid = tctx(sess).xid;
    let snapxmin = sctx(sess).snap.as_ref().map(|v| v.xmin);
    gctx(sess).end_xid(xid, snapxmin);
    tctx(sess).xid = None;
    sctx(sess).snap = None;
    return;
}

// StartTransaction
fn start_tran(sess: &mut SessionState) -> anyhow::Result<()> {
    debug_assert!(tctx(sess).xid.is_none());
    debug_assert!(sctx(sess).last_rec_end.is_none());
    debug_assert_eq!(tctx(sess).state, TranState::Default);
    tctx(sess).state = TranState::Start;
    // tctx(sess).xid = Some(gctx(sess).start_xid()?);
    tctx(sess).startts = sess.stmt_startts;
    tctx(sess).state = TranState::Inprogress;
    sctx(sess).snap = Some(gctx(sess).get_snap());
    return Ok(());
}
// AssignTransactionId
fn assign_xid(sess: &mut SessionState) -> anyhow::Result<Xid> {
    debug_assert_eq!(tctx(sess).state, TranState::Inprogress);
    debug_assert!(tctx(sess).xid.is_none());
    let xid = gctx(sess).start_xid()?;
    tctx(sess).xid = Some(xid);
    return Ok(xid);
}

// CommitTransaction
fn commit_tran(sess: &mut SessionState) -> anyhow::Result<()> {
    if tctx(sess).state != TranState::Inprogress {
        log::warn!("commit_tran: unexpected state={:?}", tctx(sess).state);
    }
    tctx(sess).state = TranState::Commit;
    record_tran_commit(sess);
    end_xid(sess);
    sess.lock_release_all();
    tctx(sess).state = TranState::Default;
    return Ok(());
}
// AbortTransaction
fn abort_tran(sess: &mut SessionState) -> anyhow::Result<()> {
    if tctx(sess).state != TranState::Inprogress {
        log::warn!("abort_tran: unexpected state={:?}", tctx(sess).state);
    }
    tctx(sess).state = TranState::Abort;
    record_tran_abort(sess)?;
    end_xid(sess);
    sess.lock_release_all();
    return Ok(());
}
// CleanupTransaction
fn cleanup_tran(sess: &mut SessionState) -> anyhow::Result<()> {
    if tctx(sess).state != TranState::Abort {
        sess.dead = true;
        bail!(
            "cleanup_tran: unexpected state={:?}",
            tctx(sess).block_state
        );
    }
    debug_assert!(tctx(sess).xid.is_none());
    debug_assert!(sctx(sess).snap.is_none());
    tctx(sess).state = TranState::Default;
    return Ok(());
}

fn log_nextoid(sess: &mut SessionState, nextoid: u32) {
    let rec = wal::start_record_raw(&nextoid.to_ne_bytes());
    sess.insert_record(RmgrId::Xlog, XlogInfo::NextOid as u8, rec);
    return;
}

// IsTransactionBlock
fn is_transblock(sess: &SessionState) -> bool {
    let bs = itctx(sess).block_state;
    return !(bs == TBlockState::Default || bs == TBlockState::Started);
}

pub trait SessionExt {
    // StartTransactionCommand
    fn start_tran_cmd(&mut self) -> anyhow::Result<()>;
    // CommitTransactionCommand
    fn commit_tran_cmd(&mut self) -> anyhow::Result<()>;
    // AbortCurrentTransaction
    fn abort_cur_tran(&mut self) -> anyhow::Result<()>;
    // BeginTransactionBlock
    fn begin_tran_block(&mut self) -> anyhow::Result<()>;
    // EndTransactionBlock
    fn end_tran_block(&mut self) -> anyhow::Result<bool>;
    // UserAbortTransactionBlock
    fn user_abort_tran_block(&mut self) -> anyhow::Result<()>;
    fn get_xid(&mut self) -> anyhow::Result<Xid>;
    fn is_aborted(&self) -> bool;
    fn insert_record(&mut self, id: RmgrId, info: u8, rec: Vec<u8>) -> Lsn;
    fn try_insert_record(
        &mut self,
        id: RmgrId,
        info: u8,
        rec: Vec<u8>,
        page_lsn: Lsn,
    ) -> Option<Lsn>;
    fn xact_status(&self) -> XactStatus;
    fn new_oid(&mut self) -> Oid;
    // PreventInTransactionBlock
    fn prevent_in_transblock(&self, stmt: &str) -> anyhow::Result<()>;
    // RequireTransactionBlock
    fn require_transblock(&self, stmt: &str) -> anyhow::Result<()>;
}

impl SessionExt for SessionState {
    fn start_tran_cmd(&mut self) -> anyhow::Result<()> {
        match tctx(self).block_state {
            TBlockState::Default => {
                start_tran(self)?;
                tctx(self).block_state = TBlockState::Started;
            }
            TBlockState::Inprogress | TBlockState::Abort => {}
            TBlockState::Begin
            | TBlockState::Started
            | TBlockState::End
            | TBlockState::AbortEnd
            | TBlockState::AbortPending => {
                bail!(
                    "start_tran_cmd: unexpected state={:?}",
                    tctx(self).block_state
                );
            }
        }
        return Ok(());
    }

    fn commit_tran_cmd(&mut self) -> anyhow::Result<()> {
        match tctx(self).block_state {
            TBlockState::Default => {
                self.dead = true;
                bail!(
                    "commit_tran_cmd: unexpected state={:?}",
                    tctx(self).block_state
                );
            }
            TBlockState::Started | TBlockState::End => {
                commit_tran(self)?;
                tctx(self).block_state = TBlockState::Default;
            }
            TBlockState::Begin => {
                tctx(self).block_state = TBlockState::Inprogress;
            }
            TBlockState::Inprogress | TBlockState::Abort => {}
            TBlockState::AbortEnd => {
                cleanup_tran(self)?;
                tctx(self).block_state = TBlockState::Default;
            }
            TBlockState::AbortPending => {
                abort_tran(self)?;
                cleanup_tran(self)?;
                tctx(self).block_state = TBlockState::Default;
            }
        }
        return Ok(());
    }

    fn abort_cur_tran(&mut self) -> anyhow::Result<()> {
        match tctx(self).block_state {
            TBlockState::Default => {
                if self.xact.tranctx.state != TranState::Default {
                    if self.xact.tranctx.state == TranState::Start {
                        self.xact.tranctx.state = TranState::Inprogress;
                    }
                    abort_tran(self)?;
                    cleanup_tran(self)?;
                }
            }
            TBlockState::Started
            | TBlockState::Begin
            | TBlockState::End
            | TBlockState::AbortPending => {
                abort_tran(self)?;
                cleanup_tran(self)?;
                tctx(self).block_state = TBlockState::Default;
            }
            TBlockState::Inprogress => {
                abort_tran(self)?;
                tctx(self).block_state = TBlockState::Abort;
            }
            TBlockState::Abort => {}
            TBlockState::AbortEnd => {
                cleanup_tran(self)?;
                tctx(self).block_state = TBlockState::Default;
            }
        }
        return Ok(());
    }

    fn begin_tran_block(&mut self) -> anyhow::Result<()> {
        match tctx(self).block_state {
            TBlockState::Started => {
                tctx(self).block_state = TBlockState::Begin;
            }
            TBlockState::Inprogress | TBlockState::Abort => {
                log::warn!("there is already a transaction in progress");
            }
            TBlockState::Default
            | TBlockState::Begin
            | TBlockState::End
            | TBlockState::AbortEnd
            | TBlockState::AbortPending => {
                self.dead = true;
                bail!(
                    "begin_tran_block: unexpected state={:?}",
                    tctx(self).block_state
                );
            }
        }
        return Ok(());
    }

    fn end_tran_block(&mut self) -> anyhow::Result<bool> {
        let mut ret = false;
        match tctx(self).block_state {
            TBlockState::Inprogress => {
                tctx(self).block_state = TBlockState::End;
                ret = true;
            }
            TBlockState::Abort => {
                tctx(self).block_state = TBlockState::AbortEnd;
            }
            TBlockState::Started => {
                ret = true;
            }
            TBlockState::Default
            | TBlockState::Begin
            | TBlockState::End
            | TBlockState::AbortEnd
            | TBlockState::AbortPending => {
                self.dead = true;
                bail!(
                    "end_tran_block: unexpected state={:?}",
                    tctx(self).block_state
                );
            }
        }
        return Ok(ret);
    }

    fn user_abort_tran_block(&mut self) -> anyhow::Result<()> {
        match tctx(self).block_state {
            TBlockState::Inprogress | TBlockState::Started => {
                tctx(self).block_state = TBlockState::AbortPending;
            }
            TBlockState::Abort => {
                tctx(self).block_state = TBlockState::AbortEnd;
            }
            TBlockState::Default
            | TBlockState::Begin
            | TBlockState::End
            | TBlockState::AbortEnd
            | TBlockState::AbortPending => {
                self.dead = true;
                bail!(
                    "user_abort_tran_block: unexpected state={:?}",
                    tctx(self).block_state
                );
            }
        }
        return Ok(());
    }

    fn is_aborted(&self) -> bool {
        itctx(self).block_state == TBlockState::Abort
    }

    fn insert_record(&mut self, id: RmgrId, info: u8, mut rec: Vec<u8>) -> Lsn {
        wal::finish_record(&mut rec, id, info, self.xact.tranctx.xid);
        let ret = self.wal.unwrap().insert_record(rec);
        self.xact.last_rec_end = Some(ret);
        return ret;
    }

    fn try_insert_record(
        &mut self,
        id: RmgrId,
        info: u8,
        mut r: Vec<u8>,
        page_lsn: Lsn,
    ) -> Option<Lsn> {
        wal::finish_record(&mut r, id, info, self.xact.tranctx.xid);
        let ret = self.wal.unwrap().try_insert_record(r, page_lsn);
        if ret.is_none() {
            return None;
        }
        self.xact.last_rec_end = ret;
        return ret;
    }
    fn get_xid(&mut self) -> anyhow::Result<Xid> {
        if let Some(xid) = tctx(self).xid {
            return Ok(xid);
        }
        return assign_xid(self);
    }
    // TransactionBlockStatusCode
    fn xact_status(&self) -> XactStatus {
        match itctx(self).block_state {
            TBlockState::Default | TBlockState::Started => XactStatus::NotInBlock,
            TBlockState::Begin | TBlockState::Inprogress | TBlockState::End => XactStatus::InBlock,
            TBlockState::Abort | TBlockState::AbortEnd | TBlockState::AbortPending => {
                XactStatus::Failed
            }
        }
    }
    fn new_oid(&mut self) -> Oid {
        let curoid = self.oid_creator.unwrap().fetch_add(1, Relaxed);
        let nextoid = curoid + 1;
        if nextoid == 0 {
            panic!("no more oid")
        }
        log_nextoid(self, nextoid);
        return Oid::new(curoid).unwrap();
    }

    fn prevent_in_transblock(&self, stmt: &str) -> anyhow::Result<()> {
        if is_transblock(self) {
            kbbail!(
                ERRCODE_ACTIVE_SQL_TRANSACTION,
                "{} cannot run inside a transaction block",
                stmt
            );
        }
        return Ok(());
    }

    fn require_transblock(&self, stmt: &str) -> anyhow::Result<()> {
        if !is_transblock(self) {
            kbbail!(
                ERRCODE_NO_ACTIVE_SQL_TRANSACTION,
                "{} can only be used in transaction blocks",
                stmt
            );
        }
        return Ok(());
    }
}

pub struct XactRmgr {}

impl XactRmgr {
    pub fn new() -> XactRmgr {
        XactRmgr {}
    }
}

impl Rmgr for XactRmgr {
    fn name(&self) -> &'static str {
        "Transaction"
    }

    fn redo(&mut self, hdr: &RecordHdr, _: &[u8], state: &mut RedoState) -> anyhow::Result<()> {
        let xid = hdr.xid.ok_or(anyhow!("XactRmgr::redo: invalid xid"))?;
        let xidstatus = match hdr.rmgr_info().into() {
            XactInfo::Commit => XidStatus::Committed,
            XactInfo::Abort => XidStatus::Aborted,
        };
        return state.worker.clog.set_xid_status(xid, xidstatus);
    }

    fn desc(&self, out: &mut String, hdr: &RecordHdr, data: &[u8]) {
        match hdr.rmgr_info().into() {
            XactInfo::Commit => {
                let xact = get_xact_rec(data);
                write!(out, "COMMIT {:?}", xact).unwrap();
            }
            XactInfo::Abort => {
                let xact = get_xact_rec(data);
                write!(out, "ABORT {:?}", xact).unwrap();
            }
        }
    }
}

pub struct WorkerStateExt {
    last_rec_end: Option<Lsn>,
    xid: Option<Xid>,
}

impl WorkerStateExt {
    pub fn new(sess: &SessionState) -> Self {
        Self {
            last_rec_end: None,
            xid: sess.xact.tranctx.xid,
        }
    }
}

pub trait WorkerExt {
    fn insert_record(&mut self, id: RmgrId, info: u8, rec: Vec<u8>) -> Lsn;
    fn try_insert_record(
        &mut self,
        id: RmgrId,
        info: u8,
        rec: Vec<u8>,
        page_lsn: Lsn,
    ) -> Option<Lsn>;
}

impl WorkerExt for WorkerState {
    fn insert_record(&mut self, id: RmgrId, info: u8, mut rec: Vec<u8>) -> Lsn {
        wal::finish_record(&mut rec, id, info, self.xact.xid);
        let ret = self.wal.unwrap().insert_record(rec);
        self.xact.last_rec_end = Some(ret);
        return ret;
    }

    fn try_insert_record(
        &mut self,
        id: RmgrId,
        info: u8,
        mut r: Vec<u8>,
        page_lsn: Lsn,
    ) -> Option<Lsn> {
        wal::finish_record(&mut r, id, info, self.xact.xid);
        let ret = self.wal.unwrap().try_insert_record(r, page_lsn);
        if ret.is_none() {
            return None;
        }
        self.xact.last_rec_end = ret;
        return ret;
    }
}

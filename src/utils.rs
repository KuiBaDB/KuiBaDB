/*
Copyright 2020 <盏一 w@hidva.com>
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
use crate::access::{clog, wal, xact};
use crate::catalog::namespace::SessionStateExt as NameSpaceSessionStateExt;
use crate::Oid;
use crate::{guc, protocol, GlobalState};
use anyhow::anyhow;
use chrono::offset::Local;
use chrono::DateTime;
use std::cell::RefCell;
use std::debug_assert;
use std::fs::File;
use std::io::Write;
use std::net::TcpStream;
use std::num::NonZeroU16;
use std::path::Path;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;
use thread_local::ThreadLocal;

pub mod err;
pub mod fmgr;
mod sb;

pub use sb::new_fifo_sb;
pub use sb::SharedBuffer;

pub struct Worker {
    pub cache: &'static RefCell<WorkerCache>, // thread local
    pub state: WorkerState,
}

impl Worker {
    pub fn new(state: WorkerState) -> Worker {
        let cache = state
            .worker_cache
            .get_or(|| RefCell::new(WorkerCache::new(&state)));
        Worker { cache, state }
    }
}

pub struct WorkerCache {
    pub clog: clog::WorkerCacheExt,
}

impl WorkerCache {
    fn new(state: &WorkerState) -> WorkerCache {
        WorkerCache {
            clog: clog::WorkerCacheExt::new(&state.gucstate),
        }
    }
}

pub struct WorkerState {
    pub worker_cache: &'static ThreadLocal<RefCell<WorkerCache>>,
    pub clog: clog::WorkerStateExt,
    pub fmgr_builtins: &'static fmgr::FmgrBuiltinsMap,
    pub sessid: u32,
    pub reqdb: Oid,
    pub termreq: Arc<AtomicBool>,
    pub gucstate: Arc<guc::GucState>,
}

impl WorkerState {
    pub fn new(session: &SessionState) -> WorkerState {
        WorkerState {
            worker_cache: session.worker_cache,
            fmgr_builtins: session.fmgr_builtins,
            sessid: session.sessid,
            reqdb: session.reqdb,
            termreq: session.termreq.clone(),
            gucstate: session.gucstate.clone(),
            clog: session.clog,
        }
    }
}

pub struct SessionState {
    pub worker_cache: &'static ThreadLocal<RefCell<WorkerCache>>,
    pub clog: clog::WorkerStateExt,
    pub fmgr_builtins: &'static fmgr::FmgrBuiltinsMap,
    pub sessid: u32,
    pub reqdb: Oid,
    pub db: String,
    pub termreq: Arc<AtomicBool>,
    pub gucstate: Arc<guc::GucState>,
    pub metaconn: sqlite::Connection,
    pub xact: xact::SessionStateExt,
    pub wal: Option<&'static wal::GlobalStateExt>,
    pub stmt_startts: KBSystemTime,
    pub dead: bool,
    pub nsstate: NameSpaceSessionStateExt,
}

impl SessionState {
    pub fn new(
        sessid: u32,
        reqdb: Oid,
        db: String,
        termreq: Arc<AtomicBool>,
        metaconn: sqlite::Connection,
        gstate: GlobalState,
    ) -> Self {
        let now = KBSystemTime::now();
        Self {
            sessid,
            reqdb,
            db,
            termreq,
            fmgr_builtins: gstate.fmgr_builtins,
            gucstate: gstate.gucstate,
            metaconn,
            dead: false,
            nsstate: NameSpaceSessionStateExt::default(),
            clog: clog::WorkerStateExt::new(gstate.clog),
            stmt_startts: now.into(),
            xact: xact::SessionStateExt::new(gstate.xact, now),
            wal: gstate.wal,
            worker_cache: gstate.worker_cache,
        }
    }

    pub fn error(&self, code: &str, msg: &str, stream: &mut TcpStream) {
        log::error!("{}", msg);
        let loglvl = if self.dead { "FATAL" } else { "ERROR" };
        protocol::write_message(stream, &protocol::ErrorResponse::new(loglvl, code, msg))
    }

    pub fn on_error(&self, err: &anyhow::Error, stream: &mut TcpStream) {
        let errcode = err::errcode(err);
        self.error(errcode, &format!("{:#}", err), stream);
    }

    pub fn update_stmt_startts(&mut self) {
        self.stmt_startts = KBSystemTime::now();
    }
    pub fn new_worker(&self) -> Worker {
        Worker::new(WorkerState::new(self))
    }
}

// The maximum length of a fixed length type is 2 ^ 15 - 1, so this will never overflow
#[derive(Copy, Clone, Debug)]
pub enum TypLen {
    Var,
    Fixed(NonZeroU16),
}

impl std::convert::From<i16> for TypLen {
    fn from(val: i16) -> Self {
        if val < 0 {
            TypLen::Var
        } else {
            TypLen::Fixed(NonZeroU16::new(val as u16).unwrap())
        }
    }
}

impl std::convert::From<TypLen> for i16 {
    fn from(val: TypLen) -> Self {
        match val {
            TypLen::Var => -1,
            TypLen::Fixed(v) => v.get() as i16,
        }
    }
}

// We don't want to increase the size of the TypMod
#[derive(Clone, Copy, Debug)]
pub struct TypMod(i32);

impl TypMod {
    pub fn none() -> TypMod {
        TypMod(-1)
    }

    pub fn is_none(self) -> bool {
        self.0 < 0
    }

    pub fn get(self) -> i32 {
        debug_assert!(!self.is_none());
        self.0
    }
}

impl std::convert::From<i32> for TypMod {
    fn from(val: i32) -> Self {
        Self(val)
    }
}

impl std::convert::From<TypMod> for i32 {
    fn from(val: TypMod) -> i32 {
        val.0
    }
}

pub type AttrNumber = NonZeroU16;
pub type Xid = std::num::NonZeroU64;

pub fn inc_xid(v: Xid) -> Xid {
    Xid::new(v.get() + 1).unwrap()
}

pub fn dec_xid(v: Xid) -> Xid {
    Xid::new(v.get() - 1).unwrap()
}

pub fn sync_dir<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

// Path::new(file).parent().unwrap() must not be empty.
pub fn persist<P: AsRef<Path>>(file: P, d: &[u8]) -> anyhow::Result<()> {
    let path = file.as_ref();
    {
        let mut tempf = NamedTempFile::new_in(".")?;
        tempf.write_all(d)?;
        tempf.flush()?;
        let targetfile = tempf.persist(path)?;
        targetfile.sync_all()?;
    }
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("persist: invalid filepath. file={:?}", path))?;
    sync_dir(dir)?;
    Ok(())
}

// just for implementing Display trait
#[derive(Clone, Copy)]
pub struct KBSystemTime(SystemTime);

impl std::fmt::Display for KBSystemTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let t: DateTime<Local> = self.0.into();
        write!(f, "{}", t)
    }
}

impl std::fmt::Debug for KBSystemTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let t: DateTime<Local> = self.0.into();
        write!(f, "{:?}", t)
    }
}

impl std::convert::From<SystemTime> for KBSystemTime {
    fn from(v: SystemTime) -> Self {
        Self(v)
    }
}

impl std::convert::From<KBSystemTime> for SystemTime {
    fn from(v: KBSystemTime) -> Self {
        v.0
    }
}

impl KBSystemTime {
    pub fn now() -> Self {
        Self(SystemTime::now())
    }
}

impl std::convert::From<u64> for KBSystemTime {
    fn from(v: u64) -> Self {
        UNIX_EPOCH.checked_add(Duration::new(v, 0)).unwrap().into()
    }
}

impl std::convert::From<KBSystemTime> for u64 {
    fn from(v: KBSystemTime) -> Self {
        v.0.duration_since(UNIX_EPOCH).unwrap().as_secs()
    }
}

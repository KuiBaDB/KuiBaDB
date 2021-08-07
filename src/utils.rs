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
use crate::access::clog::SessionExt as ClogSessionExt;
use crate::access::clog::WorkerExt as ClogWorkerExt;
use crate::access::csmvcc::TabMVCC;
use crate::access::fd::{SessionExt as FDSessionExt, WorkerExt as FDWorkerExt};
use crate::access::lmgr;
use crate::access::{ckpt, sv};
use crate::access::{clog, wal, xact};
use crate::catalog::namespace::SessionStateExt as NameSpaceSessionStateExt;
use crate::Oid;
use crate::{guc, kbensure, protocol, GlobalState, SockWriter};
use anyhow::anyhow;
use chrono::offset::Local;
use chrono::DateTime;
use crossbeam_channel::{unbounded, Receiver};
use nix::libc::off_t;
use nix::sys::uio::IoVec;
use nix::unistd::SysconfVar::IOV_MAX;
use std::alloc::Layout;
use std::fs::File;
use std::io::Write;
use std::num::NonZeroU16;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::{atomic::AtomicBool, atomic::AtomicU32, atomic::Ordering::Relaxed, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;
use threadpool::ThreadPool;

pub mod adt;
pub mod err;
pub mod fmgr;
pub mod marc;
pub mod sb;
pub mod ser;

pub struct WorkerState {
    pub wal: Option<&'static wal::GlobalStateExt>,
    pub clog: clog::WorkerStateExt,
    pub xact: xact::WorkerStateExt,
    pub fmgr_builtins: &'static fmgr::FmgrBuiltinsMap,
    pub sessid: u32,
    pub reqdb: Oid,
    pub termreq: Arc<AtomicBool>,
    pub gucstate: Arc<guc::GucState>,
}

pub struct WorkerExit {
    pub xact: xact::WorkerExitExt,
}

impl WorkerState {
    pub fn new(session: &SessionState) -> WorkerState {
        WorkerState {
            fmgr_builtins: session.fmgr_builtins,
            sessid: session.sessid,
            reqdb: session.reqdb,
            termreq: session.termreq.clone(),
            gucstate: session.gucstate.clone(),
            clog: session.clog,
            xact: xact::WorkerStateExt::new(session),
            wal: session.wal,
        }
    }

    pub fn init_thread_locals(&self) {
        self.resize_clog_l1cache();
        self.resize_fdcache();
    }

    pub fn exit(&self) -> WorkerExit {
        WorkerExit {
            xact: self.xact.exit(),
        }
    }
}

pub struct SessionState {
    thdpool: Option<threadpool::ThreadPool>,
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
    pub oid_creator: Option<&'static AtomicU32>, // nextoid
    pub lmgrg: &'static lmgr::GlobalStateExt,
    pub lmgrs: lmgr::SessionStateExt<'static>,
    pub pending_fileops: &'static ckpt::PendingFileOps,
    pub tabsv: &'static sv::TabSupVer,
    pub tabmvcc: &'static TabMVCC,
}

pub struct WorkerExitGuard<'a, T> {
    rec: &'a Receiver<T>,
}

impl<'a, T> WorkerExitGuard<'a, T> {
    pub fn new(rec: &'a Receiver<T>) -> Self {
        Self { rec }
    }
}

impl<'a, T> Drop for WorkerExitGuard<'a, T> {
    fn drop(&mut self) {
        for _item in self.rec.iter() {}
    }
}

impl SessionState {
    pub fn init_thread_locals(&self) {
        self.resize_clog_l1cache();
        self.resize_fdcache();
    }

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
            thdpool: None,
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
            lmgrg: gstate.lmgr,
            lmgrs: lmgr::SessionStateExt::new(),
            oid_creator: gstate.oid_creator,
            pending_fileops: gstate.pending_fileops,
            tabsv: gstate.tabsv,
            tabmvcc: gstate.tabmvcc,
        }
    }

    pub fn on_error(&self, err: &anyhow::Error, stream: &mut SockWriter) {
        let lvl = if self.dead {
            protocol::SEVERITY_FATAL
        } else {
            protocol::SEVERITY_ERR
        };
        crate::on_error(lvl, err, stream);
    }

    pub fn update_stmt_startts(&mut self) {
        self.stmt_startts = KBSystemTime::now();
    }

    pub fn new_worker(&self) -> WorkerState {
        WorkerState::new(self)
    }

    // Only invokded when commit.
    pub fn exit_worker(&mut self, e: WorkerExit) {
        self.xact.exit_worker(e.xact);
    }

    fn pool(&self) -> &ThreadPool {
        // unsafe {self.thdpool.as_ref().unwrap_unchecked()}
        self.thdpool.as_ref().unwrap()
    }

    fn resize_pool(&mut self, parallel: usize) {
        match self.thdpool.as_mut() {
            None => {
                self.thdpool = Some(ThreadPool::new(parallel));
            }
            Some(p) => {
                debug_assert_eq!(p.queued_count(), 0);
                p.set_num_threads(parallel);
            }
        }
    }

    pub fn exec<Args: Send + 'static, Ret: Send + 'static>(
        &mut self,
        parallel: usize,
        args_gene: impl Fn(usize) -> Args,
        body: impl FnOnce(Args, &mut WorkerState) -> Ret + Send + 'static + Clone,
    ) -> Receiver<(WorkerExit, Ret)> {
        debug_assert!(parallel > 0);
        self.resize_pool(parallel);
        let (send, receiver) = unbounded::<(WorkerExit, Ret)>();
        let lastno = parallel - 1;
        for idx in 0..lastno {
            let args = args_gene(idx);
            let mut worker = self.new_worker();
            let body2 = body.clone();
            let send2 = send.clone();
            self.pool().execute(move || {
                worker.init_thread_locals();
                let ret = body2(args, &mut worker);
                send2.send((worker.exit(), ret)).unwrap();
            });
        }
        let args = args_gene(lastno);
        let mut worker = self.new_worker();
        self.pool().execute(move || {
            worker.init_thread_locals();
            let ret = body(args, &mut worker);
            send.send((worker.exit(), ret)).unwrap();
        });
        return receiver;
    }

    pub fn check_termreq(&self) -> anyhow::Result<()> {
        kbensure!(
            !self.termreq.load(Relaxed),
            ERRCODE_ADMIN_SHUTDOWN,
            "terminating connection due to administrator command"
        );
        return Ok(());
    }
}

pub struct ExecSQLOnDrop<'a, 'b> {
    conn: &'a sqlite::Connection,
    sql: &'b str,
}

impl<'a, 'b> ExecSQLOnDrop<'a, 'b> {
    pub fn new(conn: &'a sqlite::Connection, sql: &'b str) -> ExecSQLOnDrop<'a, 'b> {
        Self { conn, sql }
    }
}

impl<'a, 'b> Drop for ExecSQLOnDrop<'a, 'b> {
    fn drop(&mut self) {
        if let Err(err) = self.conn.execute(self.sql) {
            log::warn!(
                "ExecSQLOnDrop: failed to execute sql: sql={} err={}",
                self.sql,
                err
            );
        }
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
    File::open(path)?.sync_data()
}

// Path::new(file).parent().unwrap() must not be empty.
pub fn persist<P: AsRef<Path>>(file: P, d: &[u8]) -> anyhow::Result<()> {
    let path = file.as_ref();
    {
        let mut tempf = NamedTempFile::new_in(".")?;
        tempf.write_all(d)?;
        tempf.flush()?;
        let targetfile = tempf.persist(path)?;
        targetfile.sync_data()?;
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

fn valid_layout(size: usize, align: usize) -> bool {
    // align is the typalign that has been checked at CRAETE TYPE.
    debug_assert!(align.is_power_of_two());
    size <= usize::MAX - (align - 1)
}

// Use std::alloc::Allocator instead.
// Just like Vec and HashMap, out-of-memory is not considered here..
pub fn doalloc(mut size: usize, align: usize) -> NonNull<u8> {
    if size == 0 {
        // GlobalAlloc: undefined behavior can result if the caller does not ensure
        // that layout has non-zero size.
        size = 2;
    }
    debug_assert!(Layout::from_size_align(size, align).is_ok());
    let ret = unsafe { std::alloc::alloc(Layout::from_size_align_unchecked(size, align)) };
    return NonNull::new(ret).expect("alloc failed");
}

pub fn alloc(size: usize, align: usize) -> NonNull<u8> {
    assert!(
        valid_layout(size, align),
        "valid_layout failed: size: {}, align: {}",
        size,
        align
    );
    return doalloc(size, align);
}

pub fn dealloc(ptr: NonNull<u8>, mut size: usize, align: usize) {
    if size == 0 {
        size = 2;
    }
    debug_assert!(Layout::from_size_align(size, align).is_ok());
    unsafe {
        std::alloc::dealloc(ptr.as_ptr(), Layout::from_size_align_unchecked(size, align));
    }
}

pub fn realloc(ptr: NonNull<u8>, align: usize, mut osize: usize, mut nsize: usize) -> NonNull<u8> {
    if osize == 0 {
        osize = 2;
    }
    if nsize == 0 {
        nsize = 2;
    }
    assert!(
        valid_layout(nsize, align),
        "valid_layout failed: size: {}, align: {}",
        nsize,
        align
    );
    debug_assert!(Layout::from_size_align(osize, align).is_ok());
    let ret = unsafe {
        std::alloc::realloc(
            ptr.as_ptr(),
            Layout::from_size_align_unchecked(osize, align),
            nsize,
        )
    };
    return NonNull::new(ret).expect("realloc failed");
}

#[cfg(target_os = "linux")]
fn pwritev(fd: RawFd, iov: &[IoVec<&[u8]>], offset: off_t) -> nix::Result<usize> {
    use nix::sys::uio::pwritev as _pwritev;
    _pwritev(fd, iov, offset)
}

#[cfg(target_os = "macos")]
fn pwritev(fd: RawFd, iov: &[IoVec<&[u8]>], offset: off_t) -> nix::Result<usize> {
    use nix::sys::uio::pwrite;
    let mut buff = Vec::<u8>::new();
    for iv in iov {
        buff.extend_from_slice(iv.as_slice());
    }
    pwrite(fd, buff.as_slice(), offset)
}

pub fn pwritevn<'a>(
    fd: RawFd,
    iov: &'a mut [IoVec<&'a [u8]>],
    mut offset: off_t,
) -> nix::Result<usize> {
    let orig_offset = offset;
    let iovmax = IOV_MAX as usize;
    let iovlen = iov.len();
    let mut sidx: usize = 0;
    while sidx < iovlen {
        let eidx = std::cmp::min(iovlen, sidx + iovmax);
        let wplan = &mut iov[sidx..eidx];
        let mut part = pwritev(fd, wplan, offset)?;
        offset += part as off_t;
        for wiov in wplan {
            let wslice = wiov.as_slice();
            let wiovlen = wslice.len();
            if wiovlen > part {
                let wpartslice = unsafe {
                    std::slice::from_raw_parts(wslice.as_ptr().add(part), wiovlen - part)
                };
                *wiov = IoVec::from_slice(wpartslice);
                break;
            }
            sidx += 1;
            part -= wiovlen;
            if part <= 0 {
                break;
            }
        }
    }
    Ok((offset - orig_offset) as usize)
}

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
use crate::utils::{persist, Xid};
use crate::Oid;
use anyhow::anyhow;
use crc32c;
use log;
use memoffset::offset_of;
use nix::libc::off_t;
use nix::sys::uio::IoVec;
use nix::unistd::SysconfVar::IOV_MAX;
use std::cmp::min;
use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread::panicking;
use std::time::SystemTime;

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

fn pwritevn<'a>(
    fd: RawFd,
    iov: &'a mut [IoVec<&'a [u8]>],
    mut offset: off_t,
) -> nix::Result<usize> {
    let orig_offset = offset;
    let iovmax = IOV_MAX as usize;
    let iovlen = iov.len();
    let mut sidx: usize = 0;
    while sidx < iovlen {
        let eidx = min(iovlen, sidx + iovmax);
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

#[repr(C, packed(1))]
#[derive(Copy, Clone)]
pub struct Ckpt {
    pub redo: Lsn,
    pub curtli: TimeLineID,
    pub prevtli: TimeLineID,
    pub nextxid: Xid,
    pub nextoid: Oid,
    pub time: SystemTime,
}
const CKPTLEN: usize = size_of::<Ckpt>();

#[repr(C, packed(1))]
#[derive(Copy, Clone)]
pub struct Ctl {
    pub ctlver: u32,
    pub catver: u32,
    pub time: SystemTime,
    pub ckpt: Lsn,
    pub ckptcpy: Ckpt,
    pub crc32c: u32,
}
const CTLLEN: usize = size_of::<Ctl>();

pub const KB_CTL_VER: u32 = 20130203;
pub const KB_CAT_VER: u32 = 20181218;
pub const CONTROL_FILE: &'static str = "global/kb_control";

impl Ctl {
    pub fn new(ckpt: Lsn, ckptcpy: Ckpt) -> Ctl {
        let mut ctl = Ctl {
            ctlver: KB_CTL_VER,
            catver: KB_CAT_VER,
            time: SystemTime::now(),
            ckpt,
            ckptcpy,
            crc32c: 0,
        };
        let crc = ctl.cal_crc32c();
        ctl.crc32c = crc;
        ctl
    }

    pub fn cal_crc32c(&self) -> u32 {
        let crc = unsafe {
            let ptr = self as *const Ctl as *const u8;
            let len = offset_of!(Ctl, crc32c);
            let d = std::slice::from_raw_parts(ptr, len);
            crc32c::crc32c(d)
        };
        crc
    }

    pub fn persist(&self) -> anyhow::Result<()> {
        unsafe {
            let ptr = self as *const _ as *const u8;
            let d = std::slice::from_raw_parts(ptr, CTLLEN);
            persist(CONTROL_FILE, d)
        }
    }

    pub fn load() -> anyhow::Result<Ctl> {
        let mut d = Vec::with_capacity(CTLLEN);
        File::open(CONTROL_FILE)?.read_to_end(&mut d)?;
        if d.len() != CTLLEN {
            Err(anyhow!("load: invalid control file. len={}", d.len()))
        } else {
            Ok(unsafe { std::ptr::read(d.as_ptr() as *const Ctl) })
        }
    }
}

pub fn new_ckpt_rec(ckpt: &Ckpt) -> Vec<u8> {
    let mut record = Vec::with_capacity(RECHDRLEN + CKPTLEN);
    record.resize(RECHDRLEN, 0);
    unsafe {
        let ptr = ckpt as *const _ as *const u8;
        let d = std::slice::from_raw_parts(ptr, CKPTLEN);
        record.extend_from_slice(d);
    }
    record
}

trait Rmgr {
    fn name(&self) -> &'static str;
    fn redo(&mut self, record: &[u8]);
    fn desc(&self, out: &mut String, record: &[u8]);
    fn descstr(&self, record: &[u8]) -> String {
        let mut s = String::new();
        self.desc(&mut s, record);
        s
    }
}

#[repr(u8)]
#[derive(Debug)]
pub enum RmgrId {
    Xlog,
    Xact,
    Total,
}

pub trait WalStorageFile {
    fn pread(&self, buf: &mut [u8], offset: usize) -> anyhow::Result<usize>;
    fn len(&self) -> usize;
    fn lsn(&self) -> Lsn;
}

pub trait WalStorage {
    fn find(&self, lsn: Lsn) -> anyhow::Result<Option<String>>;
    fn open(&mut self, key: &str) -> anyhow::Result<Box<dyn WalStorageFile>>;
    fn recycle(&mut self, lsn: Lsn) -> anyhow::Result<()>;
}

pub struct LocalWalStorage {}

impl LocalWalStorage {
    pub fn new() -> LocalWalStorage {
        todo!()
    }
}

impl WalStorage for LocalWalStorage {
    fn find(&self, lsn: Lsn) -> anyhow::Result<Option<String>> {
        todo!()
    }

    fn open(&mut self, key: &str) -> anyhow::Result<Box<dyn WalStorageFile>> {
        todo!()
    }

    fn recycle(&mut self, lsn: Lsn) -> anyhow::Result<()> {
        todo!()
    }
}

pub struct WalReader {
    storage: Box<dyn WalStorage>,
    readlsn: Option<Lsn>,
    endlsn: Lsn,
    file: Option<Box<dyn WalStorageFile>>,
}

impl WalReader {
    pub fn new(storage: Box<dyn WalStorage>, startlsn: Lsn) -> WalReader {
        todo!()
    }

    pub fn rescan(&mut self, startlsn: Lsn) {
        todo!()
    }

    pub fn read_record(&mut self) -> anyhow::Result<&[u8]> {
        todo!()
    }
}

struct Progress {
    pt: Mutex<crate::ProgressTracker>,
    p: crate::Progress,
}

impl Progress {
    fn new(d: u64) -> Progress {
        Progress {
            pt: Mutex::new(crate::ProgressTracker::new(d)),
            p: crate::Progress::new(d),
        }
    }

    fn wait(&self, p: u64) {
        self.p.wait(p)
    }

    fn done(&self, start: u64, end: u64) {
        let np = {
            let mut pt = self.pt.lock().unwrap();
            pt.done(start, end)
        };
        if let Some(np) = np {
            self.p.set(np)
        }
    }

    fn get(&self) -> u64 {
        self.p.get()
    }
}

struct AbortWhenPanic;

impl Drop for AbortWhenPanic {
    fn drop(&mut self) {
        if panicking() {
            std::process::abort();
        }
    }
}

// The lsn for the first record is 0x0133F0E2
pub type Lsn = NonZeroU64;
pub type TimeLineID = NonZeroU32;

struct WritingWalFile {
    fd: File,
    start_lsn: Lsn,
    write: &'static Progress,
    flush: &'static Progress,
}

fn wal_filepath(tli: TimeLineID, lsn: Lsn) -> String {
    format!("kb_wal/{:0>8X}{:0>16X}.wal", tli, lsn)
}

impl WritingWalFile {
    fn new(
        tli: TimeLineID,
        lsn: Lsn,
        write: &'static Progress,
        flush: &'static Progress,
    ) -> std::io::Result<WritingWalFile> {
        Ok(WritingWalFile {
            fd: WritingWalFile::open_file(tli, lsn)?,
            start_lsn: lsn,
            write,
            flush,
        })
    }

    fn open_file(tli: TimeLineID, lsn: Lsn) -> std::io::Result<File> {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(wal_filepath(tli, lsn))
    }

    fn fsync(&self, end_lsn: u64) -> std::io::Result<()> {
        self.fd.sync_data()?;
        let start_lsn = self.start_lsn.get();
        self.flush.done(start_lsn, end_lsn);
        Ok(())
    }
}

impl Drop for WritingWalFile {
    fn drop(&mut self) {
        if panicking() {
            return;
        }
        let filesize = match self.fd.metadata() {
            Ok(md) => md.len(),
            Err(e) => {
                let errmsg = format!(
                    "WritingWalFile::drop get metadata failed. lsn={} err={}",
                    self.start_lsn, e
                );
                log::error!("{}", errmsg);
                panic!("{}", errmsg);
            }
        };
        let end_lsn = self.start_lsn.get() + filesize;
        self.write.wait(end_lsn);
        if let Err(e) = self.fsync(end_lsn) {
            let errmsg = format!(
                "WritingWalFile::drop sync_data failed. lsn={} err={}",
                self.start_lsn, e
            );
            log::error!("{}", errmsg);
            panic!("{}", errmsg);
        }
    }
}

#[repr(C, packed(1))]
pub struct RecordHdr {
    pub totlen: u32,
    pub info: u8,
    pub id: RmgrId,
    pub xid: Option<Xid>,
    pub prev: Option<Lsn>,
    pub crc32c: u32,
}
pub const RECHDRLEN: usize = size_of::<RecordHdr>();

impl RecordHdr {
    pub fn rmgr_info(&self) -> u8 {
        self.info & 0xf0
    }
}

fn mut_hdr(d: &mut [u8]) -> &mut RecordHdr {
    unsafe { &mut *(d.as_mut_ptr() as *mut RecordHdr) }
}

pub fn finish_record(d: &mut [u8], id: RmgrId, info: u8, xid: Option<Xid>) {
    let len = d.len();
    if len > u32::MAX as usize || len < RECHDRLEN {
        panic!(
            "invalid record in finish_record(). len={} id={:?} info={} xid={:?}",
            len, id, info, xid
        );
    }
    let crc = crc32c::crc32c(&d[RECHDRLEN..]);
    let len = len as u32;
    let hdr = mut_hdr(d);
    hdr.totlen = len;
    hdr.info = info;
    hdr.id = id;
    hdr.xid = xid;
    hdr.crc32c = crc;
    return;
}

type RecordBuff = Vec<u8>;

struct InsertWriteReq {
    buf: Vec<RecordBuff>,
    record: Option<RecordBuff>,
    buflsn: Lsn,
    file: Arc<WritingWalFile>,
}

impl InsertWriteReq {
    fn write(self) -> nix::Result<usize> {
        let mut iovec = Vec::with_capacity(self.buf.len() + 1);
        for ref onebuf in &self.buf {
            iovec.push(IoVec::from_slice(onebuf.as_slice()));
        }
        if let Some(ref record) = self.record {
            iovec.push(IoVec::from_slice(record.as_slice()));
        }
        let fd = self.file.fd.as_raw_fd();
        let buflsn = self.buflsn.get();
        let iovec = iovec.as_mut_slice();
        let off = (buflsn - self.file.start_lsn.get()) as off_t;
        let writen = pwritevn(fd, iovec, off)?;
        self.file.write.done(buflsn, buflsn + writen as u64);
        Ok(writen)
    }
}

struct InsertState {
    curtimeline: TimeLineID,
    wal_buff_max_size: usize,
    wal_file_max_size: u64,
    redo: Lsn,
    buf: Vec<RecordBuff>,
    buflsn: Lsn,
    prevlsn: Option<Lsn>,
    bufsize: usize,
    forcesync: bool,
    // if file is None, it means that file_start_lsn = buflsn.
    file: Option<Arc<WritingWalFile>>,
}

enum InsertRet {
    WriteAndCreate {
        tli: TimeLineID,
        retlsn: Lsn,
        wreq: InsertWriteReq,
    },
    Write(Lsn, InsertWriteReq),
    NoAction(Lsn),
}

impl InsertState {
    fn swap_buff(
        &mut self,
        file: Arc<WritingWalFile>,
        record: Option<RecordBuff>,
        newbuflsn: Lsn,
    ) -> InsertWriteReq {
        let writereq = InsertWriteReq {
            buf: std::mem::replace(&mut self.buf, Vec::new()),
            record,
            buflsn: self.buflsn,
            file,
        };
        self.buflsn = newbuflsn;
        self.bufsize = 0;
        writereq
    }

    fn fill_record(record: &mut RecordBuff, prevlsn: Option<Lsn>) {
        let hdr = mut_hdr(record.as_mut_slice());
        hdr.prev = prevlsn;
        let bodycrc = hdr.crc32c;
        let crc =
            crc32c::crc32c_append(bodycrc, &record.as_slice()[..offset_of!(RecordHdr, crc32c)]);
        let hdr = mut_hdr(record.as_mut_slice());
        hdr.crc32c = crc;
    }

    // Remeber we are locking, so be quick.
    fn insert(&mut self, mut record: RecordBuff) -> InsertRet {
        InsertState::fill_record(&mut record, self.prevlsn);
        let reclsn = self.nextlsn();
        let newbufsize = self.bufsize + record.len();
        let retlsnval = reclsn.get() + record.len() as u64;
        self.prevlsn = Some(reclsn);
        let retlsn = Lsn::new(retlsnval).unwrap();
        if let Some(ref file) = self.file {
            let newfilesize = retlsnval - file.start_lsn.get();
            if newfilesize >= self.wal_file_max_size {
                let file = std::mem::replace(&mut self.file, None).unwrap();
                let wreq = self.swap_buff(file, Some(record), retlsn);
                let ret = InsertRet::WriteAndCreate {
                    tli: self.curtimeline,
                    retlsn,
                    wreq,
                };
                return ret;
            }
            if newbufsize >= self.wal_buff_max_size {
                let file = Arc::clone(file);
                let writereq = self.swap_buff(file, Some(record), retlsn);
                return InsertRet::Write(retlsn, writereq);
            }
        }
        self.bufsize = newbufsize;
        self.buf.push(record);
        return InsertRet::NoAction(retlsn);
    }

    fn nextlsn(&self) -> Lsn {
        Lsn::new(self.buflsn.get() + self.bufsize as u64).unwrap()
    }
}

// Since flush will be referenced by insert.file, for convenience, we make it as a static variable,
// otherwise, facilities like Pin + unsafe will be used.
pub struct GlobalStateExt {
    // redo is the value of insert.redo at a past time.
    redo: AtomicU64,
    insert: Mutex<InsertState>,
    write: &'static Progress,
    flush: &'static Progress,
}

enum FlushAction {
    Noop,
    Wait,
    Flush(Weak<WritingWalFile>),
    Write(InsertWriteReq),
}

impl GlobalStateExt {
    // We make the type of return value as a static ref to tell the caller that
    // you should call this method only once.
    fn new(
        tli: TimeLineID,
        lsn: Lsn,
        prevlsn: Option<Lsn>,
        redo: Lsn,
        wal_buff_max_size: usize,
        wal_file_max_size: u64,
    ) -> std::io::Result<&'static GlobalStateExt> {
        let flush: &'static Progress = Box::leak(Box::new(Progress::new(lsn.get())));
        let write: &'static Progress = Box::leak(Box::new(Progress::new(lsn.get())));
        Ok(Box::leak(Box::new(GlobalStateExt {
            redo: AtomicU64::new(redo.get()),
            write,
            flush,
            insert: Mutex::new(InsertState {
                wal_buff_max_size,
                wal_file_max_size,
                redo,
                prevlsn,
                curtimeline: tli,
                buf: Vec::new(),
                buflsn: lsn,
                bufsize: 0,
                forcesync: false,
                file: Some(Arc::new(WritingWalFile::new(tli, lsn, write, flush)?)),
            }),
        })))
    }

    fn get_insert_state(&self) -> MutexGuard<InsertState> {
        let insert = self.insert.lock().unwrap();
        self.redo.store(insert.redo.get(), Ordering::Relaxed);
        insert
    }

    fn do_create(&self, tli: TimeLineID, retlsn: Lsn) {
        let file = WritingWalFile::new(tli, retlsn, self.write, self.flush).unwrap();
        let file = Arc::new(file);
        let wreq = {
            let mut insert = self.get_insert_state();
            if insert.forcesync {
                let nxtlsn = insert.nextlsn();
                insert.file = Some(file.clone());
                insert.forcesync = false;
                Some(insert.swap_buff(file, None, nxtlsn))
            } else {
                insert.file = Some(file);
                None
            }
        };
        if let Some(wreq) = wreq {
            let weak_file = Arc::downgrade(&wreq.file);
            let filelsn = wreq.buflsn.get();
            let wn = wreq.write().unwrap();
            self.do_fsync(weak_file, filelsn + wn as u64);
        }
    }

    fn handle_insert_ret(&self, ret: InsertRet) -> Lsn {
        match ret {
            InsertRet::NoAction(lsn) => lsn,
            InsertRet::Write(lsn, wreq) => {
                wreq.write().unwrap();
                lsn
            }
            InsertRet::WriteAndCreate { tli, retlsn, wreq } => {
                wreq.write().unwrap();
                self.do_create(tli, retlsn);
                retlsn
            }
        }
    }

    pub fn insert_record(&self, r: RecordBuff) -> Lsn {
        let _guard = AbortWhenPanic;
        let insert_res = {
            let mut state = self.get_insert_state();
            state.insert(r)
        };
        self.handle_insert_ret(insert_res)
    }

    pub fn try_insert_record(&self, r: RecordBuff, page_lsn: Lsn) -> Option<Lsn> {
        let _guard = AbortWhenPanic;
        let insert_res = {
            let mut state = self.get_insert_state();
            if page_lsn <= state.redo {
                return None;
            }
            state.insert(r)
        };
        Some(self.handle_insert_ret(insert_res))
    }

    fn flush_action(&self, lsn: Lsn) -> FlushAction {
        let lsnval = lsn.get();
        let mut insert = self.get_insert_state();
        if lsnval <= self.flush.get() {
            return FlushAction::Noop;
        }
        if let Some(ref file) = insert.file {
            if lsn <= file.start_lsn {
                return FlushAction::Wait;
            }
            if lsn <= insert.buflsn {
                return FlushAction::Flush(Arc::downgrade(file));
            }
            let file = file.clone();
            let nxtlsn = insert.nextlsn();
            let wreq = insert.swap_buff(file, None, nxtlsn);
            return FlushAction::Write(wreq);
        }
        if lsn <= insert.buflsn {
            return FlushAction::Wait;
        }
        insert.forcesync = true;
        return FlushAction::Wait;
    }

    fn do_fsync(&self, weak_file: Weak<WritingWalFile>, lsnval: u64) {
        let file = weak_file.upgrade();
        if let Some(file) = file {
            self.write.wait(lsnval);
            // Remember fsync() will still be called again in WritingWalFile::drop(),
            // and that invocation may succeed. If we return an error here, not panic,
            // this may cause a transaction to be considered aborted, but all wal records
            // of this transaction have been flushed successfully.
            file.fsync(lsnval).unwrap();
        }
        self.flush.wait(lsnval);
    }

    fn do_write(&self, wreq: InsertWriteReq, lsnval: u64) {
        let weak_file = Arc::downgrade(&wreq.file);
        wreq.write().unwrap();
        self.do_fsync(weak_file, lsnval);
    }

    pub fn fsync(&self, lsn: Lsn) {
        let _guard = AbortWhenPanic;
        let lsnval = lsn.get();
        if lsnval <= self.flush.get() {
            return;
        }
        let action = self.flush_action(lsn);
        match action {
            FlushAction::Noop => (),
            FlushAction::Wait => self.flush.wait(lsnval),
            FlushAction::Flush(weak_file) => self.do_fsync(weak_file, lsnval),
            FlushAction::Write(wreq) => self.do_write(wreq, lsnval),
        }
    }
}

pub fn init(
    tli: TimeLineID,
    lsn: Lsn,
    prevlsn: Option<Lsn>,
    redo: Lsn,
    gucstate: &GucState,
) -> std::io::Result<&'static GlobalStateExt> {
    let wal_buff_max_size = guc::get_int(gucstate, guc::WalBuffMaxSize) as usize;
    let wal_file_max_size = guc::get_int(gucstate, guc::WalFileMaxSize) as u64;
    GlobalStateExt::new(
        tli,
        lsn,
        prevlsn,
        redo,
        wal_buff_max_size,
        wal_file_max_size,
    )
}

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum XlogInfo {
    NextOid = 0x30,
    Ckpt = 0x10,
}

impl TryFrom<u8> for XlogInfo {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value == XlogInfo::NextOid as u8 {
            Ok(XlogInfo::NextOid)
        } else if value == XlogInfo::Ckpt as u8 {
            Ok(XlogInfo::Ckpt)
        } else {
            Err(anyhow!("try from u8 to XlogInfo failed. value={}", value))
        }
    }
}

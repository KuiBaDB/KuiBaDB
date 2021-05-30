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
use crate::access::redo::RedoState;
use crate::guc::{self, GucState};
use crate::utils::{persist, KBSystemTime, Xid};
use crate::{make_static, Oid};
use anyhow::anyhow;
use log;
use memoffset::offset_of;
use nix::libc::off_t;
use nix::sys::uio::{pread, IoVec};
use nix::unistd::SysconfVar::IOV_MAX;
use std::cmp::min;
use std::convert::{From, Into};
use std::fmt::Write;
use std::fs::{self, read_dir, File, OpenOptions};
use std::io::Read;
use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread::panicking;
use std::{debug_assert, debug_assert_eq};

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

#[derive(Debug)]
pub struct Ckpt {
    pub redo: Lsn,
    pub curtli: TimeLineID,
    pub prevtli: TimeLineID,
    pub nextxid: Xid,
    pub nextoid: Oid,
    pub time: KBSystemTime,
}

#[repr(C, packed(1))]
struct CkptSer {
    redo: u64,
    curtli: u32,
    prevtli: u32,
    nextxid: u64,
    nextoid: u32,
    time: u64,
}

impl From<&Ckpt> for CkptSer {
    fn from(v: &Ckpt) -> CkptSer {
        CkptSer {
            redo: v.redo.get(),
            curtli: v.curtli.get(),
            prevtli: v.prevtli.get(),
            nextxid: v.nextxid.get(),
            nextoid: v.nextoid.get(),
            time: v.time.into(),
        }
    }
}

impl From<&CkptSer> for Ckpt {
    fn from(v: &CkptSer) -> Ckpt {
        Ckpt {
            redo: Lsn::new(v.redo).unwrap(),
            curtli: TimeLineID::new(v.curtli).unwrap(),
            prevtli: TimeLineID::new(v.prevtli).unwrap(),
            nextxid: Xid::new(v.nextxid).unwrap(),
            nextoid: Oid::new(v.nextoid).unwrap(),
            time: v.time.into(),
        }
    }
}

pub fn new_ckpt_rec(ckpt: &Ckpt) -> Vec<u8> {
    let ckptser: CkptSer = ckpt.into();
    return start_record(&ckptser);
}

fn get_ckpt(recdata: &[u8]) -> Ckpt {
    unsafe { (&*(recdata.as_ptr() as *const CkptSer)).into() }
}

pub const KB_CTL_VER: u32 = 20130203;
pub const KB_CAT_VER: u32 = 20181218;
const CONTROL_FILE: &'static str = "global/kb_control";

#[derive(Debug)]
pub struct Ctl {
    pub time: KBSystemTime,
    pub ckpt: Lsn,
    pub ckptcpy: Ckpt,
}

#[repr(C, packed(1))]
struct CtlSer {
    ctlver: u32,
    catver: u32,
    time: u64,
    ckpt: u64,
    ckptcpy: CkptSer,
    crc32c: u32,
}

const CTLLEN: usize = size_of::<CtlSer>();

impl CtlSer {
    fn cal_crc32c(&self) -> u32 {
        unsafe {
            let ptr = self as *const _ as *const u8;
            let len = offset_of!(CtlSer, crc32c);
            let d = std::slice::from_raw_parts(ptr, len);
            crc32c::crc32c(d)
        }
    }

    fn persist(&self) -> anyhow::Result<()> {
        unsafe {
            let ptr = self as *const _ as *const u8;
            let d = std::slice::from_raw_parts(ptr, CTLLEN);
            persist(CONTROL_FILE, d)
        }
    }

    fn load() -> anyhow::Result<CtlSer> {
        let mut d = Vec::with_capacity(CTLLEN);
        File::open(CONTROL_FILE)?.read_to_end(&mut d)?;
        if d.len() != CTLLEN {
            Err(anyhow!("load: invalid control file. len={}", d.len()))
        } else {
            let ctl = unsafe { std::ptr::read(d.as_ptr() as *const CtlSer) };
            if ctl.ctlver != KB_CTL_VER {
                let v = ctl.ctlver;
                return Err(anyhow!("load: unexpected ctlver={}", v));
            }
            if ctl.catver != KB_CAT_VER {
                let v = ctl.catver;
                return Err(anyhow!("load: unexpected catver={}", v));
            }
            let v1 = ctl.cal_crc32c();
            if ctl.crc32c != v1 {
                let v = ctl.crc32c;
                return Err(anyhow!(
                    "load: unexpected crc32c. actual={} expected={}",
                    v,
                    v1
                ));
            }
            Ok(ctl)
        }
    }
}
impl Ctl {
    pub fn new(ckpt: Lsn, ckptcpy: Ckpt) -> Ctl {
        Ctl {
            time: KBSystemTime::now(),
            ckpt,
            ckptcpy,
        }
    }

    pub fn persist(&self) -> anyhow::Result<()> {
        let v: CtlSer = self.into();
        v.persist()
    }

    pub fn load() -> anyhow::Result<Ctl> {
        let ctlser = CtlSer::load()?;
        Ok((&ctlser).into())
    }
}

impl From<&Ctl> for CtlSer {
    fn from(v: &Ctl) -> Self {
        let mut ctlser = CtlSer {
            ctlver: KB_CTL_VER,
            catver: KB_CAT_VER,
            time: v.time.into(),
            ckpt: v.ckpt.get(),
            ckptcpy: (&v.ckptcpy).into(),
            crc32c: 0,
        };
        ctlser.crc32c = ctlser.cal_crc32c();
        ctlser
    }
}

impl From<&CtlSer> for Ctl {
    fn from(ctlser: &CtlSer) -> Ctl {
        Ctl {
            time: ctlser.time.into(),
            ckpt: Lsn::new(ctlser.ckpt).unwrap(),
            ckptcpy: (&ctlser.ckptcpy).into(),
        }
    }
}

pub trait Rmgr {
    fn name(&self) -> &'static str;
    fn redo(&mut self, hdr: &RecordHdr, data: &[u8], state: &mut RedoState) -> anyhow::Result<()>;
    fn desc(&self, out: &mut String, hdr: &RecordHdr, data: &[u8]);
    fn descstr(&self, hdr: &RecordHdr, data: &[u8]) -> String {
        let mut s = String::new();
        self.desc(&mut s, hdr, data);
        s
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone)]
pub enum RmgrId {
    Xlog,
    Xact,
}

impl From<u8> for RmgrId {
    fn from(v: u8) -> Self {
        if v == RmgrId::Xlog as u8 {
            RmgrId::Xlog
        } else if v == RmgrId::Xact as u8 {
            RmgrId::Xact
        } else {
            panic!("try from u8 to RmgrId failed. value={}", v)
        }
    }
}

pub trait WalStorageFile {
    fn pread(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<usize>;
    fn len(&self) -> u64;
}

pub trait WalStorageWalFile: WalStorageFile {
    fn lsn(&self) -> Lsn;
    fn tli(&self) -> TimeLineID;
}

struct LocalWalStorageFile {
    // Use BufReader<R>~
    file: File,
    filelen: u64,
}

impl LocalWalStorageFile {
    fn new(file: File, filelen: u64) -> LocalWalStorageFile {
        LocalWalStorageFile { file, filelen }
    }
}

impl WalStorageFile for LocalWalStorageFile {
    fn pread(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<usize> {
        Ok(pread(self.file.as_raw_fd(), buf, offset as off_t)?)
    }

    fn len(&self) -> u64 {
        self.filelen
    }
}

struct LocalWalStorageWalFile {
    file: LocalWalStorageFile,
    lsn: Lsn,
}

impl LocalWalStorageWalFile {
    fn new(file: File, len: u64, lsn: Lsn) -> LocalWalStorageWalFile {
        LocalWalStorageWalFile {
            file: LocalWalStorageFile::new(file, len),
            lsn,
        }
    }
}

impl WalStorageFile for LocalWalStorageWalFile {
    fn pread(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<usize> {
        self.file.pread(buf, offset)
    }
    fn len(&self) -> u64 {
        self.file.filelen
    }
}

impl WalStorageWalFile for LocalWalStorageWalFile {
    fn lsn(&self) -> Lsn {
        self.lsn
    }
    fn tli(&self) -> TimeLineID {
        TimeLineID::new(1).unwrap()
    }
}

pub trait WalStorage {
    fn find(&self, lsn: Lsn) -> anyhow::Result<(TimeLineID, Lsn)>;
    // fn open(&mut self, key: &str) -> anyhow::Result<Box<dyn WalStorageFile>>;
    fn open_wal(&mut self, tli: TimeLineID, lsn: Lsn)
        -> anyhow::Result<Box<dyn WalStorageWalFile>>;
    fn recycle(&mut self, lsn: Lsn) -> anyhow::Result<()>;
}

pub struct LocalWalStorage {}

impl LocalWalStorage {
    pub fn new() -> LocalWalStorage {
        LocalWalStorage {}
    }
}

fn lsn_in_file(filelsn: Lsn, len: u64, lsn: Lsn) -> bool {
    let lsn = lsn.get();
    let filelsn = filelsn.get();
    // shouldn't use lsn < (filelsn + len), filelsn + len may overflow.
    lsn >= filelsn && (lsn - filelsn) < len
}

impl WalStorage for LocalWalStorage {
    fn find(&self, lsn: Lsn) -> anyhow::Result<(TimeLineID, Lsn)> {
        for direntry in read_dir("kb_wal")? {
            let direntry = direntry?;
            let name = direntry.file_name();
            let name = name.as_os_str().as_bytes();
            if !is_wal(&name) {
                continue;
            }
            let (tli, filelsn) = parse_wal_filename(name);
            if tli.get() != 1 {
                continue;
            }
            let meta = direntry.metadata()?;
            debug_assert!(meta.is_file());
            let filelen = meta.len();
            if lsn_in_file(filelsn, filelen, lsn) {
                return Ok((tli, filelsn));
            }
        }
        return Err(anyhow!(
            "LocalWalStorage::find:  can not find the expected wal file. lsn={}",
            lsn
        ));
    }

    fn open_wal(
        &mut self,
        tli: TimeLineID,
        lsn: Lsn,
    ) -> anyhow::Result<Box<dyn WalStorageWalFile>> {
        let file = File::open(wal_filepath(tli, lsn))?;
        let filelen = file.metadata()?.len();
        Ok(Box::new(LocalWalStorageWalFile::new(file, filelen, lsn)))
    }

    fn recycle(&mut self, lsn: Lsn) -> anyhow::Result<()> {
        for direntry in read_dir("kb_wal")? {
            let direntry = direntry?;
            let name = direntry.file_name();
            let name = name.as_os_str().as_bytes();
            if !is_wal(&name) {
                continue;
            }
            let (tli, filelsn) = parse_wal_filename(name);
            if tli.get() != 1 {
                continue;
            }
            if filelsn >= lsn {
                let path = direntry.path();
                log::info!(
                    "LocalWalStorage::recycle: remove wal file. path={:?} lsn={}",
                    path,
                    lsn
                );
                fs::remove_file(direntry.path())?;
                continue;
            }
            let meta = direntry.metadata()?;
            debug_assert!(meta.is_file());
            let filelen = meta.len();
            let lsnlen = lsn.get() - filelsn.get();
            if lsnlen >= filelen {
                continue;
            }
            let path = direntry.path();
            log::info!("LocalWalStorage::recycle: truncate wal file. path={:?} lsn={} filelen={} lsnlen={}", path, lsn, filelen, lsnlen);
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(lsnlen)?;
        }
        Ok(())
    }
}

pub struct WalReader {
    pub storage: Box<dyn WalStorage>,
    pub readlsn: Option<Lsn>,
    pub endlsn: Lsn,
    file: Option<Box<dyn WalStorageWalFile>>,
}

impl WalReader {
    pub fn new(storage: Box<dyn WalStorage>, startlsn: Lsn) -> WalReader {
        WalReader {
            storage,
            readlsn: None,
            endlsn: startlsn,
            file: None,
        }
    }

    pub fn rescan(&mut self, _startlsn: Lsn) {
        unimplemented!()
    }

    fn open_file(&mut self) -> anyhow::Result<u64> {
        let mut endlsn_is_filelsn = false;
        if let Some(ref file) = self.file {
            let filelsn = file.lsn();
            let filelen = file.len();
            if self.endlsn >= filelsn {
                let endlsnlen = self.endlsn.get() - filelsn.get();
                if endlsnlen < filelen {
                    return Ok(endlsnlen);
                }
                if endlsnlen == filelen {
                    endlsn_is_filelsn = true;
                }
            }
        }
        let (tli, filelsn, endlsnlen) = if endlsn_is_filelsn {
            (TimeLineID::new(1).unwrap(), self.endlsn, 0 as u64)
        } else {
            let (tli, filelsn) = self.storage.find(self.endlsn)?;
            (tli, filelsn, self.endlsn.get() - filelsn.get())
        };
        self.file = Some(self.storage.open_wal(tli, filelsn)?);
        Ok(endlsnlen)
    }

    pub fn read_record(&mut self) -> anyhow::Result<(RecordHdr, Vec<u8>)> {
        let recoff = self.open_file()?;
        let file = self.file.as_ref().unwrap();
        let mut hdrbytes = [0; RECHDRLEN];
        let hdrlen = file.pread(&mut hdrbytes, recoff)?;
        if hdrlen != RECHDRLEN {
            return Err(anyhow!("WalReader::read_record: cannot read RecordHdr. readlen={} filetli={} filelsn={} filelen={} recoff={}",
                hdrlen, file.tli(), file.lsn(), file.len(), recoff));
        }
        let rechdrser = hdr(&hdrbytes);
        let rechdr: RecordHdr = rechdrser.into();
        if let Some(prevlsn) = self.readlsn {
            if let Some(recprevlsn) = rechdr.prev {
                if prevlsn != recprevlsn {
                    return Err(anyhow!("WalReader::read_record: unexpected prevlsn. expected={} actual={} filetli={} filelsn={} filelen={} recoff={}", prevlsn, recprevlsn, file.tli(), file.lsn(), file.len(), recoff));
                }
            } else {
                return Err(anyhow!("WalReader::read_record: no prevlsn. expected={} filetli={} filelsn={} filelen={} recoff={}", prevlsn, file.tli(), file.lsn(), file.len(), recoff));
            }
        }
        let recdatlen = rechdr.totlen as usize - RECHDRLEN;
        let mut databytes = Vec::<u8>::with_capacity(recdatlen);
        databytes.resize(recdatlen, 0); // there is no need to do the zeroing.
        let readlen = file.pread(&mut databytes, recoff + RECHDRLEN as u64)?;
        if recdatlen != readlen {
            return Err(anyhow!("WalReader::read_record: cannot read data. readlen={} reclen={} filetli={} filelsn={} filelen={} recoff={}",
                readlen, recdatlen, file.tli(), file.lsn(), file.len(), recoff));
        }
        let crc = crc32c::crc32c(&databytes);
        let crc = crc32c::crc32c_append(crc, hdr_crc_area(&hdrbytes));
        let actual_crc = rechdrser.crc32c;
        if actual_crc != crc {
            return Err(anyhow!("WalReader::read_record: unexpected crc. expected={} actual={} filetli={} filelsn={} filelen={} recoff={}",
                crc, actual_crc, file.tli(), file.lsn(), file.len(), recoff));
        }
        self.readlsn = Some(self.endlsn);
        self.endlsn = Lsn::new(self.endlsn.get() + rechdr.totlen as u64).unwrap();
        Ok((rechdr, databytes))
    }

    pub fn endtli(&self) -> TimeLineID {
        TimeLineID::new(1).unwrap()
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

// The lsn for the first record is 0x0133F0E2
pub type Lsn = NonZeroU64;
pub type TimeLineID = NonZeroU32;

struct WritingWalFile {
    fd: File,
    start_lsn: Lsn,
    write: &'static Progress,
    flush: &'static Progress,
}

const WAL_FILENAME_LEN: usize = 8 + 16 + 4;
fn wal_filepath(tli: TimeLineID, lsn: Lsn) -> String {
    format!("kb_wal/{:0>8X}{:0>16X}.wal", tli, lsn)
}

fn is_wal(filename: &[u8]) -> bool {
    filename.len() == WAL_FILENAME_LEN && filename.ends_with(&[b'.', b'w', b'a', b'l'])
}

fn parse_tli(v: &[u8]) -> TimeLineID {
    debug_assert_eq!(v.len(), 8);
    let n = u32::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap();
    TimeLineID::new(n).unwrap()
}

fn parse_lsn(v: &[u8]) -> Lsn {
    debug_assert_eq!(v.len(), 16);
    let n = u64::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap();
    Lsn::new(n).unwrap()
}

fn parse_wal_filename(filename: &[u8]) -> (TimeLineID, Lsn) {
    debug_assert!(is_wal(filename));
    (parse_tli(&filename[..8]), parse_lsn(&filename[8..24]))
}

#[cfg(test)]
mod parse_wal_filepath_test {
    use super::{parse_wal_filename, wal_filepath, Lsn, TimeLineID};
    #[test]
    fn f() {
        let tli = TimeLineID::new(0x20181218).unwrap();
        let lsn = Lsn::new(0x2013020320181218).unwrap();
        let fp = wal_filepath(tli, lsn);
        assert_eq!(fp, "kb_wal/201812182013020320181218.wal");
        assert_eq!(parse_wal_filename(&fp.as_bytes()[7..]), (tli, lsn));

        let tli = TimeLineID::new(1).unwrap();
        let lsn = Lsn::new(20181218).unwrap();
        let fp = wal_filepath(tli, lsn);
        assert_eq!(fp, "kb_wal/00000001000000000133F0E2.wal");
        assert_eq!(parse_wal_filename(&fp.as_bytes()[7..]), (tli, lsn));
    }
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

#[derive(Copy, Clone)]
pub struct RecordHdr {
    pub totlen: u32,
    pub info: u8,
    pub id: RmgrId,
    pub xid: Option<Xid>,
    pub prev: Option<Lsn>,
}

impl RecordHdr {
    pub fn rmgr_info(&self) -> u8 {
        self.info & 0xf0
    }
}

#[repr(C, packed(1))]
struct RecordHdrSer {
    totlen: u32,
    info: u8,
    id: u8,
    xid: u64,
    prev: u64,
    crc32c: u32,
}
const RECHDRLEN: usize = size_of::<RecordHdrSer>();

fn mut_hdr(d: &mut [u8]) -> &mut RecordHdrSer {
    unsafe { &mut *(d.as_mut_ptr() as *mut RecordHdrSer) }
}

fn hdr(d: &[u8]) -> &RecordHdrSer {
    unsafe { &*(d.as_ptr() as *mut RecordHdrSer) }
}

fn hdr_crc_area(rec: &[u8]) -> &[u8] {
    &rec[..offset_of!(RecordHdrSer, crc32c)]
}

fn data_area(rec: &[u8]) -> &[u8] {
    &rec[RECHDRLEN..]
}

impl std::convert::From<&RecordHdrSer> for RecordHdr {
    fn from(f: &RecordHdrSer) -> Self {
        RecordHdr {
            totlen: f.totlen,
            info: f.info,
            id: f.id.into(),
            xid: Xid::new(f.xid),
            prev: Lsn::new(f.prev),
        }
    }
}

pub fn start_record<T>(val: &T) -> Vec<u8> {
    let mut record = Vec::<u8>::with_capacity(RECHDRLEN + size_of::<T>());
    record.resize(RECHDRLEN, 0);
    unsafe {
        let ptr = val as *const T as *const u8;
        let d = std::slice::from_raw_parts(ptr, size_of::<T>());
        record.extend_from_slice(d);
    }
    return record;
}

pub fn finish_record(d: &mut [u8], id: RmgrId, info: u8, xid: Option<Xid>) {
    let len = d.len();
    if len > u32::MAX as usize || len < RECHDRLEN {
        panic!(
            "invalid record in finish_record(). len={} id={:?} info={} xid={:?}",
            len, id, info, xid
        );
    }
    let crc = crc32c::crc32c(data_area(d));
    let len = len as u32;
    let hdr = mut_hdr(d);
    hdr.totlen = len;
    hdr.info = info;
    hdr.id = id as u8;
    hdr.xid = match xid {
        None => 0,
        Some(x) => x.get(),
    };
    hdr.prev = 0;
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
        hdr.prev = match prevlsn {
            None => 0,
            Some(p) => p.get(),
        };
        let bodycrc = hdr.crc32c;
        let crc = crc32c::crc32c_append(bodycrc, hdr_crc_area(record));
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
        let flush: &'static Progress = make_static(Progress::new(lsn.get()));
        let write: &'static Progress = make_static(Progress::new(lsn.get()));
        Ok(make_static(GlobalStateExt {
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
        }))
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
        let insert_res = {
            let mut state = self.get_insert_state();
            state.insert(r)
        };
        self.handle_insert_ret(insert_res)
    }

    pub fn try_insert_record(&self, r: RecordBuff, page_lsn: Lsn) -> Option<Lsn> {
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
    Ckpt = 0x10,
}

impl From<u8> for XlogInfo {
    fn from(value: u8) -> Self {
        if value == XlogInfo::Ckpt as u8 {
            XlogInfo::Ckpt
        } else {
            panic!("try from u8 to XlogInfo failed. value={}", value)
        }
    }
}

pub struct XlogRmgr {}

impl XlogRmgr {
    pub fn new() -> XlogRmgr {
        XlogRmgr {}
    }
}

impl Rmgr for XlogRmgr {
    fn name(&self) -> &'static str {
        "XLOG"
    }

    fn redo(&mut self, hdr: &RecordHdr, data: &[u8], state: &mut RedoState) -> anyhow::Result<()> {
        match hdr.rmgr_info().into() {
            XlogInfo::Ckpt => {
                let ckpt = get_ckpt(data);
                state.set_nextxid(ckpt.nextxid);
                Ok(())
            }
        }
    }

    fn desc(&self, out: &mut String, hdr: &RecordHdr, data: &[u8]) {
        match hdr.rmgr_info().into() {
            XlogInfo::Ckpt => {
                let ckpt = get_ckpt(data);
                write!(out, "CHECKPOINT {:?}", ckpt).unwrap();
            }
        }
    }
}

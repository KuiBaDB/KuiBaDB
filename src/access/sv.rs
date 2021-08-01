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
use crate::access::ckpt::PendingFileOps;
use crate::access::wal::{self, Lsn, RmgrId};
use crate::access::xact::SessionExt as xactSessionExt;
use crate::utils::marc::{Destory, Marc};
use crate::utils::sb::{self, SharedBuffer};
use crate::utils::{persist, ser, SessionState};
use crate::{FileId, Oid};
use anyhow::ensure;
use byteorder::{NativeEndian, ReadBytesExt};
use std::fs::{self, OpenOptions};
use std::io::{Cursor, Seek, SeekFrom};
use std::mem::{self, size_of, size_of_val};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

struct L0File {
    meta: FileMeta,
    inuse: AtomicBool,
}

impl std::clone::Clone for L0File {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta,
            inuse: AtomicBool::new(self.inuse.load(Relaxed)),
        }
    }
}

impl L0File {
    fn new(fileid: FileId) -> Self {
        Self {
            meta: FileMeta::new(fileid, 0, 0),
            inuse: AtomicBool::new(true),
        }
    }

    fn start_use(&self) -> bool {
        return self
            .inuse
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_ok();
    }

    fn abort_use(&self) {
        debug_assert!(self.inuse.load(Relaxed));
        self.inuse.store(false, Relaxed);
    }

    fn commit_use(&mut self, row: u32, len: u64) {
        self.meta.len = len;
        self.meta.rownum = row;
        let inuse = self.inuse.get_mut();
        debug_assert!(*inuse);
        *inuse = false;
    }
}

#[derive(Eq, Hash, Copy, Clone, Debug, PartialEq)]
pub struct TableId {
    pub db: Oid,
    pub table: Oid,
}

pub struct SVDestoryCtx {
    tableid: TableId,
    pending_ops: &'static PendingFileOps,
}

impl SVDestoryCtx {
    fn new(tableid: TableId, pending_ops: &'static PendingFileOps) -> Self {
        Self {
            tableid,
            pending_ops,
        }
    }
}

fn get_dir(table: TableId) -> String {
    format!("base/{}/{}", table.db, table.table)
}

pub fn get_datafile_path(table: TableId, fileid: FileId) -> String {
    return format!("base/{}/{}/{}.d", table.db, table.table, fileid); // .data
}

pub fn get_mvccfile_path(table: TableId, fileid: FileId) -> String {
    return format!("base/{}/{}/{}.M", table.db, table.table, fileid); // .mvcc
}

pub fn get_minafest_path(db: Oid, table: Oid) -> String {
    format!("base/{}/{}/manifest", db, table)
}

impl Destory for L0File {
    type DestoryCtx = SVDestoryCtx;
    fn destory(&mut self, ctx: &Self::DestoryCtx) {
        let p = get_datafile_path(ctx.tableid, self.meta.fileid);
        ctx.pending_ops.unlink(p);
    }
}

struct ImmFile {
    fileid: FileId,
    rownum: u32,
    len: u64,
}

unsafe impl Sync for ImmFile {}
unsafe impl Send for ImmFile {}

impl Destory for ImmFile {
    type DestoryCtx = SVDestoryCtx;
    fn destory(&mut self, ctx: &Self::DestoryCtx) {
        let p = get_datafile_path(ctx.tableid, self.fileid);
        ctx.pending_ops.unlink(p);
    }
}

// length of l0/l1/l2 may be 0.
#[derive(Clone)]
pub struct SupVer {
    l0: Vec<L0File>,
    l1: Vec<Marc<ImmFile>>,
    l2: Vec<Marc<ImmFile>>,
    nextid: u32,
    lsn: Option<Lsn>,
    enable_cs_wal: bool,
}

impl SupVer {
    fn find_l0(&self, fileid: FileId) -> Option<usize> {
        debug_assert!(is_sorted_by_fileid(&self.l0, |f| f.meta.fileid));
        match self.l0.binary_search_by_key(&fileid, |f| f.meta.fileid) {
            Ok(idx) => Some(idx),
            Err(_) => None,
        }
    }
}

fn unref(files: Vec<Marc<ImmFile>>, ctx: &SVDestoryCtx) {
    for file in files {
        file.unref(ctx);
    }
}

impl Destory for SupVer {
    type DestoryCtx = SVDestoryCtx;
    fn destory(&mut self, ctx: &Self::DestoryCtx) {
        unref(mem::take(&mut self.l1), ctx);
        unref(mem::take(&mut self.l2), ctx);
    }
}

#[derive(Clone, Copy)]
pub struct FileMeta {
    pub len: u64,
    pub fileid: FileId,
    pub rownum: u32,
}

impl FileMeta {
    fn new(fileid: FileId, rownum: u32, len: u64) -> Self {
        Self {
            fileid,
            rownum,
            len,
        }
    }

    // #[cfg(debug_assertions)]
    fn is_valid(&self) -> bool {
        if self.rownum == 0 {
            return self.len == 0;
        }
        return self.len > 0;
    }

    fn is_empty(&self) -> bool {
        debug_assert!(self.is_valid());
        return self.rownum == 0;
    }
}

const MANIFEST_VER: u32 = 20181218;
pub const INIT_MANIFEST_DAT: [u8; 28] = [
    // ver: 4bytes
    0xe2, 0xf0, 0x33, 0x01, // lsn: 8bytes
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // crc: 4bytes
    0x00, 0x00, 0x00, 0x00, // the number of l0files.
    0x00, 0x00, 0x00, 0x00, // the number of l1files.
    0x00, 0x00, 0x00, 0x00, // the number of l2files.
    0x47, 0xde, 0xb3, 0xb1,
];

fn read_level_files<T>(
    cursor: &mut Cursor<&[u8]>,
    mut on_file: impl FnMut(u32, u64, u32) -> T,
) -> anyhow::Result<Vec<T>> {
    let numl0 = cursor.read_u32::<NativeEndian>()?;
    let mut l0files = Vec::with_capacity(numl0 as usize);
    for _i in 0..numl0 {
        let fileid = cursor.read_u32::<NativeEndian>()?;
        let filelen = cursor.read_u64::<NativeEndian>()?;
        let rownum = cursor.read_u32::<NativeEndian>()?;
        l0files.push(on_file(fileid, filelen, rownum));
    }
    return Ok(l0files);
}

fn read_manifest(path: &str, enable_cs_wal: bool) -> anyhow::Result<SupVer> {
    let mdata = fs::read(path)?;
    let mdata: &[u8] = &mdata;
    ensure!(
        mdata.len() >= size_of_val(&INIT_MANIFEST_DAT),
        "read_manifest: invalid manifest: path={} mdata={:?}",
        path,
        mdata
    );

    let crcidx = mdata.len() - size_of::<u32>();
    let expect_crc = crc32c::crc32c(&mdata[..crcidx]);

    let mut cursor = Cursor::new(mdata);
    cursor.seek(SeekFrom::Start(crcidx as u64))?;
    let actual_crc = cursor.read_u32::<NativeEndian>()?;
    ensure!(
        actual_crc == expect_crc,
        "read_manifest failed. path={} expect_crc={} actual_crc={}",
        path,
        expect_crc,
        actual_crc
    );

    cursor.seek(SeekFrom::Start(0))?;
    let ver = cursor.read_u32::<NativeEndian>()?;
    ensure!(
        ver == MANIFEST_VER,
        "read_manifest failed. path={} expect_ver={} actual_ver={}",
        path,
        MANIFEST_VER,
        ver
    );

    let lsn = Lsn::new(cursor.read_u64::<NativeEndian>()?);

    let mut newestid = 0u32;
    let l0files = read_level_files(&mut cursor, |fileid, filelen, rownum| {
        if fileid > newestid {
            newestid = fileid;
        }
        L0File {
            meta: FileMeta::new(FileId::new(fileid).unwrap(), rownum, filelen),
            inuse: AtomicBool::new(false),
        }
    })?;
    let l1files = read_level_files(&mut cursor, |fileid, filelen, rownum| {
        if fileid > newestid {
            newestid = fileid;
        }
        Marc::new(ImmFile {
            fileid: FileId::new(fileid).unwrap(),
            len: filelen,
            rownum,
        })
    })?;
    let l2files = read_level_files(&mut cursor, |fileid, filelen, rownum| {
        if fileid > newestid {
            newestid = fileid;
        }
        Marc::new(ImmFile {
            fileid: FileId::new(fileid).unwrap(),
            len: filelen,
            rownum,
        })
    })?;
    let nextid = newestid + 1;
    ensure!(nextid != 0, "read_manifest: nextid is 0. path={}", path);
    return Ok(SupVer {
        l0: l0files,
        l1: l1files,
        l2: l2files,
        nextid,
        lsn,
        enable_cs_wal,
    });
}

fn write_level_files<T>(
    files: &Vec<T>,
    out: &mut Vec<u8>,
    on_file: impl Fn(&T) -> (u32, u64, u32),
) {
    ser::ser_u32(out, files.len() as u32);
    for file in files {
        let (fileid, filelen, rownum) = on_file(file);
        ser::ser_u32(out, fileid);
        ser::ser_u64(out, filelen);
        ser::ser_u32(out, rownum);
    }
    return;
}

fn write_manifest(path: &str, sv: &SupVer) -> anyhow::Result<()> {
    let mut data = Vec::new();
    ser::ser_u32(&mut data, MANIFEST_VER);
    let lsn = match sv.lsn {
        Some(v) => v.get(),
        None => 0,
    };
    ser::ser_u64(&mut data, lsn);

    write_level_files(&sv.l0, &mut data, |file: &L0File| {
        (file.meta.fileid.get(), file.meta.len, file.meta.rownum)
    });
    let on_file = |file: &Marc<ImmFile>| (file.fileid.get(), file.len, file.rownum);
    write_level_files(&sv.l1, &mut data, on_file);
    write_level_files(&sv.l2, &mut data, on_file);

    let crc = crc32c::crc32c(&data);
    ser::ser_u32(&mut data, crc);

    return persist(path, &data);
}

pub struct SVCommonData {
    pending_ops: &'static PendingFileOps,
    walapi: Option<&'static wal::GlobalStateExt>,
}

impl SVCommonData {
    pub fn new(
        pending_ops: &'static PendingFileOps,
        walapi: Option<&'static wal::GlobalStateExt>,
    ) -> Self {
        Self {
            pending_ops,
            walapi,
        }
    }
}

impl sb::Value for Marc<SupVer> {
    type CommonData = SVCommonData;
    type LoadCtx = bool;
    type K = TableId;

    fn load(k: &Self::K, ctx2: &Self::LoadCtx, _ctx: &Self::CommonData) -> anyhow::Result<Self> {
        Ok(Marc::new(read_manifest(
            &get_minafest_path(k.db, k.table),
            *ctx2,
        )?))
    }

    fn store(&self, k: &Self::K, ctx: &Self::CommonData, _force: bool) -> anyhow::Result<()> {
        if let Some(walapi) = ctx.walapi {
            if let Some(pagelsn) = self.lsn {
                walapi.fsync(pagelsn);
            }
        }

        let manifestpath = get_minafest_path(k.db, k.table);
        write_manifest(&manifestpath, self)?;

        if self.enable_cs_wal {
            for l0file in &self.l0 {
                if !l0file.meta.is_empty() {
                    ctx.pending_ops
                        .fsync(get_datafile_path(*k, l0file.meta.fileid));
                }
            }
        }
        return Ok(());
    }
}

pub type TabSupVer = SharedBuffer<Marc<SupVer>, sb::LRUPolicy>;

pub type SBSlot = sb::Slot<Marc<SupVer>, sb::LRUPolicy>;

// #[cfg(debug_assertions)]
fn is_sorted_by_fileid<T, F: Fn(&T) -> FileId>(files: &[T], f: F) -> bool {
    let mut prev = 0u32;
    for file in files {
        let fileid = f(file).get();
        if fileid > prev {
            prev = fileid;
        } else {
            return false;
        }
    }
    return true;
}

fn alloc_from(output: &mut Vec<FileMeta>, num: usize, input: &[L0File]) -> bool {
    debug_assert!(is_sorted_by_fileid(input, |f| f.meta.fileid));
    for l0file in input {
        if !l0file.start_use() {
            continue;
        }
        output.push(l0file.meta);
        if output.len() >= num {
            return true;
        }
    }
    return false;
}

const CREATE_L0FILE: u8 = 0;
#[repr(C, packed(1))]
struct CreateL0File {
    db: u32,
    table: u32,
    startid: u32,
    endid: u32,
}

const UPDATE_L0FILE: u8 = 1;
fn do_ser_update_l0file(out: &mut Vec<u8>, table: TableId, files: &[FileMeta]) {
    ser::ser_u32(out, table.db.get());
    ser::ser_u32(out, table.table.get());
    for file in files {
        ser::ser_u32(out, file.fileid.get());
        ser::ser_u32(out, file.rownum);
        ser::ser_u64(out, file.len);
    }
    return;
}

fn ser_update_l0file(out: &mut Vec<u8>, table: TableId, files: &[FileMeta]) {
    do_ser_update_l0file(out, table, files);
}

fn insert_create_l0file_wal(
    sess: &mut SessionState,
    table: TableId,
    startid: u32,
    endid: u32,
) -> Lsn {
    let walrec = CreateL0File {
        db: table.db.get(),
        table: table.table.get(),
        startid,
        endid,
    };
    let waldat = wal::start_record(&walrec);
    return sess.insert_record(RmgrId::SV, CREATE_L0FILE, waldat);
}

fn alloc_new_l0files(sess: &mut SessionState, slot: &SBSlot, sv: &mut Marc<SupVer>, endid: u32) {
    let svctx = SVDestoryCtx::new(slot.k, sess.pending_fileops);
    let sv = sv.make_mut(&svctx);
    for fid in sv.nextid..endid {
        sv.l0.push(L0File::new(FileId::new(fid).unwrap()));
    }
    let startid = sv.nextid;
    sv.nextid = endid;
    slot.mark_dirty();
    let lsn = insert_create_l0file_wal(sess, slot.k, startid, endid);
    sv.lsn = Some(lsn);
    return;
}

fn do_alloc_l0file(sess: &mut SessionState, slot: &SBSlot, num: usize) -> Vec<FileMeta> {
    let mut files = Vec::with_capacity(num);
    {
        let sv = slot.v.read().unwrap();
        let sv: &Marc<SupVer> = sv.as_ref().unwrap();
        if alloc_from(&mut files, num, &sv.l0) {
            return files;
        }
    }
    let (nextid, endid) = {
        let mut sv = slot.v.write().unwrap();
        let sv: &mut Marc<SupVer> = sv.as_mut().unwrap();
        if alloc_from(&mut files, num, &sv.l0) {
            return files;
        }
        let ncreate = num - files.len();
        let nextid = sv.nextid;
        let endid = nextid + ncreate as u32;
        alloc_new_l0files(sess, slot, sv, endid);
        debug_assert_eq!(endid, sv.nextid);
        (nextid, endid)
    };
    for fileid in nextid..endid {
        files.push(FileMeta::new(FileId::new(fileid).unwrap(), 0, 0));
    }
    debug_assert_eq!(files.len(), num);
    return files;
}

fn create_l0file(table: TableId, fileid: FileId) -> anyhow::Result<()> {
    let path = get_datafile_path(table, fileid);
    OpenOptions::new().create(true).write(true).open(path)?;
    let path = get_mvccfile_path(table, fileid);
    OpenOptions::new().create(true).write(true).open(path)?;
    return Ok(());
}

pub struct AbortWriteGuard<'a, 'b> {
    slot: &'a SBSlot,
    files: &'b [FileMeta],
}

impl<'a, 'b> AbortWriteGuard<'a, 'b> {
    pub fn new(slot: &'a SBSlot, files: &'b [FileMeta]) -> Self {
        Self { slot, files }
    }
}

impl<'a, 'b> Drop for AbortWriteGuard<'a, 'b> {
    fn drop(&mut self) {
        abort_write(self.slot, self.files);
    }
}

pub fn abort_write(slot: &SBSlot, files: &[FileMeta]) {
    let sv = slot.v.read().unwrap();
    let sv: &Marc<SupVer> = sv.as_ref().unwrap();
    for file in files {
        let idx = sv.find_l0(file.fileid).unwrap();
        sv.l0[idx].abort_use();
    }
    return;
}

pub fn commit_write(sess: &mut SessionState, slot: &SBSlot, files: &[FileMeta]) {
    let svctx = SVDestoryCtx::new(slot.k, sess.pending_fileops);
    let mut waldat = wal::start_record_raw(&[]);
    debug_assert!(waldat.len() > 0);
    ser_update_l0file(&mut waldat, slot.k, files);

    let mut sv = slot.v.write().unwrap(); // lock guard
    let sv: &mut Marc<SupVer> = sv.as_mut().unwrap();
    let sv = sv.make_mut(&svctx);
    for file in files {
        let idx = sv.find_l0(file.fileid).unwrap();
        sv.l0[idx].commit_use(file.rownum, file.len);
    }
    slot.mark_dirty();
    let lsn = sess.insert_record(RmgrId::SV, UPDATE_L0FILE, waldat);
    sv.lsn = Some(lsn);
    return;
}

pub fn start_write(
    sess: &mut SessionState,
    slot: &SBSlot,
    parallel: usize,
) -> anyhow::Result<Vec<FileMeta>> {
    let files = do_alloc_l0file(sess, slot, parallel);
    let _guard = AbortWriteGuard::new(slot, &files);
    for file in &files {
        if !file.is_empty() {
            continue;
        }
        create_l0file(slot.k, file.fileid)?;
    }
    sess.pending_fileops.fsync(get_dir(slot.k));
    mem::forget(_guard);
    return Ok(files);
}

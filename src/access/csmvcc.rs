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
use crate::access::fd;
use crate::access::sv::{get_mvccfile_path, TableId};
use crate::access::wal::{self, Lsn, RmgrId};
use crate::access::xact::WorkerExt as XACTWorkerExt;
use crate::utils::sb::{self, FIFOPolicy, SharedBuffer, Value};
use crate::utils::Xid;
use crate::utils::{alloc, dealloc};
use crate::utils::{pwritevn, WorkerState};
use crate::FileId;
use anyhow::ensure;
use nix::libc::off_t;
use nix::sys::uio::pread;
use nix::sys::uio::IoVec;
use static_assertions::const_assert;
use std::mem::{align_of, size_of};
use std::os::unix::io::AsRawFd;
use std::ptr::NonNull;
use std::slice;

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct PageId {
    fileid: FileId,
    blkid: u32,
}

pub struct Page(NonNull<u8>, usize);

impl Page {
    fn new(blkrows: u64) -> Self {
        let blk_size = get_blk_size(blkrows);
        debug_assert!(blk_size <= isize::MAX as usize);
        Page(alloc(blk_size, align_of::<u64>()), blk_size)
    }

    // After the load(), CRC does not need to be consistent with data.
    // We always calculate CRC in the store().
    fn crc(&self) -> u32 {
        debug_assert_eq!(self.0.as_ptr() as usize % align_of::<u64>(), 0);
        debug_assert!(self.1 >= 8);
        let val = unsafe { *self.0.cast::<u32>().as_ptr() };
        return val;
    }

    fn as_bytes_from(&self, s: isize) -> &[u8] {
        debug_assert!(self.1 <= isize::MAX as usize);
        debug_assert!(self.1 >= s as usize);
        return unsafe { slice::from_raw_parts(self.0.as_ptr().offset(s), self.1 - s as usize) };
    }

    fn calc_crc32c(&self) -> u32 {
        debug_assert!(self.blk_rows() > 0);
        debug_assert!(self.1 <= isize::MAX as usize);
        return crc32c::crc32c(self.data_as_bytes());
    }

    fn blk_rows(&self) -> u32 {
        debug_assert_eq!(self.0.as_ptr() as usize % align_of::<u64>(), 0);
        debug_assert!(self.1 >= 8);
        let val = unsafe { *self.0.cast::<u32>().as_ptr().offset(1) };
        debug_assert_eq!(get_blk_size(val as u64), self.1);
        return val;
    }

    fn set_blk_rows(&mut self, blkrows: u32) {
        debug_assert_eq!(self.0.as_ptr() as usize % align_of::<u64>(), 0);
        debug_assert!(self.1 >= 8);
        unsafe { *self.0.cast::<u32>().as_ptr().offset(1) = blkrows };
        debug_assert_eq!(self.blk_rows(), blkrows);
        return;
    }

    fn as_mut_bytes(&mut self) -> &mut [u8] {
        debug_assert!(self.1 <= isize::MAX as usize);
        return unsafe { slice::from_raw_parts_mut(self.0.as_ptr(), self.1) };
    }

    fn as_bytes(&self) -> &[u8] {
        self.as_bytes_from(0)
    }

    fn data_as_bytes(&self) -> &[u8] {
        self.as_bytes_from(4)
    }

    fn init(&mut self, blkrows: u32) {
        self.as_mut_bytes().fill(0);
        self.set_blk_rows(blkrows);
        return;
    }

    fn lsn(&self) -> Option<Lsn> {
        debug_assert_eq!(self.0.as_ptr() as usize % align_of::<u64>(), 0);
        debug_assert!(self.1 >= 16);
        let val = unsafe { *self.0.cast::<u64>().as_ptr().offset(1) };
        return Lsn::new(val);
    }

    fn set_lsn(&mut self, lsn: Lsn) {
        debug_assert_eq!(self.0.as_ptr() as usize % align_of::<u64>(), 0);
        debug_assert!(self.1 >= 16);
        unsafe { *self.0.cast::<u64>().as_ptr().offset(1) = lsn.get() };
        debug_assert_eq!(self.lsn(), Some(lsn));
        return;
    }

    fn xmin_as_mut_slice(&mut self, sidx: isize, len: usize) -> &mut [u64] {
        debug_assert!(sidx >= 0);
        debug_assert!((sidx as usize + len) <= self.blk_rows() as usize);
        debug_assert_eq!(self.0.as_ptr() as usize % align_of::<u64>(), 0);
        unsafe {
            let sptr = self.0.cast::<u64>().as_ptr().offset(2 + sidx);
            slice::from_raw_parts_mut(sptr, len)
        }
    }

    fn set_xmin(&mut self, sidx: isize, len: usize, xid: Xid) {
        self.xmin_as_mut_slice(sidx, len).fill(xid.get());
        return;
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        dealloc(self.0, self.1, align_of::<u64>());
    }
}

pub struct PageCtx {
    tableid: TableId,
    blk_rows: u32,
    pending_ops: &'static PendingFileOps,
    walapi: Option<&'static wal::GlobalStateExt>,
}

const_assert!(size_of::<usize>() == size_of::<u64>());
fn get_blk_size(rows: u64) -> usize {
    const XIDSIZE: usize = size_of::<Xid>();
    let rows = rows as usize;
    let infomasksize = (rows + 3) / 4 /* XACTS_PER_BYTE */;
    return 4 + /* page crc32c */
        4 + /* blk_rows */
        8 /* page lsn */ +
        XIDSIZE * rows +  /* xmin */
        XIDSIZE * rows +  /* xmax */
        infomasksize +  /* xmin infomask */
        infomasksize; /* xmax infomask */
}

impl Value for Page {
    type K = PageId;
    type LoadCtx = ();
    type CommonData = PageCtx;

    fn load(k: &Self::K, _lctx: &Self::LoadCtx, ctx: &Self::CommonData) -> anyhow::Result<Self> {
        let mut page = Page::new(ctx.blk_rows as u64);
        let off = page.1 * k.blkid as usize;
        debug_assert!(off <= off_t::MAX as usize);
        let filepath = get_mvccfile_path(ctx.tableid.db, ctx.tableid.table, k.fileid);
        let readsize = fd::use_file(&filepath, |mvccfile| -> anyhow::Result<usize> {
            return Ok(pread(
                mvccfile.as_raw_fd(),
                page.as_mut_bytes(),
                off as off_t,
            )?);
        })?;
        if readsize == 0 {
            page.init(ctx.blk_rows);
            return Ok(page);
        }
        ensure!(
            readsize == page.1,
            "Page::load failed. unexpected readsize. r={} e={}",
            page.1,
            readsize
        );
        let ecrc = page.calc_crc32c();
        ensure!(
            page.crc() == ecrc,
            "Page::load failed. invalid crc. e={} r={}",
            page.crc(),
            ecrc
        );
        ensure!(
            page.blk_rows() == ctx.blk_rows,
            "Page::load failed. invalid blk_rows. e={} r={}",
            page.blk_rows(),
            ctx.blk_rows
        );
        return Ok(page);
    }

    fn store(&self, k: &Self::K, ctx: &Self::CommonData, _force: bool) -> anyhow::Result<()> {
        debug_assert_eq!(self.blk_rows(), ctx.blk_rows);
        debug_assert!(self.lsn().is_some());
        if let Some(walapi) = ctx.walapi {
            if let Some(pagelsn) = self.lsn() {
                walapi.fsync(pagelsn);
            }
        }
        let crcval = self.calc_crc32c().to_ne_bytes();
        let mut iovec = [
            IoVec::from_slice(&crcval),
            IoVec::from_slice(self.data_as_bytes()),
        ];
        let off = self.1 * k.blkid as usize;
        debug_assert!(off <= off_t::MAX as usize);
        let filepath = get_mvccfile_path(ctx.tableid.db, ctx.tableid.table, k.fileid);
        let wsize = fd::use_file(&filepath, |mvccfile| -> anyhow::Result<usize> {
            return Ok(pwritevn(mvccfile.as_raw_fd(), &mut iovec, off as off_t)?);
        })?;
        ensure!(
            wsize == self.1,
            "Page::store: invalid wsize: r={} e={}",
            self.1,
            wsize
        );
        ctx.pending_ops.fsync(filepath);
        return Ok(());
    }
}

pub struct MVCCBuf {
    pages: SharedBuffer<Page, FIFOPolicy>,
}

pub struct MVCCBufCtx {
    pending_ops: &'static PendingFileOps,
    walapi: Option<&'static wal::GlobalStateExt>,
}

impl Value for MVCCBuf {
    type K = TableId;
    type LoadCtx = (/* mvcc_blk_rows */ u32, /* mvcc_buf_cap */ u32);
    type CommonData = MVCCBufCtx;

    fn load(k: &Self::K, lctx: &Self::LoadCtx, ctx: &Self::CommonData) -> anyhow::Result<Self> {
        return Ok(MVCCBuf {
            pages: sb::new_fifo_sb(
                lctx.1 as usize,
                PageCtx {
                    tableid: *k,
                    blk_rows: lctx.0,
                    pending_ops: ctx.pending_ops,
                    walapi: ctx.walapi,
                },
            ),
        });
    }

    fn store(&self, _k: &Self::K, _ctx: &Self::CommonData, force: bool) -> anyhow::Result<()> {
        self.pages.flushall(force)
    }
}

const BUF_INIT: u8 = 0;
const BUF_FPI: u8 = 1;
const BUF_SET_PAGE_XMIN: u8 = 2;
#[repr(C, packed(1))]
struct BufInitSer {
    eidx: u32,
    xid: Xid,
}
#[repr(C, packed(1))]
struct BufSetPageXminSer {
    sidx: u32,
    eidx: u32,
    xid: Xid,
}

// insert wal record for set_page_xmin().
fn insert_xmin_wal(page: &Page, sidx: u32, eidx: u32, xid: Xid, worker: &mut WorkerState) -> Lsn {
    if let Some(pagelsn) = page.lsn() {
        if pagelsn <= worker.wal.unwrap().recently_redo_lsn() {
            let waldat = wal::start_record_raw(page.as_bytes());
            return worker.insert_record(RmgrId::CSMvcc, BUF_FPI, waldat);
        } else {
            let args = BufSetPageXminSer { sidx, eidx, xid };
            let waldat = wal::start_record(&args);
            let lsnret =
                worker.try_insert_record(RmgrId::CSMvcc, BUF_SET_PAGE_XMIN, waldat, pagelsn);
            if let Some(retlsn) = lsnret {
                return retlsn;
            }
            let waldat = wal::start_record_raw(page.as_bytes());
            return worker.insert_record(RmgrId::CSMvcc, BUF_FPI, waldat);
        }
    } else {
        // See XLOG_HEAP_INIT_PAGE in heap_insert().
        debug_assert_eq!(sidx, 0);
        let rec = BufInitSer { eidx, xid };
        let waldat = wal::start_record(&rec);
        return worker.insert_record(RmgrId::CSMvcc, BUF_INIT, waldat);
    }
}

impl MVCCBuf {
    // Set the xmin of [sr, er) to xid.
    fn set_blk_xmin(
        &self,
        pageid: PageId,
        sr: u32,
        er: u32,
        xid: Xid,
        ws: &mut WorkerState,
    ) -> anyhow::Result<()> {
        let blk_rows = self.pages.valctx.blk_rows;
        let blksr = blk_rows * pageid.blkid;
        debug_assert!(sr >= blksr);
        debug_assert!(er <= blksr + blk_rows);
        debug_assert!(sr < er);
        let sidx = sr - blksr;
        let eidx = er - blksr;
        let sidx_i = sidx as isize;
        let xmin_len = (eidx - sidx) as usize;
        let slot = self.pages.read(&pageid, &())?; // pin guard
        let mut pageguard = slot.v.write().unwrap(); // page write lock guard
        let pagedat = pageguard.as_mut().unwrap();
        pagedat.set_xmin(sidx_i, xmin_len, xid);
        slot.mark_dirty();
        let lsn = insert_xmin_wal(pagedat, sidx, eidx, xid, ws);
        pagedat.set_lsn(lsn);
        return Ok(());
    }

    // Set the xmin of [sr, er) to xid.
    pub fn set_xmin(
        &self,
        fileid: FileId,
        mut sr: u32,
        er: u32,
        xid: Xid,
        ws: &mut WorkerState,
    ) -> anyhow::Result<()> {
        let blk_rows = self.pages.valctx.blk_rows;
        while sr < er {
            let blkid = sr / blk_rows;
            let blker = (blkid + 1) * blk_rows;
            let nextsr = std::cmp::min(blker, er);
            self.set_blk_xmin(PageId { fileid, blkid }, sr, nextsr, xid, ws)?;
            sr = nextsr;
        }
        return Ok(());
    }
}

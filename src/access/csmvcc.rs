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
use crate::utils::pwritevn;
use crate::utils::sb::{self, FIFOPolicy, SharedBuffer, Value};
use crate::utils::Xid;
use crate::utils::{alloc, dealloc};
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

    fn as_slice_from(&self, s: isize) -> &[u8] {
        debug_assert!(self.1 <= isize::MAX as usize);
        debug_assert!(self.1 >= s as usize);
        return unsafe { slice::from_raw_parts(self.0.as_ptr().offset(s), self.1 - s as usize) };
    }

    fn calc_crc32c(&self) -> u32 {
        debug_assert!(self.blk_rows() > 0);
        debug_assert!(self.1 <= isize::MAX as usize);
        return crc32c::crc32c(self.as_slice_from(4));
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

    fn as_mut_slice(&mut self) -> &mut [u8] {
        debug_assert!(self.1 <= isize::MAX as usize);
        return unsafe { slice::from_raw_parts_mut(self.0.as_ptr(), self.1) };
    }

    fn data_as_slice(&self) -> &[u8] {
        self.as_slice_from(4)
    }

    fn init(&mut self, blkrows: u32) {
        self.as_mut_slice().fill(0);
        self.set_blk_rows(blkrows);
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
    type Data = PageCtx;

    fn load(k: &Self::K, ctx: &Self::Data) -> anyhow::Result<Self> {
        let mut page = Page::new(ctx.blk_rows as u64);
        let off = page.1 * k.blkid as usize;
        debug_assert!(off <= off_t::MAX as usize);
        let filepath = get_mvccfile_path(ctx.tableid.db, ctx.tableid.table, k.fileid);
        let readsize = fd::use_file(&filepath, |mvccfile| -> anyhow::Result<usize> {
            return Ok(pread(
                mvccfile.as_raw_fd(),
                page.as_mut_slice(),
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

    fn store(&self, k: &Self::K, ctx: &Self::Data, _force: bool) -> anyhow::Result<()> {
        debug_assert_eq!(self.blk_rows(), ctx.blk_rows);
        let crcval = self.calc_crc32c().to_ne_bytes();
        let mut iovec = [
            IoVec::from_slice(&crcval),
            IoVec::from_slice(self.data_as_slice()),
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

impl Value for MVCCBuf {
    type K = (TableId, /* mvcc_blk_rows */ u32);
    type Data = &'static PendingFileOps;

    fn load(k: &Self::K, ctx: &Self::Data) -> anyhow::Result<Self> {
        return Ok(MVCCBuf {
            pages: sb::new_fifo_sb(
                64, /* todo!(盏一): Use table config */
                PageCtx {
                    tableid: k.0,
                    blk_rows: k.1,
                    pending_ops: *ctx,
                },
            ),
        });
    }

    fn store(&self, _k: &Self::K, _ctx: &Self::Data, force: bool) -> anyhow::Result<()> {
        self.pages.flushall(force)
    }
}

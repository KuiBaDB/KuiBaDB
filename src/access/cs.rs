// Copyright 2021 <盏一 w@hidva.com>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use crate::access::csmvcc::MVCCBuf;
use crate::access::rel;
use crate::access::{fd, sv};
use crate::datums::{self, Datums};
use crate::kbensure;
use crate::utils::WorkerState;
use anyhow::ensure;
use nix::libc::off_t;
use nix::sys::uio::pwrite;
use std::mem::size_of;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;

// Do not use L0Writer after error.
pub struct L0Writer {
    table: sv::TableId,
    rel: rel::Rel,
    path: String,

    pub meta: sv::FileMeta,
    hasnull: Vec<bool>,
    rows: Vec<(Vec<Rc<Datums>>, u32)>,
    rownum: u32,

    blockbuf: Vec<u8>,
    nextsetxminrow: u32,
}

impl L0Writer {
    pub fn new(table: sv::TableId, rel: rel::Rel, meta: sv::FileMeta) -> Self {
        let mut hasnull = Vec::with_capacity(rel.attrs.len());
        hasnull.resize(rel.attrs.len(), false);
        let nextsetxminrow = meta.rownum;
        Self {
            table,
            rel,
            meta,
            path: sv::get_datafile_path(table, meta.fileid),
            rows: vec![],
            rownum: 0,
            hasnull,
            blockbuf: Vec::new(),
            nextsetxminrow,
        }
    }

    // ExecConstraints
    fn check_notnull(&mut self, data: &[Rc<Datums>]) -> anyhow::Result<()> {
        debug_assert_eq!(self.rel.attrs.len(), data.len());
        for idx in 0..self.rel.attrs.len() {
            let attr = &self.rel.attrs[idx];
            let datums = &data[idx];
            if attr.notnull {
                kbensure!(
                    !datums.has_null(),
                    ERRCODE_NOT_NULL_VIOLATION,
                    "null value in column {} of relation {:?} violates not-null constraint",
                    &attr.name,
                    self.table
                );
            } else if datums.has_null() {
                self.hasnull[idx] = true;
            }
        }
        return Ok(());
    }

    fn flush_with_wal(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if self.rel.opt.enable_cs_wal {
            return self.flush_with_wal();
        }

        if self.rownum == 0 {
            return Ok(());
        }

        self.blockbuf.clear();
        // let hdrsize = 8 /* total size */ + 4 /* rownum */ + 2 /* colnum */;
        self.blockbuf.extend_from_slice(&0u64.to_ne_bytes());
        self.blockbuf.extend_from_slice(&self.rownum.to_ne_bytes());
        self.blockbuf
            .extend(&(self.rel.attrs.len() as u16).to_ne_bytes());
        datums::ser(
            &mut self.blockbuf,
            &self.rel,
            self.rownum,
            &self.hasnull,
            &self.rows,
        );
        let totalsize = (self.blockbuf.len() + size_of::<u32>()) as u64;
        let totalsizearea = &mut self.blockbuf.as_mut_slice()[..size_of::<u64>()];
        totalsizearea.copy_from_slice(&totalsize.to_ne_bytes());
        let crc = crc32c::crc32c(&self.blockbuf);
        self.blockbuf.extend_from_slice(&crc.to_ne_bytes());

        let off = self.meta.len as off_t;
        let wn = fd::use_file(&self.path, |l0file| -> anyhow::Result<usize> {
            Ok(pwrite(l0file.as_raw_fd(), &self.blockbuf, off)?)
        })?;
        ensure!(
            wn == self.blockbuf.len(),
            "L0Writer::flush: invalid wn: path={} a={} e={} o={}",
            &self.path,
            wn,
            self.blockbuf.len(),
            off
        );

        self.meta.len += wn as u64;
        self.meta.rownum += self.rownum;
        self.hasnull.clear();
        self.hasnull.resize(self.rel.attrs.len(), false);
        self.rows.clear();
        self.rownum = 0;
        return Ok(());
    }

    pub fn write(&mut self, data: Vec<Rc<Datums>>, rownum: u32) -> anyhow::Result<()> {
        debug_assert_eq!(self.rel.attrs.len(), data.len());
        self.check_notnull(&data)?;
        self.rownum += rownum;
        self.rows.push((data, rownum));
        if self.rownum < self.rel.opt.data_blk_rows {
            return Ok(());
        }
        return self.flush();
    }

    fn set_xmin(&mut self, worker: &mut WorkerState, mvccbuf: &MVCCBuf) -> anyhow::Result<()> {
        mvccbuf.set_xmin(
            self.meta.fileid,
            self.nextsetxminrow,
            self.meta.rownum,
            worker,
        )?;
        self.nextsetxminrow = self.meta.rownum;
        return Ok(());
    }

    pub fn sync(&mut self, worker: &mut WorkerState, mvccbuf: &MVCCBuf) -> anyhow::Result<()> {
        self.flush()?;
        debug_assert_eq!(self.rownum, 0);
        self.set_xmin(worker, mvccbuf)?;
        fd::use_file(&self.path, |l0file| l0file.sync_data())?;
        return Ok(());
    }
}

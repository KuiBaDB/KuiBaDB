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
use crate::utils::sb;
use crate::KB_BLCKSZ;
use anyhow::{bail, ensure};
use nix::sys::uio::{pread, pwrite};
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU8, Ordering};

const PAGES_PER_SEGMENT: u64 = 32;

pub type Pageno = u64;

pub struct CommonData {
    pub pending_ops: &'static PendingFileOps,
    pub dir: &'static str,
}

const ATOMICU80: AtomicU8 = AtomicU8::new(0);

pub struct Buff(pub [AtomicU8; KB_BLCKSZ]);

impl Buff {
    fn zeroed() -> Self {
        return Self([ATOMICU80; KB_BLCKSZ]);
    }

    fn new(v: &[u8; KB_BLCKSZ]) -> Self {
        let mut ret = [ATOMICU80; KB_BLCKSZ];
        for idx in 0..KB_BLCKSZ {
            ret[idx] = AtomicU8::new(v[idx]);
        }
        return Self(ret);
    }

    fn to_u8(&self) -> [u8; KB_BLCKSZ] {
        let mut ret = [0u8; KB_BLCKSZ]; // unneed zeroed.
        for idx in 0..KB_BLCKSZ {
            ret[idx] = self.0[idx].load(Ordering::Relaxed);
        }
        return ret;
    }

    // is this method safe?
    // fn as_u8(&self) -> &[u8; KB_BLCKSZ] {
    //     return unsafe {&*(&self.0 as *const _ as *const [u8; KB_BLCKSZ])};
    // }
}

impl sb::Value for Buff {
    type K = Pageno;
    type CommonData = CommonData;
    type LoadCtx = ();

    fn load(k: &Self::K, _ctx: &Self::LoadCtx, dat: &Self::CommonData) -> anyhow::Result<Self> {
        let pageno = *k;
        let segno = pageno / PAGES_PER_SEGMENT;
        let rpageno = pageno % PAGES_PER_SEGMENT;
        let off = (rpageno * KB_BLCKSZ as u64) as i64;
        let path = seg_path(dat.dir, segno);
        let mut buff = [0u8; KB_BLCKSZ]; // may be uninit~
        let ret = fd::try_use_file(&path, |file| -> anyhow::Result<usize> {
            let ret = pread(file.as_raw_fd(), &mut buff, off)?;
            return Ok(ret);
        });
        match ret {
            None => {
                let _newf = OpenOptions::new().create(true).write(true).open(&path)?;
                dat.pending_ops.fsync(dat.dir.to_string());
                return Ok(Buff::zeroed());
            }
            Some(readn) => {
                let readn = readn?;
                if readn == KB_BLCKSZ {
                    return Ok(Buff::new(&buff));
                }
                if readn == 0 {
                    return Ok(Buff::zeroed());
                }
                bail!(
                    "SLRU_READ_FAILED: dir: {} k: {} a: {}",
                    dat.dir,
                    pageno,
                    readn
                );
            }
        }
    }

    fn store(&self, k: &Self::K, ctx: &Self::CommonData, _force: bool) -> anyhow::Result<()> {
        let data = self.to_u8();
        let pageno = *k;
        let segno = pageno / PAGES_PER_SEGMENT;
        let rpageno = pageno % PAGES_PER_SEGMENT;
        let off = (rpageno * KB_BLCKSZ as u64) as i64;
        let path = seg_path(ctx.dir, segno);
        let wret = fd::use_file(&path, |file| -> anyhow::Result<usize> {
            Ok(pwrite(file.as_raw_fd(), &data, off)?)
        })?;
        ensure!(
            KB_BLCKSZ == wret,
            "SLRU_WRITE_FAILED: dir: {} k: {} a: {}",
            ctx.dir,
            pageno,
            wret
        );
        ctx.pending_ops.fsync(path);
        return Ok(());
    }
}

fn seg_path(dir: &str, segno: u64) -> String {
    format!("{}/{}", dir, segno)
}

pub struct Slru {
    data: sb::SharedBuffer<Buff, sb::LRUPolicy>,
}

impl Slru {
    pub fn new(max_size: usize, ctx: CommonData) -> Slru {
        Slru {
            data: sb::new_lru_sb(max_size, ctx),
        }
    }

    pub fn writable_load<F>(&self, pageno: Pageno, cb: F) -> anyhow::Result<()>
    where
        F: FnOnce(&Buff),
    {
        let slot = self.data.read(&pageno, &())?;
        let buff = slot.v.read().unwrap();
        let buff = buff.as_ref().unwrap();
        cb(buff);
        slot.mark_dirty();
        return Ok(());
    }

    pub fn try_readonly_load<T, F>(&self, pageno: Pageno, cb: F) -> anyhow::Result<T>
    where
        F: FnOnce(&Buff) -> T,
    {
        let slot = self.data.read(&pageno, &())?;
        let buff = slot.v.read().unwrap();
        let buff = buff.as_ref().unwrap();
        return Ok(cb(buff));
    }
}

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
use crate::utils::marc::{Destory, Marc};
use crate::utils::persist;
use crate::utils::sb::{self, SharedBuffer};
use crate::{FileId, Oid};
use anyhow::ensure;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs;
use std::io::{Cursor, Seek, SeekFrom};
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

struct L0File {
    fileid: FileId,
    inuse: AtomicBool,
    len: AtomicU64,
}

#[derive(Eq, Hash, Copy, Clone, Debug, PartialEq)]
pub struct TableId {
    db: Oid,
    table: Oid,
}

pub struct SVDestoryCtx {
    tableid: TableId,
    pending_ops: &'static PendingFileOps,
}

fn get_datafile_path(dboid: Oid, table: Oid, fileid: FileId) -> String {
    return format!("{}/{}/{}.d", dboid, table, fileid);
}

impl Destory for L0File {
    type DestoryCtx = SVDestoryCtx;
    fn destory(&mut self, ctx: &Self::DestoryCtx) {
        let p = get_datafile_path(ctx.tableid.db, ctx.tableid.table, self.fileid);
        ctx.pending_ops.unlink(p);
    }
}

struct ImmFile {
    fileid: FileId,
    len: u64,
}

impl Destory for ImmFile {
    type DestoryCtx = SVDestoryCtx;
    fn destory(&mut self, ctx: &Self::DestoryCtx) {
        let p = get_datafile_path(ctx.tableid.db, ctx.tableid.table, self.fileid);
        ctx.pending_ops.unlink(p);
    }
}

// length of l0/l1/l2 may be 0.
pub struct SupVer {
    l0: Vec<L0File>,
    l1: Vec<Marc<ImmFile>>,
    l2: Vec<Marc<ImmFile>>,
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

const MANIFEST_VER: u32 = 20181218;

fn read_level_files<T>(
    cursor: &mut Cursor<&[u8]>,
    on_file: impl Fn(u32, u64) -> T,
) -> anyhow::Result<Vec<T>> {
    let numl0 = cursor.read_u32::<LittleEndian>()?;
    let mut l0files = Vec::with_capacity(numl0 as usize);
    for _i in 0..numl0 {
        let fileid = cursor.read_u32::<LittleEndian>()?;
        let filelen = cursor.read_u64::<LittleEndian>()?;
        l0files.push(on_file(fileid, filelen));
    }
    return Ok(l0files);
}

fn read_manifest(path: &str) -> anyhow::Result<SupVer> {
    let mdata = fs::read(path)?;
    let mdata: &[u8] = &mdata;

    let crcidx = mdata.len() - mem::size_of::<u32>();
    let expect_crc = crc32c::crc32c(&mdata[..crcidx]);

    let mut cursor = Cursor::new(mdata);
    cursor.seek(SeekFrom::Start(crcidx as u64))?;
    let actual_crc = cursor.read_u32::<LittleEndian>()?;
    ensure!(
        actual_crc == expect_crc,
        "read_manifest failed. path={} expect_crc={} actual_crc={}",
        path,
        expect_crc,
        actual_crc
    );

    cursor.seek(SeekFrom::Start(0))?;
    let ver = cursor.read_u32::<LittleEndian>()?;
    ensure!(
        ver == MANIFEST_VER,
        "read_manifest failed. path={} expect_ver={} actual_ver={}",
        path,
        MANIFEST_VER,
        ver
    );

    let l0files = read_level_files(&mut cursor, |fileid, filelen| L0File {
        fileid: FileId::new(fileid).unwrap(),
        inuse: AtomicBool::new(false),
        len: AtomicU64::new(filelen),
    })?;

    let on_file_for_l1 = |fileid, filelen| {
        Marc::new(ImmFile {
            fileid: FileId::new(fileid).unwrap(),
            len: filelen,
        })
    };
    let l1files = read_level_files(&mut cursor, on_file_for_l1)?;
    let l2files = read_level_files(&mut cursor, on_file_for_l1)?;
    return Ok(SupVer {
        l0: l0files,
        l1: l1files,
        l2: l2files,
    });
}

fn write_level_files<T>(
    files: &Vec<T>,
    cursor: &mut Cursor<Vec<u8>>,
    on_file: impl Fn(&T) -> (u32, u64),
) -> anyhow::Result<()> {
    cursor.write_u32::<LittleEndian>(files.len() as u32)?;
    for file in files {
        let (fileid, filelen) = on_file(file);
        cursor.write_u32::<LittleEndian>(fileid)?;
        cursor.write_u64::<LittleEndian>(filelen)?;
    }
    return Ok(());
}

fn write_manifest(path: &str, sv: &SupVer) -> anyhow::Result<()> {
    let mut cursor = Cursor::new(Vec::new());
    cursor.write_u32::<LittleEndian>(MANIFEST_VER)?;

    write_level_files(&sv.l0, &mut cursor, |file: &L0File| {
        (file.fileid.get(), file.len.load(Relaxed))
    })?;
    let on_file = |file: &Marc<ImmFile>| (file.fileid.get(), file.len);
    write_level_files(&sv.l1, &mut cursor, on_file)?;
    write_level_files(&sv.l2, &mut cursor, on_file)?;

    let crc = crc32c::crc32c(cursor.get_ref());
    cursor.write_u32::<LittleEndian>(crc)?;

    return persist(path, cursor.get_ref());
}

fn get_minafest_path(db: Oid, table: Oid) -> String {
    format!("{}/{}/manifest", db, table)
}

impl sb::Value for SupVer {
    type Data = &'static PendingFileOps;
    type K = TableId;

    fn load(k: &Self::K, _ctx: &Self::Data) -> anyhow::Result<Self> {
        read_manifest(&get_minafest_path(k.db, k.table))
    }

    fn store(&self, k: &Self::K, ctx: &Self::Data, _force: bool) -> anyhow::Result<()> {
        let manifestpath = get_minafest_path(k.db, k.table);
        write_manifest(&manifestpath, self)?;

        let pending_fileops = *ctx;
        for l0file in &self.l0 {
            pending_fileops.fsync(get_datafile_path(k.db, k.table, l0file.fileid));
        }
        // pending_fileops.fsync(manifestpath);
        return Ok(());
    }
}

pub type TabSupVer = SharedBuffer<SupVer, sb::LRUPolicy>;

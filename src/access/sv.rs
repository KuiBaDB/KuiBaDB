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
use crate::{FileId, Oid};
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64};

struct L0File {
    fileid: FileId,
    inuse: AtomicBool,
    len: AtomicU64,
}

struct TableId {
    db: Oid,
    table: Oid,
}

struct SVDestoryCtx {
    tableid: TableId,
    pending_ops: &'static PendingFileOps,
}

impl Destory for L0File {
    type DestoryCtx = SVDestoryCtx;
    fn destory(&mut self, ctx: &Self::DestoryCtx) {
        ctx.pending_ops
            .unlink(ctx.tableid.db, ctx.tableid.table, self.fileid);
    }
}

struct ImmFile {
    fileid: FileId,
    len: u64,
}

impl Destory for ImmFile {
    type DestoryCtx = SVDestoryCtx;
    fn destory(&mut self, ctx: &Self::DestoryCtx) {
        ctx.pending_ops
            .unlink(ctx.tableid.db, ctx.tableid.table, self.fileid);
    }
}

struct SupVer {
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

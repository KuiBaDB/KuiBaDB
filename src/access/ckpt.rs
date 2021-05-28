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
use crate::{FileId, Oid};
use std::sync::Mutex;
use std::vec::Vec;

enum FileOp {
    Fsync,
    Unlink,
}

struct PendingFileOp {
    op: FileOp,
    db: Oid,
    table: Oid,
    fileid: FileId,
}

pub struct PendingFileOps(Mutex<Vec<PendingFileOp>>);

impl PendingFileOps {
    pub fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    pub fn unlink(&self, db: Oid, table: Oid, fileid: FileId) {
        let mut ops = self.0.lock().unwrap();
        ops.push(PendingFileOp {
            op: FileOp::Unlink,
            db,
            table,
            fileid,
        });
    }

    pub fn fsync(&self, db: Oid, table: Oid, fileid: FileId) {
        let mut ops = self.0.lock().unwrap();
        ops.push(PendingFileOp {
            op: FileOp::Fsync,
            db,
            table,
            fileid,
        });
    }
}

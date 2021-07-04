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

use crate::catalog;
use crate::datums::Datums;
use crate::executor::DestReceiver;
use crate::parser::sem;
use crate::utils::fmgr::FmgrInfo;
use crate::utils::{SessionState, WorkerState};
use crate::{protocol, Oid};
use std::debug_assert;
use std::net::TcpStream;
use std::rc::Rc;

pub mod ckpt;
pub mod clog;
pub mod cs;
pub mod csmvcc;
pub mod fd;
pub mod lmgr;
pub mod redo;
pub mod rel;
mod slru;
pub mod sv;
pub mod wal;
pub mod xact;

pub struct DestRemote<'sess> {
    session: &'sess SessionState,
    stream: &'sess mut TcpStream,
    typout: Vec<FmgrInfo>,
    outstr: Vec<Rc<Datums>>,
    pub processed: u64,
}

impl DestRemote<'_> {
    pub fn new<'sess>(
        session: &'sess SessionState,
        stream: &'sess mut TcpStream,
    ) -> DestRemote<'sess> {
        DestRemote {
            session,
            stream,
            typout: Vec::new(),
            processed: 0,
            outstr: Vec::new(),
        }
    }
}

impl DestReceiver for DestRemote<'_> {
    fn startup(&mut self, tlist: &Vec<sem::TargetEntry>) -> anyhow::Result<()> {
        self.outstr.resize_with(tlist.len(), Default::default);
        self.typout.clear();
        let mut fields = Vec::new();
        for target in tlist {
            let typoid = target.expr.val_type();
            let (typoutproc, typlen) = catalog::get_type_output_info(self.session, typoid)?;
            self.typout
                .push(FmgrInfo::new(typoutproc, self.session.fmgr_builtins)?);
            let fieldname = match &target.resname {
                None => "", // TupleDescInitEntry() set name to empty if target.resname is None.
                Some(v) => v,
            };
            fields.push(protocol::FieldDesc::new(fieldname, typoid, -1, typlen));
        }
        protocol::write_message(
            self.stream,
            &protocol::RowDescription {
                fields: fields.as_slice(),
            },
        );
        Ok(())
    }

    fn receive(
        &mut self,
        tuples: &[Rc<Datums>],
        orownum: u32,
        worker: &WorkerState,
    ) -> anyhow::Result<()> {
        debug_assert!(tuples.len() == self.typout.len());
        let rownum = orownum as u64;
        self.processed += rownum;
        for (idx, _) in tuples.iter().enumerate() {
            let fnaddr = self.typout[idx].fn_addr;
            fnaddr(
                &self.typout[idx],
                &mut self.outstr[idx],
                &tuples[idx..idx + 1],
                &worker,
            )?;
        }

        let mut ostr = Vec::with_capacity(tuples.len());
        for idx in 0..rownum {
            ostr.clear();
            for col in &self.outstr {
                let colstr = col.try_get_varchar_at(idx as isize).map(|v| v.as_bytes());
                ostr.push(colstr);
            }
            // TODO: Cache the message in memory and flush the message when necessary.
            protocol::write_message(
                self.stream,
                &protocol::DataRow {
                    data: ostr.as_slice(),
                },
            );
        }
        return Ok(());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TypeDesc {
    pub id: Oid,
    pub len: i16,
    pub align: u8,
    pub mode: i32,
}

impl TypeDesc {
    pub fn hash(&self, md5h: &mut md5::Context) {
        md5h.consume(self.id.get().to_ne_bytes());
        md5h.consume(self.len.to_ne_bytes());
        md5h.consume(self.align.to_ne_bytes());
        md5h.consume(self.mode.to_ne_bytes());
        return;
    }
}

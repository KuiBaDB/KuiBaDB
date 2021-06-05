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
use crate::datumblock::DatumBlock;
use crate::executor::DestReceiver;
use crate::parser::sem;
use crate::utils::fmgr::{get_fn_addr, get_single_bytes, FmgrInfo};
use crate::utils::{SessionState, WorkerState};
use crate::{protocol, Oid};
use std::debug_assert;
use std::net::TcpStream;
use std::rc::Rc;

pub mod ckpt;
pub mod clog;
pub mod lmgr;
pub mod redo;
mod slru;
pub mod sv;
pub mod wal;
pub mod xact;

pub struct DestRemote<'sess> {
    session: &'sess SessionState,
    stream: &'sess mut TcpStream,
    state: WorkerState,
    typout: Vec<FmgrInfo>,
    pub processed: u64,
}

impl DestRemote<'_> {
    pub fn new<'sess>(
        session: &'sess SessionState,
        stream: &'sess mut TcpStream,
    ) -> DestRemote<'sess> {
        let state = WorkerState::new(session);
        DestRemote {
            session,
            stream,
            state,
            typout: Vec::new(),
            processed: 0,
        }
    }
}

impl DestReceiver for DestRemote<'_> {
    fn startup(&mut self, tlist: &Vec<sem::TargetEntry<'_>>) -> anyhow::Result<()> {
        self.typout.clear();
        let mut fields = Vec::new();
        for target in tlist {
            let typoid = target.expr.val_type();
            let (typoutproc, typlen) = catalog::get_type_output_info(self.session, typoid)?;
            self.typout.push(FmgrInfo {
                fn_addr: get_fn_addr(typoutproc, self.session.fmgr_builtins)?,
                fn_oid: typoutproc,
            });
            let fieldname = match target.resname {
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

    fn receive(&mut self, tuples: &Vec<Rc<DatumBlock>>) -> anyhow::Result<()> {
        debug_assert!(tuples.len() == self.typout.len());
        self.processed += 1;
        let mut output = Vec::with_capacity(tuples.len());
        // How to traverse two Vec at the same time?
        for idx in 0..tuples.len() {
            let ostr =
                (self.typout[idx].fn_addr)(&self.typout[idx], &tuples[idx..idx + 1], &self.state)?;
            output.push(ostr);
        }
        let mut ostr = Vec::with_capacity(output.len());
        for o in &output {
            ostr.push(Some(get_single_bytes(o)));
        }
        protocol::write_message(
            self.stream,
            &protocol::DataRow {
                data: ostr.as_slice(),
            },
        );
        Ok(())
    }
}

pub struct TypeDesc {
    pub id: Oid,
    pub len: i16,
    pub align: u8,
    pub mode: i32,
}

pub struct TupleDesc {
    pub desc: Vec<TypeDesc>,
}

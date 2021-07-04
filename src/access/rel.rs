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
use crate::access::TypeDesc;
use crate::catalog::column_val;
use crate::guc::{self, GucState};
use crate::utils::AttrNumber;
use crate::utils::SessionState;
use crate::Oid;
use anyhow::bail;

#[derive(Clone, Copy, Debug)]
pub struct RelOpt {
    pub mvcc_blk_rows: u32,
    pub mvcc_buf_cap: u32,
    pub data_blk_rows: u32,
    pub enable_cs_wal: bool,
}

impl RelOpt {
    fn new(gucstate: &GucState) -> Self {
        Self {
            mvcc_blk_rows: guc::get_int(gucstate, guc::MvccBlkRows) as u32,
            mvcc_buf_cap: guc::get_int(gucstate, guc::MvccBufCap) as u32,
            data_blk_rows: guc::get_int(gucstate, guc::DataBlkRows) as u32,
            enable_cs_wal: guc::get_bool(gucstate, guc::EnableCsWal),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Attr {
    pub num: AttrNumber,
    pub name: String,
    pub typ: TypeDesc,
    pub notnull: bool,
    pub dropped: bool,
}

#[derive(Clone, Debug)]
pub struct Rel {
    pub attrs: Vec<Attr>,
    pub opt: RelOpt,
}

fn getrelopt(sess: &mut SessionState, table: Oid) -> anyhow::Result<RelOpt> {
    let mut reloptsstr = String::new();
    let sql = format!("select reloptions from kb_class where oid = {}", table);
    sess.metaconn.iterate(sql, |row| {
        reloptsstr = column_val(row, "reloptions").unwrap().into();
        true
    })?;

    let mut relopt = RelOpt::new(&sess.gucstate);
    for reloptstr in reloptsstr.split(',') {
        if let Some(eqidx) = reloptstr.find('=') {
            let optname = &reloptstr[..eqidx];
            let optval = &reloptstr[eqidx + 1..];
            match optname {
                "mvcc_blk_rows" => relopt.mvcc_blk_rows = optval.parse()?,
                "mvcc_buf_cap" => relopt.mvcc_buf_cap = optval.parse()?,
                "data_blk_rows" => relopt.data_blk_rows = optval.parse()?,
                "enable_cs_wal" => relopt.enable_cs_wal = optval.parse()?,
                _ => continue,
            }
        } else {
            bail!(
                "getrelopt: invalid relopt: table={} relopt={}",
                table,
                reloptstr
            );
        }
    }
    return Ok(relopt);
}

fn getrelattrs(sess: &mut SessionState, table: Oid) -> anyhow::Result<Vec<Attr>> {
    let mut attrs = Vec::<Attr>::new();
    let mut nextattnum = 1;
    let sql = format!(
        "select * from kb_attribute where attrelid = {} order by attnum",
        table
    );
    sess.metaconn.iterate(sql, |row| {
        let num: AttrNumber = column_val(row, "attnum").unwrap().parse().unwrap();
        if num.get() != nextattnum {
            return false;
        }
        nextattnum += 1;
        let name = column_val(row, "attname").unwrap().to_string();
        let atttypid: Oid = column_val(row, "atttypid").unwrap().parse().unwrap();
        let attlen: i16 = column_val(row, "attlen").unwrap().parse().unwrap();
        let attalign: u8 = column_val(row, "attalign").unwrap().parse().unwrap();
        let typmod: i32 = column_val(row, "atttypmod").unwrap().parse().unwrap();
        let notnull: i32 = column_val(row, "attnotnull").unwrap().parse().unwrap();
        let notnull = notnull != 0;
        let dropped: i32 = column_val(row, "attisdropped").unwrap().parse().unwrap();
        let dropped = dropped != 0;
        let attr = Attr {
            num,
            name,
            notnull,
            dropped,
            typ: TypeDesc {
                id: atttypid,
                len: attlen,
                align: attalign,
                mode: typmod,
            },
        };
        attrs.push(attr);
        return true;
    })?;
    return Ok(attrs);
}

pub fn getrel(sess: &mut SessionState, table: Oid) -> anyhow::Result<Rel> {
    return Ok(Rel {
        attrs: getrelattrs(sess, table)?,
        opt: getrelopt(sess, table)?,
    });
}

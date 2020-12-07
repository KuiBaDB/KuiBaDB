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

use crate::datumblock::{DatumBlock, DatumBlockSingle};
use crate::utils::WorkerState;
use crate::{protocol, ErrCode};
use anyhow::{anyhow, Context};
use kuiba::Oid;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FmgrInfo {
    pub fn_addr: KBFunction,
    pub fn_oid: Oid,
}

// PGFunction
pub type KBFunction = fn(
    flinfo: &FmgrInfo,
    args: &[Rc<DatumBlock>],
    state: &WorkerState,
) -> anyhow::Result<Rc<DatumBlock>>;

// TODO: remove unwrap()!!!
fn get_i32(val: &DatumBlockSingle) -> i32 {
    val.to_i32().unwrap()
}

pub fn get_single_bytes(val: &DatumBlock) -> &[u8] {
    match val {
        DatumBlock::Single(s) => s.as_bytes().unwrap(),
    }
}

fn signle_bytes(val: &[u8]) -> anyhow::Result<Rc<DatumBlock>> {
    Ok(Rc::new(DatumBlock::Single(DatumBlockSingle::new_bytes(
        val,
    ))))
}

fn single_i32(val: i32) -> anyhow::Result<Rc<DatumBlock>> {
    Ok(Rc::new(DatumBlock::Single(DatumBlockSingle::from_i32(val))))
}

fn int4out(
    _flinfo: &FmgrInfo,
    args: &[Rc<DatumBlock>],
    _state: &WorkerState,
) -> anyhow::Result<Rc<DatumBlock>> {
    match &(*args[0]) {
        DatumBlock::Single(s) => signle_bytes(get_i32(s).to_string().as_bytes()),
    }
}

fn i32_2args(args: &[Rc<DatumBlock>]) -> (i32, i32) {
    (
        match &(*args[0]) {
            DatumBlock::Single(s) => get_i32(s),
        },
        match &(*args[1]) {
            DatumBlock::Single(s) => get_i32(s),
        },
    )
}

fn int4pl(
    _flinfo: &FmgrInfo,
    args: &[Rc<DatumBlock>],
    _state: &WorkerState,
) -> anyhow::Result<Rc<DatumBlock>> {
    let (l, r) = i32_2args(args);
    let (r, overflow) = l.overflowing_add(r);
    if overflow {
        Err(anyhow!("integer out of range"))
            .context(ErrCode(protocol::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
    } else {
        single_i32(r)
    }
}

fn int4mi(
    _flinfo: &FmgrInfo,
    args: &[Rc<DatumBlock>],
    _state: &WorkerState,
) -> anyhow::Result<Rc<DatumBlock>> {
    let (l, r) = i32_2args(args);
    let (r, overflow) = l.overflowing_sub(r);
    if overflow {
        Err(anyhow!("integer out of range"))
            .context(ErrCode(protocol::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
    } else {
        single_i32(r)
    }
}

fn int4mul(
    _flinfo: &FmgrInfo,
    args: &[Rc<DatumBlock>],
    _state: &WorkerState,
) -> anyhow::Result<Rc<DatumBlock>> {
    let (l, r) = i32_2args(args);
    let (r, overflow) = l.overflowing_mul(r);
    if overflow {
        Err(anyhow!("integer out of range"))
            .context(ErrCode(protocol::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
    } else {
        single_i32(r)
    }
}

fn int4div(
    _flinfo: &FmgrInfo,
    args: &[Rc<DatumBlock>],
    _state: &WorkerState,
) -> anyhow::Result<Rc<DatumBlock>> {
    let (l, r) = i32_2args(args);
    if r == 0 {
        return Err(anyhow!("division by zero"))
            .context(ErrCode(protocol::ERRCODE_DIVISION_BY_ZERO));
    }
    let (r, overflow) = l.overflowing_div(r);
    if overflow {
        Err(anyhow!("integer out of range"))
            .context(ErrCode(protocol::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
    } else {
        single_i32(r)
    }
}

pub type FmgrBuiltinsMap = HashMap<Oid, KBFunction>;
pub fn get_fmgr_builtins() -> FmgrBuiltinsMap {
    let mut m = FmgrBuiltinsMap::new();
    // TODO: MAGIC NUMBER!!!
    m.insert(Oid::new(43).unwrap(), int4out);
    m.insert(Oid::new(177).unwrap(), int4pl);
    m.insert(Oid::new(181).unwrap(), int4mi);
    m.insert(Oid::new(154).unwrap(), int4div);
    m.insert(Oid::new(141).unwrap(), int4mul);
    m
}

pub fn get_fn_addr(oid: Oid, map: &FmgrBuiltinsMap) -> anyhow::Result<KBFunction> {
    map.get(&oid)
        .ok_or(
            anyhow!("internal function {} is not in internal lookup table", oid)
                .context(ErrCode(protocol::ERRCODE_UNDEFINED_FUNCTION)),
        )
        .map(|v| *v)
}

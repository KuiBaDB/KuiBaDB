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

use crate::datums::Datums;
use crate::kbanyhow;
use crate::utils::adt;
use crate::utils::WorkerState;
use crate::Oid;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Copy)]
pub struct FmgrInfo {
    pub fn_addr: KBFunction,
    pub fn_oid: Oid,
}

impl FmgrInfo {
    pub fn new(oid: Oid, map: &FmgrBuiltinsMap) -> anyhow::Result<Self> {
        Ok(Self {
            fn_oid: oid,
            fn_addr: get_fn_addr(oid, map)?,
        })
    }
}

// PGFunction
pub type KBFunction = fn(
    flinfo: &FmgrInfo,
    ret: &mut Rc<Datums>,
    args: &[Rc<Datums>],
    state: &WorkerState,
) -> anyhow::Result<()>;

pub type FmgrBuiltinsMap = HashMap<Oid, KBFunction>;
pub fn get_fmgr_builtins() -> FmgrBuiltinsMap {
    let mut m = FmgrBuiltinsMap::new();
    m.insert(Oid::new(42).unwrap(), adt::int4in);
    m.insert(Oid::new(43).unwrap(), adt::int4out);
    m.insert(Oid::new(177).unwrap(), adt::int4pl);
    m.insert(Oid::new(181).unwrap(), adt::int4mi);
    m.insert(Oid::new(154).unwrap(), adt::int4div);
    m.insert(Oid::new(141).unwrap(), adt::int4mul);
    m
}

pub fn get_fn_addr(oid: Oid, map: &FmgrBuiltinsMap) -> anyhow::Result<KBFunction> {
    map.get(&oid)
        .ok_or(kbanyhow!(
            ERRCODE_UNDEFINED_FUNCTION,
            "internal function {} is not in internal lookup table",
            oid
        ))
        .map(|v| *v)
}

// InputFunctionCall
pub fn call_inproc(
    flinfo: &FmgrInfo,
    out: &mut Rc<Datums>,
    indatum: Rc<Datums>,
    typmod: Rc<Datums>,
    worker: &WorkerState,
) -> anyhow::Result<()> {
    (flinfo.fn_addr)(flinfo, out, &[indatum, typmod], worker)
}

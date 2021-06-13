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
use crate::kbensure;
use crate::utils::fmgr::FmgrInfo;
use crate::utils::WorkerState;
use std::mem::{align_of, size_of};
use std::rc::Rc;

fn ornull(ret: &mut Datums, left: &Datums, right: &Datums) {
    // TODO: speed
    debug_assert!(!left.is_single());
    debug_assert!(!right.is_single());
    debug_assert_eq!(left.len(), right.len());
    debug_assert_eq!(ret.len(), left.len());
    ret.set_notnull_all();
    for i in 0..left.len() as isize {
        if left.is_null_at(i) || right.is_null_at(i) {
            ret.set_null_at(i);
        }
    }
    return;
}

macro_rules! typbinop {
    ($ret: ident, $left: ident, $right: ident, $optyp: ty, $binop: ident, $getsingle: ident, $setsingle: ident, $getat: ident, $setat: ident) => {
        let retdatum = Rc::make_mut($ret);
        if $left.is_single() && $right.is_single() {
            if $left.is_single_null() || $right.is_single_null() {
                retdatum.set_single_null();
                return Ok(());
            }
            let (retval, of) = $left.$getsingle().$binop($right.$getsingle());
            kbensure!(
                !of,
                ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "integer out of range"
            );
            retdatum.$setsingle(retval);
            return Ok(());
        }
        if $left.is_single() {
            retdatum.resize_fixedlen($right.len(), size_of::<$optyp>(), align_of::<$optyp>());
            if $left.is_single_null() {
                retdatum.set_null_all();
                return Ok(());
            }
            retdatum.set_notnull_all();
            let li32 = $left.$getsingle();
            for idx in 0..$right.len() as isize {
                if $right.is_null_at(idx) {
                    retdatum.set_null_at(idx);
                } else {
                    let (reti32, of) = li32.$binop($right.$getat(idx));
                    kbensure!(
                        !of,
                        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                        "integer out of range"
                    );
                    retdatum.$setat(idx, reti32);
                }
            }
            return Ok(());
        }
        if $right.is_single() {
            retdatum.resize_fixedlen($left.len(), size_of::<$optyp>(), align_of::<$optyp>());
            if $right.is_single_null() {
                retdatum.set_null_all();
                return Ok(());
            }
            retdatum.set_notnull_all();
            let li32 = $right.$getsingle();
            for idx in 0..$left.len() as isize {
                if $left.is_null_at(idx) {
                    retdatum.set_null_at(idx);
                } else {
                    let (reti32, of) = $left.$getat(idx).$binop(li32);
                    kbensure!(
                        !of,
                        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                        "integer out of range"
                    );
                    retdatum.$setat(idx, reti32);
                }
            }
            return Ok(());
        }
        debug_assert_eq!($left.len(), $right.len());
        retdatum.resize_fixedlen($left.len(), size_of::<$optyp>(), align_of::<$optyp>());
        ornull(retdatum, $left, $right);
        for idx in 0..$left.len() as isize {
            if !retdatum.is_null_at(idx) {
                let (retval, of) = $left.$getat(idx).$binop($right.$getat(idx));
                kbensure!(
                    !of,
                    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                    "integer out of range"
                );
                retdatum.$setsingle(retval);
            }
        }
    };
}

macro_rules! i32binop {
    ($ret: ident, $left: ident, $right: ident, $binop: ident) => {
        typbinop!(
            $ret,
            $left,
            $right,
            i32,
            $binop,
            get_single_i32,
            set_single_i32,
            get_i32_at,
            set_i32_at
        )
    };
}

pub fn int4pl(
    _flinfo: &FmgrInfo,
    ret: &mut Rc<Datums>,
    args: &[Rc<Datums>],
    _state: &WorkerState,
) -> anyhow::Result<()> {
    let left = &args[0];
    let right = &args[1];
    log::debug!(
        "int4pl: l: {}, r: {}",
        left.get_single_i32(),
        right.get_single_i32()
    );
    i32binop!(ret, left, right, overflowing_add);
    return Ok(());
}

pub fn int4out(
    _flinfo: &FmgrInfo,
    ret: &mut Rc<Datums>,
    args: &[Rc<Datums>],
    _state: &WorkerState,
) -> anyhow::Result<()> {
    let retdatum = Rc::make_mut(ret);
    let arg = &args[0];
    if arg.is_single() {
        if arg.is_single_null() {
            retdatum.set_single_null();
        } else {
            retdatum.set_single_varchar(arg.get_single_i32().to_string().as_bytes());
        }
        return Ok(());
    }
    retdatum.resize_varlen(arg.len());
    retdatum.set_null_to(arg);
    for idx in 0..arg.len() as isize {
        if !arg.is_null_at(idx) {
            retdatum.set_varchar_at(idx, arg.get_i32_at(idx).to_string().as_bytes());
        }
    }
    return Ok(());
}

pub fn int4mi(
    _flinfo: &FmgrInfo,
    ret: &mut Rc<Datums>,
    args: &[Rc<Datums>],
    _state: &WorkerState,
) -> anyhow::Result<()> {
    let left = &args[0];
    let right = &args[1];
    i32binop!(ret, left, right, overflowing_sub);
    return Ok(());
}

pub fn int4div(
    _flinfo: &FmgrInfo,
    ret: &mut Rc<Datums>,
    args: &[Rc<Datums>],
    _state: &WorkerState,
) -> anyhow::Result<()> {
    let left = &args[0];
    let right = &args[1];
    i32binop!(ret, left, right, overflowing_div);
    return Ok(());
}

pub fn int4mul(
    _flinfo: &FmgrInfo,
    ret: &mut Rc<Datums>,
    args: &[Rc<Datums>],
    _state: &WorkerState,
) -> anyhow::Result<()> {
    let left = &args[0];
    let right = &args[1];
    i32binop!(ret, left, right, overflowing_mul);
    return Ok(());
}

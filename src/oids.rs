/*
Copyright 2020 <盏一 w@hidva.com>
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

pub type Oid = std::num::NonZeroU32;
#[derive(Copy, Clone)]
pub struct OptOid(pub Option<Oid>);

impl std::convert::From<u32> for OptOid {
    fn from(val: u32) -> Self {
        Self(Oid::new(val))
    }
}

impl std::convert::From<OptOid> for u32 {
    fn from(val: OptOid) -> Self {
        match val.0 {
            None => 0,
            Some(v) => v.get(),
        }
    }
}

pub const TEMPLATE0_DB: Oid = unsafe { Oid::new_unchecked(1) };
pub const KUIBADB: Oid = unsafe { Oid::new_unchecked(2) };
pub const KBCATLOGNS: Oid = unsafe { Oid::new_unchecked(11) };
pub const BOOLOID: Oid = unsafe { Oid::new_unchecked(16) };
pub const BOOLINPROC: Oid = unsafe { Oid::new_unchecked(1242) };
pub const BOOLOUTPROC: Oid = unsafe { Oid::new_unchecked(1243) };
pub const BYTEAOID: Oid = unsafe { Oid::new_unchecked(17) };
pub const BYTEAINPROC: Oid = unsafe { Oid::new_unchecked(1244) };
pub const BYTEAOUTPROC: Oid = unsafe { Oid::new_unchecked(31) };
pub const INT8OID: Oid = unsafe { Oid::new_unchecked(20) };
pub const INT8INPROC: Oid = unsafe { Oid::new_unchecked(460) };
pub const INT8OUTPROC: Oid = unsafe { Oid::new_unchecked(461) };
pub const INT2OID: Oid = unsafe { Oid::new_unchecked(21) };
pub const INT2INPROC: Oid = unsafe { Oid::new_unchecked(38) };
pub const INT2OUTPROC: Oid = unsafe { Oid::new_unchecked(39) };
pub const INT4OID: Oid = unsafe { Oid::new_unchecked(23) };
pub const INT4INPROC: Oid = unsafe { Oid::new_unchecked(42) };
pub const INT4OUTPROC: Oid = unsafe { Oid::new_unchecked(43) };
pub const FLOAT4OID: Oid = unsafe { Oid::new_unchecked(700) };
pub const FLOAT4INPROC: Oid = unsafe { Oid::new_unchecked(200) };
pub const FLOAT4OUTPROC: Oid = unsafe { Oid::new_unchecked(201) };
pub const FLOAT8OID: Oid = unsafe { Oid::new_unchecked(701) };
pub const FLOAT8INPROC: Oid = unsafe { Oid::new_unchecked(214) };
pub const FLOAT8OUTPROC: Oid = unsafe { Oid::new_unchecked(215) };
pub const VARCHAROID: Oid = unsafe { Oid::new_unchecked(1043) };
pub const VARCHARINPROC: Oid = unsafe { Oid::new_unchecked(1046) };
pub const VARCHAROUTPROC: Oid = unsafe { Oid::new_unchecked(1047) };
pub const TYPERELID: Oid = unsafe { Oid::new_unchecked(1247) };
pub const ATTRRELID: Oid = unsafe { Oid::new_unchecked(1249) };
pub const PROCRELID: Oid = unsafe { Oid::new_unchecked(1255) };
pub const RELRELID: Oid = unsafe { Oid::new_unchecked(1259) };
pub const DBRELID: Oid = unsafe { Oid::new_unchecked(1262) };
pub const KBPUBLICNS: Oid = unsafe { Oid::new_unchecked(2200) };
pub const NSRELID: Oid = unsafe { Oid::new_unchecked(2615) };
pub const OPRELID: Oid = unsafe { Oid::new_unchecked(2617) };
// pub const MaxOid: Oid = unsafe {Oid::new_unchecked(16384)};  // The oid of system catalogs should be less than MaxOid.

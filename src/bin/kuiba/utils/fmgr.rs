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

use crate::datumblock::DatumBlock;
use kuiba::Oid;
use std::rc::Rc;

pub struct FmgrInfo {
    pub fn_addr: KBFunction,
    pub fn_oid: Oid,
}

pub struct FunctionCallInfoBaseData<'a> {
    flinfo: &'a FmgrInfo,
    args: Vec<Rc<DatumBlock>>,
}

// PGFunction
pub type KBFunction = fn(fcinfo: &FunctionCallInfoBaseData) -> Rc<DatumBlock>;

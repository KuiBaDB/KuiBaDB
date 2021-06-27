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
use std::io::BufRead;
use crate::parser::syn::RangeVar;

// pub fn lock_stmt(sess: &mut SessionState, lock: &syn::LockStmt<'_>) -> anyhow::Result<Response> {

fn copy(dest: &RangeVar<'_>, input: impl BufRead, delim: &str) -> anyhow::Result<u64> {
    todo!()
}

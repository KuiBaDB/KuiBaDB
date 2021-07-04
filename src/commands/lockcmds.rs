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

use crate::catalog::namespace::SessionExt as NSSessionExt;
use crate::parser::syn;
use crate::utility::Response;
use crate::utils::SessionState;
use crate::xact::SessionExt as XACTSessionExt;

pub fn lock_stmt(sess: &mut SessionState, lock: &syn::LockStmt<'_>) -> anyhow::Result<Response> {
    sess.require_transblock("LOCK TABLE")?;
    for rv in &lock.rels {
        sess.rv_get_oid(rv, lock.mode)?;
    }
    return Ok(Response::new("LOCK TABLE"));
}

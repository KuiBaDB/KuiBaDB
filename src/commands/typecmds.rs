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

use crate::catalog::namespace::SessionExt;
use crate::parser::syn;
use crate::utility::{Response, StrResp};
use crate::utils::SessionState;

pub fn define_type(stmt: &syn::DefineTypeStmt, state: &SessionState) -> anyhow::Result<Response> {
    let (typnsoid, typname) = state.qualname_get_create_ns(&stmt.defnames)?;

    Ok(Response {
        resp: Some(StrResp {
            name: "CREATE TYPE".to_string(),
            val: format!(
                "DefineType. typensoid={} typname={} stmt={:?}",
                typnsoid, typname, stmt
            ),
        }),
        tag: "CREATE TYPE",
    })
}

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
use anyhow::anyhow;
use lalrpop_util::lalrpop_mod;

pub mod sem;
pub mod syn;
lalrpop_mod!(sql, "/parser/sql.rs");

pub fn parse(query: &str) -> anyhow::Result<syn::Stmt> {
    match sql::StmtParser::new().parse(query) {
        Ok(v) => Ok(v),
        Err(err) => Err(anyhow!("Parse Error. {:?}", err)),
    }
}

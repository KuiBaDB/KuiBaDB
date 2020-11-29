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

use super::syn;

// 'syn is the lifetime of syntax tree returned by parser::parse().

pub enum UtilityStmt<'syn, 'input> {
    VariableSet(&'syn syn::VariableSetStmt<'input>),
    VariableShow(&'syn syn::VariableShowStmt<'input>),
}

pub enum Stmt<'syn, 'input> {
    Utility(UtilityStmt<'syn, 'input>),
}

pub fn analyze<'syn, 'input>(stmt: &'syn syn::Stmt<'input>) -> anyhow::Result<Stmt<'syn, 'input>> {
    match stmt {
        syn::Stmt::VariableSet(v) => Ok(Stmt::Utility(UtilityStmt::VariableSet(v))),
        syn::Stmt::VariableShow(v) => Ok(Stmt::Utility(UtilityStmt::VariableShow(v))),
        syn::Stmt::Empty => unreachable!(),
    }
}

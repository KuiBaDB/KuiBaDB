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

use crate::parser::sem;
use crate::utils::SessionState;
use anyhow;

// 'sem is the lifetime of stuff returned by kb_analyze().

pub struct Result<'syn> {
    pub resconstantqual: Option<sem::Expr>,
    pub lefttree: Option<Box<Plan<'syn>>>,
    pub qual: Vec<sem::Expr>,
    pub tlist: Vec<sem::TargetEntry<'syn>>,
}

pub enum Plan<'syn> {
    Result(Result<'syn>),
}

// TODO: Try to simplify it, remove the match.
impl Plan<'_> {
    pub fn tlist(&self) -> &Vec<sem::TargetEntry<'_>> {
        match self {
            Plan::Result(r) => &r.tlist,
        }
    }
}

pub struct PlannedStmt<'syn> {
    pub plan_tree: Plan<'syn>,
}

pub fn planner<'syn>(
    _state: &mut SessionState,
    parse: &sem::Query<'syn>,
) -> anyhow::Result<PlannedStmt<'syn>> {
    Ok(PlannedStmt {
        plan_tree: Plan::Result(Result {
            tlist: parse.tlist.clone(),
            qual: Vec::new(),
            lefttree: None,
            resconstantqual: None,
        }),
    })
}

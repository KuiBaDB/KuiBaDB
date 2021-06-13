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

// Common should always be placed first so that Plan::common can erase the match expr.
pub struct PlanCommon {
    pub tlist: Vec<sem::TargetEntry>,
}

pub struct Result {
    pub plan: PlanCommon,
    pub resconstantqual: Option<sem::Expr>,
    pub lefttree: Option<Box<Plan>>,
    pub qual: Vec<sem::Expr>,
}

pub enum Plan {
    Result(Result),
}

// TODO: Try to simplify it, remove the match.
impl Plan {
    pub fn common(&self) -> &PlanCommon {
        match self {
            Plan::Result(r) => &r.plan,
        }
    }

    pub fn tlist(&self) -> &Vec<sem::TargetEntry> {
        return &self.common().tlist;
    }
}

// PlannedStmt may be cached by the plan cache, so it should have no lifetime.
pub struct PlannedStmt {
    pub plan_tree: Plan,
}

pub fn planner(_state: &mut SessionState, parse: &sem::Query) -> anyhow::Result<PlannedStmt> {
    Ok(PlannedStmt {
        plan_tree: Plan::Result(Result {
            plan: PlanCommon {
                tlist: parse.tlist.clone(),
            },
            qual: Vec::new(),
            lefttree: None,
            resconstantqual: None,
        }),
    })
}

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
use crate::optimizer;
use crate::optimizer::PlannedStmt;
use crate::parser::sem;
use crate::utils::fmgr::{get_fn_addr, FmgrInfo};
use crate::utils::{SessionState, WorkerState};
use anyhow::anyhow;
use std::rc::Rc;

pub trait DestReceiver {
    fn startup(&mut self, tlist: &Vec<sem::TargetEntry<'_>>) -> anyhow::Result<()>;
    fn receive(&mut self, tuples: &Vec<Rc<DatumBlock>>) -> anyhow::Result<()>;
}

// At present, we directly pass the fields of ExprContext as parameters to the function,
// which will use registers instead of stack to pass these parameters.
// f(ctx: ExprContext) will always use the stack to pass ctx.
//
// NO! f(x: Option<&i32>) will also use the stack to pass x!
struct ExprContext<'exe> {
    ecxt_scantuple: Option<&'exe Vec<Rc<DatumBlock>>>,
    ecxt_innertuple: Option<&'exe Vec<Rc<DatumBlock>>>,
    ecxt_outertuple: Option<&'exe Vec<Rc<DatumBlock>>>,
    state: &'exe WorkerState,
}

impl<'exe> ExprContext<'exe> {
    fn new(state: &'exe WorkerState) -> ExprContext<'exe> {
        Self {
            ecxt_scantuple: None,
            ecxt_innertuple: None,
            ecxt_outertuple: None,
            state,
        }
    }
}

struct ConstState {
    constvalue: Rc<DatumBlock>,
}

struct FuncExprState {
    args: Vec<ExprState>,
    func: FmgrInfo,
    argsval: Vec<Rc<DatumBlock>>,
}

impl FuncExprState {
    fn eval(&mut self, ctx: &ExprContext) -> anyhow::Result<Rc<DatumBlock>> {
        self.argsval.clear();
        for argexpr in &mut self.args {
            self.argsval.push(argexpr.eval(ctx)?);
        }
        (self.func.fn_addr)(&self.func, &self.argsval, ctx.state)
    }
}

enum ExprState {
    Const(ConstState),
    Func(FuncExprState),
}

impl ExprState {
    fn eval(&mut self, ctx: &ExprContext) -> anyhow::Result<Rc<DatumBlock>> {
        match self {
            ExprState::Const(c) => Ok(c.constvalue.clone()),
            ExprState::Func(f) => f.eval(ctx),
        }
    }
}

fn exec_init_expr(node: &sem::Expr, state: &WorkerState) -> anyhow::Result<ExprState> {
    match node {
        sem::Expr::Const(c) => Ok(ExprState::Const(ConstState {
            constvalue: Rc::new(DatumBlock::Single(c.constvalue.clone())),
        })),
        sem::Expr::Func(f) => {
            let mut args = Vec::new();
            for e in &f.args {
                args.push(exec_init_expr(e, state)?);
            }
            let fn_addr = get_fn_addr(f.funcid, state.fmgr_builtins)?;
            Ok(ExprState::Func(FuncExprState {
                argsval: Vec::with_capacity(args.len()),
                args,
                func: FmgrInfo {
                    fn_oid: f.funcid,
                    fn_addr,
                },
            }))
        }
    }
}

struct ProjectionInfo {
    pi_state: Vec<ExprState>,
    result: Vec<Rc<DatumBlock>>,
}

impl ProjectionInfo {
    fn eval(&mut self, ctx: &ExprContext) -> anyhow::Result<&Vec<Rc<DatumBlock>>> {
        self.result.clear();
        for col in &mut self.pi_state {
            self.result.push(col.eval(ctx)?);
        }
        Ok(&self.result)
    }

    fn try_new(tlist: &Vec<sem::TargetEntry>, state: &WorkerState) -> anyhow::Result<Self> {
        let mut v = Vec::new();
        for e in tlist {
            v.push(exec_init_expr(&e.expr, state)?);
        }
        Ok(ProjectionInfo {
            result: Vec::with_capacity(tlist.len()),
            pi_state: v,
        })
    }
}

// // Put only fields that will be needed by all operators in PlanStateBase.
// struct PlanStateBase<'exe> {
//     // plan: &'opt optimizer::Plan<'syn>, // It is not always necessary.
//     // lefttree: Option<Box<PlanState<'syn, 'opt, 'exe>>>,
//     // righttree: Option<Box<PlanState<'syn, 'opt, 'exe>>>,
//     state: &'exe WorkerState,
// }

struct ResultState<'exe> {
    state: &'exe WorkerState,
    proj_info: ProjectionInfo,
    resconstantqual: Option<ExprState>,
    rs_done: bool,
    rs_checkqual: bool,
    lefttree: Option<Box<PlanState<'exe>>>,
    qual: Vec<ExprState>,
}

impl ResultState<'_> {
    fn exec(&mut self) -> anyhow::Result<Option<&Vec<Rc<DatumBlock>>>> {
        if !self.rs_done {
            self.rs_done = true;
            self.proj_info
                .eval(&ExprContext::new(self.state))
                .map(|v| Some(v))
        } else {
            Ok(None)
        }
    }
}

enum PlanState<'exe> {
    Result(ResultState<'exe>),
}

impl PlanState<'_> {
    fn exec(&mut self) -> anyhow::Result<Option<&Vec<Rc<DatumBlock>>>> {
        match self {
            PlanState::Result(s) => s.exec(),
        }
    }
}

fn exec_init_result<'syn, 'opt, 'exe>(
    node: &'opt optimizer::Result<'syn>,
    state: &'exe WorkerState,
) -> anyhow::Result<ResultState<'exe>> {
    Ok(ResultState {
        state,
        proj_info: ProjectionInfo::try_new(&node.tlist, state)?,
        resconstantqual: None,
        rs_done: false,
        rs_checkqual: false,
        lefttree: match node.lefttree {
            None => None,
            Some(ref v) => exec_init_plan(&v, state).map(|v| Some(Box::new(v)))?,
        },
        qual: Vec::new(),
    })
}

fn exec_init_plan<'syn, 'opt, 'exe>(
    node: &'opt optimizer::Plan<'syn>,
    state: &'exe WorkerState,
) -> anyhow::Result<PlanState<'exe>> {
    match node {
        optimizer::Plan::Result(r) => exec_init_result(r, state).map(|v| PlanState::Result(v)),
    }
}

pub fn exec_select(
    stmt: &PlannedStmt,
    session: &SessionState,
    dest: &mut dyn DestReceiver,
) -> anyhow::Result<()> {
    let state = WorkerState::new(session);
    let mut planstate = exec_init_plan(&stmt.plan_tree, &state)?;
    dest.startup(stmt.plan_tree.tlist())?;
    loop {
        match planstate.exec()? {
            None => break,
            Some(tuples) => dest.receive(tuples)?,
        }
    }
    Ok(())
}

pub fn exec_iud(_stmt: &PlannedStmt, _session: &SessionState) -> anyhow::Result<u64> {
    Err(anyhow!("Biu"))
}

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

use crate::datums::Datums;
use crate::optimizer;
use crate::optimizer::PlannedStmt;
use crate::parser::sem::{self, ExprHash};
use crate::utils::fmgr::{get_fn_addr, FmgrInfo};
use crate::utils::{SessionState, WorkerState};
use std::collections::HashMap;
use std::rc::Rc;

pub trait DestReceiver {
    fn startup(&mut self, tlist: &Vec<sem::TargetEntry>) -> anyhow::Result<()>;
    fn receive(
        &mut self,
        tuples: &[Rc<Datums>],
        rownum: u32,
        worker: &WorkerState,
    ) -> anyhow::Result<()>;
}

struct ExprInitCtx {
    nextid: usize,
    exprid: HashMap<ExprHash, usize>,
}

impl ExprInitCtx {
    fn new() -> Self {
        Self {
            nextid: 0,
            exprid: HashMap::new(),
        }
    }

    fn advance(&mut self) -> usize {
        let r = self.nextid;
        self.nextid += 1;
        return r;
    }
}

// At present, we directly pass the fields of ExprContext as parameters to the function,
// which will use registers instead of stack to pass these parameters.
// f(ctx: ExprContext) will always use the stack to pass ctx.
//
// NO! f(x: Option<&i32>) will also use the stack to pass x!
struct ExprContext<'exe> {
    state: &'exe WorkerState,
    results: &'exe mut [Rc<Datums>],
}

impl<'exe> ExprContext<'exe> {
    fn new(state: &'exe WorkerState, results: &'exe mut [Rc<Datums>]) -> ExprContext<'exe> {
        Self { state, results }
    }
}

// CommonExprState should always be placed first so that ExprState::common_state can erase the match expr.
struct CommonExprState {
    residx: usize,
}

struct RefRes {
    es: CommonExprState,
}

impl RefRes {
    fn new(idx: usize) -> Self {
        return RefRes {
            es: CommonExprState { residx: idx },
        };
    }
}

struct ConstState {
    es: CommonExprState,
    v: Rc<Datums>,
}

impl ConstState {
    fn eval(&mut self, ctx: &mut ExprContext) -> anyhow::Result<()> {
        ctx.results[self.es.residx] = Datums::clonerc(&self.v);
        return Ok(());
    }
}

struct FuncExprState {
    es: CommonExprState,
    args: Vec<ExprState>,
    func: FmgrInfo,
    argsval: Vec<Rc<Datums>>,
}

struct Clear(*mut Vec<Rc<Datums>>);

impl Drop for Clear {
    fn drop(&mut self) {
        unsafe { &mut *self.0 }.clear();
    }
}

impl FuncExprState {
    fn eval(&mut self, ctx: &mut ExprContext) -> anyhow::Result<()> {
        let _clear = Clear(&mut self.argsval as *mut _);
        for argexpr in &mut self.args {
            argexpr.eval(ctx)?;
            let rescln = Datums::clonerc(&ctx.results[argexpr.es().residx]);
            self.argsval.push(rescln);
        }
        (self.func.fn_addr)(
            &self.func,
            &mut ctx.results[self.es.residx],
            &self.argsval,
            ctx.state,
        )?;
        return Ok(());
    }
}

enum ExprState {
    Const(ConstState),
    Func(FuncExprState),
    RefRes(RefRes),
}

impl ExprState {
    fn eval(&mut self, ctx: &mut ExprContext) -> anyhow::Result<()> {
        match self {
            ExprState::Const(c) => c.eval(ctx),
            ExprState::Func(f) => f.eval(ctx),
            ExprState::RefRes(_) => {
                return Ok(());
            }
        }
    }

    fn es(&self) -> &CommonExprState {
        // Release build will erase the match expr:
        // ExprState::common_state:
        //      movsd rax, qword ptr [rdi + 8]
        //      ret
        match self {
            ExprState::Const(c) => &c.es,
            ExprState::Func(f) => &f.es,
            ExprState::RefRes(r) => &r.es,
        }
    }
}

fn exec_init_const(
    node: &sem::Const,
    _: &WorkerState,
    initctx: &mut ExprInitCtx,
) -> anyhow::Result<ExprState> {
    let residx = initctx.advance();
    let ret = ExprState::Const(ConstState {
        es: CommonExprState { residx },
        v: Rc::new(node.v.clone()),
    });
    return Ok(ret);
}

fn exec_init_func(
    node: &sem::FuncExpr,
    state: &WorkerState,
    initctx: &mut ExprInitCtx,
) -> anyhow::Result<ExprState> {
    let mut args = Vec::new();
    for e in &node.args {
        args.push(exec_init_expr(e, state, initctx)?);
    }
    let fn_addr = get_fn_addr(node.funcid, state.fmgr_builtins)?;
    let residx = initctx.advance();
    return Ok(ExprState::Func(FuncExprState {
        es: CommonExprState { residx },
        argsval: Vec::with_capacity(args.len()),
        args,
        func: FmgrInfo {
            fn_oid: node.funcid,
            fn_addr,
        },
    }));
}

fn exec_init_expr(
    node: &sem::Expr,
    state: &WorkerState,
    initctx: &mut ExprInitCtx,
) -> anyhow::Result<ExprState> {
    let exprhash = node.hash();
    if let Some(&idx) = initctx.exprid.get(&exprhash) {
        return Ok(ExprState::RefRes(RefRes::new(idx)));
    }

    let exprstate = match node {
        sem::Expr::Const(c) => exec_init_const(c, state, initctx)?,
        sem::Expr::Func(f) => exec_init_func(f, state, initctx)?,
    };

    initctx.exprid.insert(exprhash, exprstate.es().residx);
    return Ok(exprstate);
}

struct ProjectionInfo {
    pi_state: Vec<ExprState>,
}

impl ProjectionInfo {
    fn eval(&mut self, ctx: &mut ExprContext) -> anyhow::Result<()> {
        for col in &mut self.pi_state {
            col.eval(ctx)?;
        }
        return Ok(());
    }

    fn try_new(
        tlist: &Vec<sem::TargetEntry>,
        state: &WorkerState,
        initctx: &mut ExprInitCtx,
    ) -> anyhow::Result<Self> {
        let mut pi_state = Vec::with_capacity(tlist.len());
        for e in tlist {
            let exprstat = exec_init_expr(&e.expr, state, initctx)?;
            pi_state.push(exprstat);
        }
        Ok(ProjectionInfo { pi_state })
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
    done: bool,
    results: Vec<Rc<Datums>>,
    ret: Vec<Rc<Datums>>,
}

impl ResultState<'_> {
    fn exec(
        &mut self,
    ) -> anyhow::Result<(
        /* rows */ Option<&[Rc<Datums>]>,
        /* rownumber */ u32,
    )> {
        if self.done {
            return Ok((None, 0));
        }
        self.done = true;
        self.ret.clear();
        for res in (&mut self.results).iter_mut().rev() {
            if Rc::strong_count(res) > 1 {
                // Delay this allocation?
                *res = Rc::new(Datums::new());
            }
        }
        let mut ectx = ExprContext::new(self.state, &mut self.results);

        self.proj_info.eval(&mut ectx)?;
        for expr in &self.proj_info.pi_state {
            let rescln = Datums::clonerc(&self.results[expr.es().residx]);
            self.ret.push(rescln);
        }
        return Ok((Some(&self.ret), 1));
    }
}

enum PlanState<'exe> {
    Result(ResultState<'exe>),
}

impl PlanState<'_> {
    fn exec(
        &mut self,
    ) -> anyhow::Result<(
        /* rows */ Option<&[Rc<Datums>]>,
        /* rownumber */ u32,
    )> {
        match self {
            PlanState::Result(s) => s.exec(),
        }
    }
}

fn exec_init_result<'opt, 'exe>(
    node: &'opt optimizer::Result,
    state: &'exe WorkerState,
) -> anyhow::Result<ResultState<'exe>> {
    let mut initctx = ExprInitCtx::new();
    let proj_info = ProjectionInfo::try_new(&node.plan.tlist, state, &mut initctx)?;
    let mut results = Vec::with_capacity(initctx.nextid);
    results.resize_with(initctx.nextid, Default::default);
    Ok(ResultState {
        state,
        proj_info,
        results,
        done: false,
        ret: Vec::with_capacity(node.plan.tlist.len()),
    })
}

fn exec_init_plan<'opt, 'exe>(
    node: &'opt optimizer::Plan,
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
        let (rows, rownumber) = planstate.exec()?;
        match rows {
            None => break,
            Some(tuples) => dest.receive(tuples, rownumber, &state)?,
        }
    }
    Ok(())
}

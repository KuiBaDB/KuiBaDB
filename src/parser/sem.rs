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
use crate::access::TypeDesc;
use crate::catalog::namespace::SessionExt as NamespaceSessionExt;
use crate::catalog::{get_proc, FormOperator};
use crate::datums::Datums;
use crate::utils::{AttrNumber, SessionState};
use crate::{kbbail, Oid, OptOid, FLOAT8OID, INT4OID, INT8OID, VARCHAROID};
use std::convert::TryInto;
use std::debug_assert;
use std::mem::{align_of, size_of};

// 'syn is the lifetime of syntax tree returned by parser::parse().

pub enum UtilityStmt<'syn, 'input> {
    VariableSet(&'syn syn::VariableSetStmt<'input>),
    VariableShow(&'syn syn::VariableShowStmt<'input>),
    DefineType(&'syn syn::DefineTypeStmt<'input>),
    CreateTable(&'syn syn::CreateTableStmt<'input>),
    Tran(&'syn syn::TranStmt),
    Lock(&'syn syn::LockStmt<'input>),
}

pub type ExprHash = md5::Digest;

#[derive(Debug, Clone)]
pub struct Const {
    pub typ: TypeDesc,
    pub v: Datums, // v.is_single() == true!
    pub loc: syn::Location,
}

impl Const {
    fn try_new(input: &syn::AConst) -> anyhow::Result<Self> {
        let (constv, consttypeoid, constlen, constalign) = match &input.val {
            syn::Value::Num(v) => match v {
                &syn::NumVal::Int(i) => (
                    Datums::new_single_fixedlen(i),
                    INT4OID,
                    size_of::<i32>() as i16,
                    align_of::<i32>(),
                ),
                &syn::NumVal::Float { neg, v } => {
                    if let Ok(i) = v.parse::<i64>() {
                        (
                            Datums::new_single_fixedlen(if neg { -i } else { i }),
                            INT8OID,
                            size_of::<i64>() as i16,
                            align_of::<i64>(),
                        )
                    } else {
                        let v: f64 = v.parse()?;
                        (
                            Datums::new_single_fixedlen(if neg { -v } else { v }),
                            FLOAT8OID,
                            size_of::<f64>() as i16,
                            align_of::<f64>(),
                        )
                    }
                }
            },
            syn::Value::Str(v) => (
                Datums::new_single_varchar(v.as_str().as_bytes()),
                VARCHAROID,
                -1,
                align_of::<usize>(), /* unused */
            ),
        };
        Ok(Const {
            typ: TypeDesc {
                id: consttypeoid,
                len: constlen,
                align: constalign as u8,
                mode: -1,
            },
            v: constv,
            loc: input.loc,
        })
    }

    pub fn hash(&self) -> ExprHash {
        let mut md5h = md5::Context::new();
        md5h.consume((9188113448065398074u64).to_ne_bytes());
        self.typ.hash(&mut md5h);
        match self.typ.id {
            INT4OID => {
                md5h.consume(self.v.get_single_fixedlen::<i32>().to_ne_bytes());
            }
            INT8OID => {
                md5h.consume(self.v.get_single_fixedlen::<i64>().to_ne_bytes());
            }
            FLOAT8OID => {
                md5h.consume(self.v.get_single_fixedlen::<f64>().to_ne_bytes());
            }
            VARCHAROID => {
                md5h.consume(self.v.get_single_varchar().as_bytes());
            }
            _ => {
                unreachable!("unknown typ: {:?}", self.typ);
            }
        }
        return md5h.compute();
    }
}

#[derive(Debug, Clone)]
pub struct FuncExpr {
    pub funcresulttype: Oid,
    pub funcid: Oid,
    pub args: Vec<Expr>,
    pub loc: syn::Location,
}

impl FuncExpr {
    pub fn hash(&self) -> ExprHash {
        let mut md5h = md5::Context::new();
        md5h.consume((8347806344167239419u64).to_ne_bytes());
        md5h.consume(self.funcresulttype.get().to_ne_bytes());
        md5h.consume(self.funcid.get().to_ne_bytes());
        for arg in &self.args {
            md5h.consume(arg.hash().0);
        }
        return md5h.compute();
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    Const(Const),
    Func(FuncExpr),
}

impl Expr {
    pub fn val_type(&self) -> Oid {
        match self {
            Expr::Const(v) => v.typ.id,
            Expr::Func(v) => v.funcresulttype,
        }
    }

    pub fn hash(&self) -> ExprHash {
        match self {
            Expr::Const(v) => v.hash(),
            Expr::Func(v) => v.hash(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TargetEntry {
    pub expr: Expr,
    pub resno: AttrNumber,
    pub resname: Option<String>,
}

#[derive(Debug)]
pub enum CmdType {
    Select,
}

#[derive(Debug)]
pub struct Query {
    pub cmdtype: CmdType,
    pub tlist: Vec<TargetEntry>,
}

pub enum Stmt<'syn, 'input> {
    Utility(UtilityStmt<'syn, 'input>),
    Optimizable(Query),
}

struct ParseState<'a> {
    sess_state: &'a mut SessionState,
    p_expr_kind: ParseExprKind,
    p_next_resno: AttrNumber,
}

#[derive(PartialEq, Clone, Copy)]
enum ParseExprKind {
    None = 0,
    SelectTarget,
}

fn binary_oper_exact(
    session: &mut SessionState,
    opname: &Vec<syn::StrVal>,
    oprleft: Oid,
    oprright: Oid,
) -> anyhow::Result<FormOperator> {
    session.opername_get_oprid(opname, OptOid(Some(oprleft)), oprright)
}

fn oper(
    session: &mut SessionState,
    opname: &Vec<syn::StrVal>,
    oprleft: Oid,
    oprright: Oid,
) -> anyhow::Result<FormOperator> {
    binary_oper_exact(session, opname, oprleft, oprright)
}

fn left_oper(
    session: &mut SessionState,
    opname: &Vec<syn::StrVal>,
    oprright: Oid,
) -> anyhow::Result<FormOperator> {
    session.opername_get_oprid(opname, OptOid(None), oprright)
}

fn make_op(
    pstate: &mut ParseState,
    opname: &Vec<syn::StrVal>,
    ltree: Option<Expr>,
    rtree: Expr,
    loc: syn::Location,
) -> anyhow::Result<FuncExpr> {
    let (op, args) = match ltree {
        None => {
            let rtype = rtree.val_type();
            (
                left_oper(&mut pstate.sess_state, opname, rtype)?,
                vec![rtree],
            )
        }
        Some(ltree) => {
            let ltype = ltree.val_type();
            let rtype = rtree.val_type();
            (
                oper(&mut pstate.sess_state, opname, ltype, rtype)?,
                vec![ltree, rtree],
            )
        }
    };
    let oprcode = match op.oprcode.0 {
        None => {
            kbbail!(
                ERRCODE_UNDEFINED_FUNCTION,
                "operator is only a shell. op={}",
                op.oid
            );
        }
        Some(v) => v,
    };
    let oprfunc = get_proc(&pstate.sess_state, oprcode)?;
    Ok(FuncExpr {
        funcid: oprcode,
        funcresulttype: oprfunc.prorettype,
        args,
        loc,
    })
}

fn transform_a_expr_op(pstate: &mut ParseState, expr: &syn::AExpr) -> anyhow::Result<FuncExpr> {
    match *expr.oprands {
        syn::AExprOprands::One(ref e) => {
            let e = transform_expr_recurse(pstate, e)?;
            make_op(pstate, &expr.name, None, e, expr.loc)
        }
        syn::AExprOprands::Two(ref l, ref r) => {
            let l = transform_expr_recurse(pstate, l)?;
            let r = transform_expr_recurse(pstate, r)?;
            make_op(pstate, &expr.name, Some(l), r, expr.loc)
        }
    }
}

fn transform_expr_recurse(pstate: &mut ParseState, expr: &syn::Expr) -> anyhow::Result<Expr> {
    match expr {
        syn::Expr::AConst(v) => Const::try_new(v).map(|v| Expr::Const(v)),
        syn::Expr::AExpr(v) => transform_a_expr_op(pstate, v).map(|v| Expr::Func(v)),
    }
}

fn transform_expr(
    pstate: &mut ParseState,
    expr: &syn::Expr,
    ekind: ParseExprKind,
) -> anyhow::Result<Expr> {
    debug_assert!(ekind != ParseExprKind::None);
    let sv_expr_kind = pstate.p_expr_kind;
    pstate.p_expr_kind = ekind;
    let ret = transform_expr_recurse(pstate, expr);
    pstate.p_expr_kind = sv_expr_kind;
    ret
}

// FigureColname
fn figure_colname<'syn>(_node: &'syn syn::Expr) -> String {
    "?column?".to_string()
}

// transformTargetEntry
fn transform_target_entry<'syn>(
    pstate: &mut ParseState,
    node: &'syn syn::Expr,
    ekind: ParseExprKind,
    colname: Option<String>,
) -> anyhow::Result<TargetEntry> {
    let expr = transform_expr(pstate, node, ekind)?;
    let resname = match colname {
        None => figure_colname(node),
        Some(v) => v,
    };
    let resno = pstate.p_next_resno;
    pstate.p_next_resno = (pstate.p_next_resno.get() + 1).try_into()?;
    Ok(TargetEntry {
        resno,
        expr,
        resname: Some(resname),
    })
}

// transformTargetList
fn transform_target_list<'syn>(
    pstate: &mut ParseState,
    tlist: &'syn Vec<syn::ResTarget>,
    ekind: ParseExprKind,
) -> anyhow::Result<Vec<TargetEntry>> {
    let mut v = Vec::<TargetEntry>::with_capacity(tlist.len());
    for target in tlist {
        v.push(transform_target_entry(
            pstate,
            &target.val,
            ekind,
            target.name.as_ref().map(|v| v.to_string()),
        )?);
    }
    Ok(v)
}

// transformSelectStmt
fn transform_select_stmt<'syn, 'input>(
    pstate: &mut ParseState,
    stmt: &'syn syn::SelectStmt<'input>,
) -> anyhow::Result<Query> {
    let tlist = transform_target_list(pstate, &stmt.tlist, ParseExprKind::SelectTarget)?;
    Ok(Query {
        cmdtype: CmdType::Select,
        tlist,
    })
}

// parse_analyze
pub fn kb_analyze<'syn, 'input>(
    state: &mut SessionState,
    stmt: &'syn syn::Stmt<'input>,
) -> anyhow::Result<Stmt<'syn, 'input>> {
    match stmt {
        syn::Stmt::VariableSet(v) => Ok(Stmt::Utility(UtilityStmt::VariableSet(v))),
        syn::Stmt::VariableShow(v) => Ok(Stmt::Utility(UtilityStmt::VariableShow(v))),
        syn::Stmt::DefineType(v) => Ok(Stmt::Utility(UtilityStmt::DefineType(v))),
        syn::Stmt::Tran(v) => Ok(Stmt::Utility(UtilityStmt::Tran(v))),
        syn::Stmt::Select(v) => {
            let mut pstate = ParseState {
                sess_state: state,
                p_expr_kind: ParseExprKind::None,
                p_next_resno: 1.try_into().unwrap(),
            };
            transform_select_stmt(&mut pstate, v).map(|v| Stmt::Optimizable(v))
        }
        syn::Stmt::CreateTable(v) => Ok(Stmt::Utility(UtilityStmt::CreateTable(v))),
        syn::Stmt::Lock(v) => Ok(Stmt::Utility(UtilityStmt::Lock(v))),
        syn::Stmt::Empty => unreachable!(),
    }
}

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

// 'input lifetime is the lifetime of query inputted by the user
use crate::access::lmgr::LockMode;

#[derive(Debug)]
pub enum StrVal<'input> {
    InPlace(&'input str),
    Dyn(String),
}

impl std::ops::Deref for StrVal<'_> {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl StrVal<'_> {
    pub fn as_str(&self) -> &str {
        match self {
            &StrVal::InPlace(val) => val,
            StrVal::Dyn(val) => val.as_str(),
        }
    }
}

#[derive(Debug)]
pub enum NumVal<'input> {
    Int(i32),
    // There is no +/- in v
    Float { neg: bool, v: &'input str },
}

impl NumVal<'_> {
    pub fn neg(self) -> Self {
        match self {
            NumVal::Int(v) => NumVal::Int(-v),
            NumVal::Float { neg, v } => NumVal::Float { neg: !neg, v },
        }
    }
}

#[derive(Debug)]
pub enum Value<'input> {
    Num(NumVal<'input>),
    Str(StrVal<'input>),
}

#[derive(Debug, Clone, Copy)]
pub struct Location {
    pub s: usize,
    pub e: usize,
}

#[derive(Debug)]
pub struct AConst<'input> {
    pub val: Value<'input>,
    pub loc: Location,
}

#[derive(Debug)]
pub struct VariableSetStmt<'input> {
    pub name: StrVal<'input>,
    pub val: AConst<'input>,
}

#[derive(Debug)]
pub struct VariableShowStmt<'input> {
    pub name: StrVal<'input>,
}

#[derive(Debug)]
pub enum TranStmt {
    Begin,
    Abort,
    Commit,
}

#[derive(Debug)]
pub enum Stmt<'input> {
    VariableSet(VariableSetStmt<'input>),
    VariableShow(VariableShowStmt<'input>),
    DefineType(DefineTypeStmt<'input>),
    Select(SelectStmt<'input>),
    Tran(TranStmt),
    CreateTable(CreateTableStmt<'input>),
    Lock(LockStmt<'input>),
    Empty,
}

impl Stmt<'_> {
    pub fn is_tran_exit(&self) -> bool {
        match self {
            Stmt::Tran(TranStmt::Commit) | Stmt::Tran(TranStmt::Abort) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod syn_test {
    use super::{Stmt, TranStmt};

    #[test]
    fn f() {
        let s = Stmt::Tran(TranStmt::Commit);
        assert!(s.is_tran_exit());
        let s = Stmt::Tran(TranStmt::Abort);
        assert!(s.is_tran_exit());
        let s = Stmt::Tran(TranStmt::Begin);
        assert!(!s.is_tran_exit());
    }
}

#[derive(Debug)]
pub struct DefElemVal<'input> {
    pub defnamespace: Option<StrVal<'input>>,
    pub defname: StrVal<'input>,
    pub arg: Value<'input>,
}

#[derive(Debug)]
pub enum DefElem<'input> {
    Unspec(DefElemVal<'input>),
    Add(DefElemVal<'input>),
    Set(DefElemVal<'input>),
    Drop(StrVal<'input>),
}

pub fn make_def_elem<'input>(defname: StrVal<'input>, arg: Value<'input>) -> DefElem<'input> {
    DefElem::Unspec(DefElemVal {
        defnamespace: None,
        defname,
        arg,
    })
}

#[derive(Debug)]
pub struct DefineTypeStmt<'input> {
    pub defnames: Vec<StrVal<'input>>,
    pub definition: Vec<DefElem<'input>>,
}

#[derive(Debug)]
pub enum Expr<'input> {
    AConst(AConst<'input>),
    AExpr(AExpr<'input>),
}

#[derive(Debug)]
pub enum AExprKind {
    Op,
}

#[derive(Debug)]
pub enum AExprOprands<'input> {
    One(Expr<'input>),
    Two(Expr<'input>, Expr<'input>),
}

#[derive(Debug)]
pub struct AExpr<'input> {
    pub kind: AExprKind,
    pub name: Vec<StrVal<'input>>,
    pub oprands: Box<AExprOprands<'input>>,
    pub loc: Location,
}

#[derive(Debug)]
pub struct InsertResTarget<'input> {
    pub name: Option<StrVal<'input>>,
    pub loc: Location,
}

#[derive(Debug)]
pub struct ResTarget<'input> {
    pub name: Option<StrVal<'input>>,
    pub val: Expr<'input>,
    pub loc: Location,
}

#[derive(Debug)]
pub struct SelectStmt<'input> {
    // tlist may be empty. `select from table` is valid.
    pub tlist: Vec<ResTarget<'input>>,
}

#[derive(Debug)]
pub struct Alias<'input> {
    pub aliasname: StrVal<'input>,
    pub colnames: Vec<StrVal<'input>>,
}

#[derive(Debug)]
pub struct RangeVar<'input> {
    pub schemaname: Option<StrVal<'input>>,
    pub relname: StrVal<'input>,
    pub alias: Option<Alias<'input>>,
}

#[derive(Debug)]
pub struct TypeName<'input> {
    pub names: Vec<StrVal<'input>>,
    pub typmods: Vec<&'input str>,
}

pub fn system_type_name(name: &str) -> TypeName<'_> {
    TypeName {
        typmods: Vec::new(),
        names: vec![StrVal::InPlace("kb_catalog"), StrVal::InPlace(name)],
    }
}

#[derive(Debug)]
pub struct ColumnDef<'input> {
    pub colname: StrVal<'input>,
    pub typename: TypeName<'input>,
}

// PG CreateStmt
#[derive(Debug)]
pub struct CreateTableStmt<'input> {
    pub relation: RangeVar<'input>,
    pub table_elts: Vec<ColumnDef<'input>>,
    pub opts: Vec<DefElem<'input>>,
}

// PG CreateStmt
#[derive(Debug)]
pub struct LockStmt<'input> {
    pub rels: Vec<RangeVar<'input>>,
    pub mode: LockMode,
}

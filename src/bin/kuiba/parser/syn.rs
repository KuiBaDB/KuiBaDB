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
pub enum Stmt<'input> {
    VariableSet(VariableSetStmt<'input>),
    VariableShow(VariableShowStmt<'input>),
    DefineType(DefineTypeStmt<'input>),
    Select(SelectStmt<'input>),
    Empty,
}

#[derive(Debug)]
pub struct DefElemAdd<'input> {
    defnamespace: StrVal<'input>,
    defname: StrVal<'input>,
}

#[derive(Debug)]
pub struct DefineTypeStmt<'input> {
    pub defnames: Vec<StrVal<'input>>,
    pub definition: Option<Vec<DefElemAdd<'input>>>,
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

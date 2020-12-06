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
#[allow(non_camel_case_types)]
pub struct A_Const<'input> {
    pub val: Value<'input>,
    pub loc: Location,
}

#[derive(Debug)]
pub struct VariableSetStmt<'input> {
    pub name: StrVal<'input>,
    pub val: A_Const<'input>,
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
pub enum DefElemVal<'input> {
    Val(Value<'input>),
}

#[derive(Debug)]
pub struct DefElemAdd<'input> {
    defnamespace: StrVal<'input>,
    defname: StrVal<'input>,
    arg: DefElemVal<'input>,
}

pub struct DefElemSet<'input> {
    elem: DefElemAdd<'input>,
}

pub struct DefElemDrop<'input> {
    defnamespace: StrVal<'input>,
    defname: StrVal<'input>,
}

#[derive(Debug)]
pub struct DefineTypeStmt<'input> {
    pub defnames: Vec<StrVal<'input>>,
    pub definition: Option<Vec<DefElemAdd<'input>>>,
}

#[allow(non_camel_case_types)]
#[derive(Debug)]
pub enum Expr<'input> {
    A_Const(A_Const<'input>),
    A_Expr(A_Expr<'input>),
}

#[allow(non_camel_case_types)]
#[derive(Debug)]
pub enum A_Expr_Kind {
    AEXPR_OP,
    AEXPR_OP_ANY,
    AEXPR_OP_ALL,
    AEXPR_DISTINCT,
    AEXPR_NOT_DISTINCT,
    AEXPR_NULLIF,
    AEXPR_IN,
    AEXPR_LIKE,
    AEXPR_ILIKE,
    AEXPR_SIMILAR,
    AEXPR_BETWEEN,
    AEXPR_NOT_BETWEEN,
    AEXPR_BETWEEN_SYM,
    AEXPR_NOT_BETWEEN_SYM,
    AEXPR_PAREN,
}

#[derive(Debug)]
pub enum AExprOprands<'input> {
    One(Expr<'input>),
    Two(Expr<'input>, Expr<'input>),
}

#[allow(non_camel_case_types)]
#[derive(Debug)]
pub struct A_Expr<'input> {
    pub kind: A_Expr_Kind,
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

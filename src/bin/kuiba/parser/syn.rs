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

#[derive(Debug)]
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
    Empty,
}

/*
Copyright 2020 <盏一 w@hidva.com>
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
use crate::commands::typecmds::define_type;
use crate::parser::{sem, syn};
use crate::{guc, protocol, ErrCode, SessionState};
use anyhow::{anyhow, Context};
use std::sync::Arc;

pub struct StrResp {
    pub name: String,
    pub val: String,
}

pub struct Response {
    pub resp: Option<StrResp>,
    pub tag: &'static str,
}

fn to_i32(val: &syn::Value) -> anyhow::Result<i32> {
    match val {
        syn::Value::Num(v) => match v {
            &syn::NumVal::Int(v) => Ok(v),
            &syn::NumVal::Float { neg, v } => {
                let v = v.parse::<f64>()?;
                Ok((if neg { -v } else { v }) as i32)
            }
        },
        syn::Value::Str(v) => Ok(v.parse::<i32>()?),
    }
}

fn to_bool(val: &syn::Value) -> anyhow::Result<bool> {
    match val {
        syn::Value::Num(v) => match v {
            &syn::NumVal::Int(v) => Ok(v != 0),
            &syn::NumVal::Float { .. } => Err(anyhow!("requires a Boolean value"))
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
        },
        syn::Value::Str(v) => Ok(v.eq_ignore_ascii_case("on") || v.eq_ignore_ascii_case("true")),
    }
}

fn to_f64(val: &syn::Value) -> anyhow::Result<f64> {
    match val {
        syn::Value::Num(v) => match v {
            &syn::NumVal::Int(v) => Ok(v as f64),
            &syn::NumVal::Float { neg, v } => {
                let v = v.parse::<f64>()?;
                Ok(if neg { -v } else { v })
            }
        },
        syn::Value::Str(v) => Ok(v.parse::<f64>()?),
    }
}

fn to_str(val: &syn::Value) -> anyhow::Result<String> {
    Ok(match val {
        syn::Value::Num(v) => match v {
            &syn::NumVal::Int(v) => v.to_string(),
            &syn::NumVal::Float { neg, v } => {
                if !neg {
                    v.to_string()
                } else {
                    format!("-{}", v)
                }
            }
        },
        syn::Value::Str(v) => v.to_string(),
    })
}

fn set_guc(stmt: &syn::VariableSetStmt, state: &mut SessionState) -> anyhow::Result<Response> {
    let gucname = &stmt.name;
    let val = &stmt.val.val;
    let gucidx = match guc::get_gucidx(gucname) {
        Some(v) => v,
        None => {
            return Err(anyhow!("unknown guc")).context(ErrCode(protocol::ERRCODE_UNDEFINED_OBJECT))
        }
    };
    let gucstate = Arc::make_mut(&mut state.gucstate);
    match gucidx {
        guc::GucIdx::I(idx) => {
            let gucval = to_i32(val)?;
            guc::set_int_guc(idx, gucval, gucstate);
        }
        guc::GucIdx::R(idx) => {
            let gucval = to_f64(val)?;
            guc::set_real_guc(idx, gucval, gucstate);
        }
        guc::GucIdx::S(idx) => {
            let gucval = to_str(val)?;
            guc::set_str_guc(idx, gucval, gucstate);
        }
        guc::GucIdx::B(idx) => {
            let gucval = to_bool(val)?;
            guc::set_bool_guc(idx, gucval, gucstate);
        }
    }
    Ok(Response {
        resp: None,
        tag: "SET",
    })
}

fn get_guc(stmt: &syn::VariableShowStmt, state: &SessionState) -> anyhow::Result<Response> {
    let gucname = &stmt.name;
    let gucidx = match guc::get_gucidx(gucname) {
        Some(v) => v,
        None => {
            return Err(anyhow!("unknown guc")).context(ErrCode(protocol::ERRCODE_UNDEFINED_OBJECT))
        }
    };
    let generic = guc::get_guc_generic(gucidx);
    let gucshow = guc::show(generic, &state.gucstate, gucidx);
    Ok(Response {
        resp: Some(StrResp {
            name: gucname.to_string(),
            val: gucshow,
        }),
        tag: "SHOW",
    })
}

pub fn process_utility(
    stmt: &sem::UtilityStmt,
    state: &mut SessionState,
) -> anyhow::Result<Response> {
    match stmt {
        &sem::UtilityStmt::VariableSet(v) => set_guc(v, state),
        &sem::UtilityStmt::VariableShow(v) => get_guc(v, state),
        &sem::UtilityStmt::DefineType(v) => define_type(v, state),
    }
}

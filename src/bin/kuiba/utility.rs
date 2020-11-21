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
use crate::{guc, protocol, ErrCode, SessionState};
use anyhow::{anyhow, Context};
use sqlparser::ast::{Ident, SetVariableValue, Statement, Value};
use std::sync::Arc;

pub struct StrResp {
    pub name: String,
    pub val: String,
}

pub struct Response {
    pub resp: Option<StrResp>,
    pub tag: String,
}

fn to_i32(val: &SetVariableValue) -> anyhow::Result<i32> {
    match val {
        SetVariableValue::Ident(_) => {
            Err(anyhow!("to_i32 failed")).context(ErrCode(protocol::ERRCODE_SYNTAX_ERROR))
        }
        SetVariableValue::Literal(literal) => match literal {
            Value::Number(rawval) => rawval
                .parse()
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
            Value::SingleQuotedString(rawval) => rawval
                .parse()
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
            _ => Err(anyhow!("to_i32 failed"))
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
        },
    }
}

fn to_bool(val: &SetVariableValue) -> anyhow::Result<bool> {
    match val {
        SetVariableValue::Ident(_) => {
            Err(anyhow!("to_bool failed")).context(ErrCode(protocol::ERRCODE_SYNTAX_ERROR))
        }
        SetVariableValue::Literal(literal) => match literal {
            Value::Number(rawval) => rawval
                .parse::<i32>()
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE))
                .map(|v| v != 0),
            Value::SingleQuotedString(rawval) => Ok(if rawval.eq_ignore_ascii_case("on") {
                true
            } else {
                false
            }),
            &Value::Boolean(rawval) => Ok(rawval),
            _ => Err(anyhow!("to_bool failed"))
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
        },
    }
}

fn to_f64(val: &SetVariableValue) -> anyhow::Result<f64> {
    match val {
        SetVariableValue::Ident(_) => {
            Err(anyhow!("to_f64 failed")).context(ErrCode(protocol::ERRCODE_SYNTAX_ERROR))
        }
        SetVariableValue::Literal(literal) => match literal {
            Value::Number(rawval) => rawval
                .parse()
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
            Value::SingleQuotedString(rawval) => rawval
                .parse()
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
            _ => Err(anyhow!("to_f64 failed"))
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
        },
    }
}

fn to_str(val: &SetVariableValue) -> anyhow::Result<String> {
    match val {
        SetVariableValue::Ident(Ident { value, .. }) => Ok(value.to_string()),
        SetVariableValue::Literal(literal) => match literal {
            Value::Number(rawval) => Ok(rawval.to_string()),
            Value::SingleQuotedString(rawval) => Ok(rawval.to_string()),
            &Value::Boolean(rawval) => Ok(rawval.to_string()),
            _ => Err(anyhow!("to_str failed"))
                .context(ErrCode(protocol::ERRCODE_INVALID_PARAMETER_VALUE)),
        },
    }
}

fn set_guc(
    name: &Ident,
    val: &SetVariableValue,
    state: &mut SessionState,
) -> anyhow::Result<Response> {
    let gucidx = match guc::get_gucidx(&name.value) {
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
        tag: "SET".to_string(),
    })
}

fn get_guc(name: &Ident, state: &SessionState) -> anyhow::Result<Response> {
    let gucidx = match guc::get_gucidx(&name.value) {
        Some(v) => v,
        None => {
            return Err(anyhow!("unknown guc")).context(ErrCode(protocol::ERRCODE_UNDEFINED_OBJECT))
        }
    };
    let generic = guc::get_guc_generic(gucidx);
    let gucshow = guc::show(generic, &state.gucstate, gucidx);
    Ok(Response {
        resp: Some(StrResp {
            name: name.value.to_string(),
            val: gucshow,
        }),
        tag: "SHOW".to_string(),
    })
}

pub fn process_utility(stmt: &Statement, state: &mut SessionState) -> anyhow::Result<Response> {
    match stmt {
        Statement::SetVariable {
            variable, value, ..
        } => set_guc(variable, value, state),
        Statement::ShowVariable { variable } => get_guc(variable, state),
        _ => Err(anyhow!("process_utility failed. unsupport statement"))
            .context(ErrCode(protocol::ERRCODE_FEATURE_NOT_SUPPORTED)),
    }
}

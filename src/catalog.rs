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
use crate::utils::SessionState;
use crate::{kbanyhow, Oid, OptOid};

pub mod namespace;

#[derive(Debug)]
pub struct FormDataDatabase {
    pub oid: Oid,
    pub datname: String,
    pub datistemplate: bool,
    pub datallowconn: bool,
}

fn column_val<'a>(row: &[(&str, Option<&'a str>)], name: &str) -> Option<&'a str> {
    for &(column, value) in row.iter() {
        if column == name {
            return value;
        }
    }
    None
}

pub fn get_database(datname: &str) -> anyhow::Result<FormDataDatabase> {
    let mut retdb: anyhow::Result<FormDataDatabase> = Err(anyhow::anyhow!("not found"));
    let conn = sqlite::open("global/meta.db")?;
    conn.iterate(
        format!("select * from kb_database where datname = '{}'", datname),
        |row| {
            retdb = Ok(FormDataDatabase {
                oid: column_val(row, "oid").unwrap().parse().unwrap(),
                datname: column_val(row, "datname").unwrap().parse().unwrap(),
                datistemplate: column_val(row, "datistemplate")
                    .unwrap()
                    .parse::<i32>()
                    .unwrap()
                    == 0,
                datallowconn: column_val(row, "datallowconn")
                    .unwrap()
                    .parse::<i32>()
                    .unwrap()
                    == 0,
            });
            true
        },
    )?;
    retdb
}

#[derive(Clone, Copy)]
pub struct FormOperator {
    pub oid: Oid,
    pub oprnamespace: Oid,
    pub oprleft: OptOid,
    pub oprright: Oid,
    pub oprresult: Oid,
    pub oprcode: OptOid,
}

fn get_oper(
    state: &SessionState,
    oprname: &str,
    oprleft: OptOid,
    oprright: Oid,
    oprnamespace: Oid,
) -> anyhow::Result<FormOperator> {
    let opers = get_opers(state, oprname, oprleft, oprright)?;
    for oper in opers {
        if oper.oprnamespace == oprnamespace {
            return Ok(oper);
        }
    }
    let oprleft: u32 = oprleft.into();
    Err(kbanyhow!(
        ERRCODE_UNDEFINED_FUNCTION,
        "operator does not exist. name={} left={} right={} nsp={}",
        oprname,
        oprleft,
        oprright,
        oprnamespace
    ))
}

fn get_opers(
    state: &SessionState,
    oprname: &str,
    oprleft: OptOid,
    oprright: Oid,
) -> anyhow::Result<Vec<FormOperator>> {
    let mut oprs = Vec::new();
    let oprleftval: u32 = oprleft.into();
    state.metaconn.iterate(format!("select oid, oprnamespace, oprresult, oprcode from kb_operator where oprname='{}' and oprleft={} and oprright={}", oprname, oprleftval, oprright), |row| {
        oprs.push(FormOperator {
            oid: column_val(row, "oid").unwrap().parse().unwrap(),
            oprnamespace: column_val(row, "oprnamespace").unwrap().parse().unwrap(),
            oprleft: oprleft,
            oprright: oprright,
            oprresult: column_val(row, "oprresult").unwrap().parse().unwrap(),
            oprcode: column_val(row, "oprcode").unwrap().parse::<u32>().unwrap().into(),
        });
        true
    })?;
    Ok(oprs)
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum ProKind {
    Normal = 102,
    Agg = 97,
    Window = 119,
}

impl std::convert::From<u8> for ProKind {
    fn from(val: u8) -> Self {
        match val {
            102 /* 'f' */ => ProKind::Normal,
            97  /* 'a' */ => ProKind::Agg,
            119 /* 'w' */ => ProKind::Window,
            _ => panic!("Invalid u8 -> ProKind. val={}", val),
        }
    }
}

impl std::convert::From<ProKind> for u8 {
    fn from(val: ProKind) -> u8 {
        val as u8
    }
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum ProVolatile {
    Immu = 105,
    Stable = 115,
    Volatile = 118,
}

impl std::convert::From<u8> for ProVolatile {
    fn from(val: u8) -> Self {
        match val {
            105 /* 'i' */ => ProVolatile::Immu,
            115 /* 's' */ => ProVolatile::Stable,
            118 /* 'v' */ => ProVolatile::Volatile,
            _ => panic!("Invalid u8 -> ProVolatile. val={}", val),
        }
    }
}

impl std::convert::From<ProVolatile> for u8 {
    fn from(val: ProVolatile) -> u8 {
        val as u8
    }
}

pub struct FormProc {
    pub oid: Oid,
    pub pronamespace: Oid,
    pub prokind: ProKind,
    pub provolatile: ProVolatile,
    pub prorettype: Oid,
    pub prosrc: String,
    pub probin: String,
}

pub fn get_proc(state: &SessionState, oid: Oid) -> anyhow::Result<FormProc> {
    let mut ret: anyhow::Result<FormProc> = Err(kbanyhow!(
        ERRCODE_UNDEFINED_FUNCTION,
        "function does not exist. oid={}",
        oid
    ));
    state.metaconn.iterate(format!("select pronamespace, prokind, provolatile, prorettype, prosrc, probin from kb_proc where oid = {}", oid), |row| {
        ret = Ok(FormProc {
            oid: oid,
            pronamespace: column_val(row, "pronamespace").unwrap().parse().unwrap(),
            prokind: column_val(row, "prokind").unwrap().parse::<u8>().unwrap().into(),
            provolatile: column_val(row, "provolatile").unwrap().parse::<u8>().unwrap().into(),
            prorettype: column_val(row, "prorettype").unwrap().parse().unwrap(),
            prosrc: column_val(row, "prosrc").unwrap().to_string(),
            probin: column_val(row, "probin").unwrap().to_string(),
        });
        true
    })?;
    ret
}

pub struct FormType {
    pub id: Oid,
    pub len: i16,
    pub align: u8,
    pub isdefined: bool,
    pub input: Oid,
    pub output: Oid,
    pub modin: Oid,
    pub modout: Oid,
}

fn cond_get_type(state: &SessionState, cond: &str) -> anyhow::Result<Option<FormType>> {
    let mut ret = None;
    state
        .metaconn
        .iterate(format!("select * from kb_type where {}", cond), |row| {
            ret = Some(FormType {
                id: column_val(row, "oid").unwrap().parse().unwrap(),
                align: column_val(row, "typalign").unwrap().parse().unwrap(),
                input: column_val(row, "typinput").unwrap().parse().unwrap(),
                output: column_val(row, "typoutput").unwrap().parse().unwrap(),
                modin: column_val(row, "typmodin").unwrap().parse().unwrap(),
                modout: column_val(row, "typmodout").unwrap().parse().unwrap(),
                isdefined: column_val(row, "typisdefined")
                    .unwrap()
                    .parse::<i32>()
                    .unwrap()
                    == 1,
                len: column_val(row, "typlen").unwrap().parse::<i16>().unwrap(),
            });
            true
        })?;
    return Ok(ret);
}

fn get_type(state: &SessionState, oid: Oid) -> anyhow::Result<FormType> {
    let ret: anyhow::Result<FormType> = Err(kbanyhow!(
        ERRCODE_UNDEFINED_OBJECT,
        "lookup failed for type {}",
        oid
    ));
    let ftype = cond_get_type(state, &format!("oid = {}", oid))?;
    if let Some(ftype) = ftype {
        return Ok(ftype);
    } else {
        return ret;
    }
}

pub fn qualname_get_type(
    state: &SessionState,
    nsoid: Oid,
    typname: &str,
) -> anyhow::Result<FormType> {
    let ret: anyhow::Result<FormType> = Err(kbanyhow!(
        ERRCODE_UNDEFINED_OBJECT,
        "lookup failed for type {}",
        typname
    ));
    let ftype = cond_get_type(
        state,
        &format!("typname = '{}' and typnamespace = {}", typname, nsoid),
    )?;
    if let Some(ftype) = ftype {
        return Ok(ftype);
    } else {
        return ret;
    }
}

pub fn get_type_output_info(state: &SessionState, oid: Oid) -> anyhow::Result<(Oid, i16)> {
    let formtype = get_type(state, oid)?;
    if !formtype.isdefined {
        Err(kbanyhow!(
            ERRCODE_UNDEFINED_OBJECT,
            "type {} is only a shell",
            oid
        ))
    } else {
        Ok((formtype.output, formtype.len))
    }
}

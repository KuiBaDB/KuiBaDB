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
use crate::access::sv;
use crate::access::{TupleDesc, TypeDesc};
use crate::catalog::namespace::SessionExt;
use crate::catalog::{qualname_get_type, FormType};
use crate::kbbail;
use crate::parser::syn;
use crate::utility::Response;
use crate::utils::{persist, sync_dir};
use crate::utils::{ExecSQLOnDrop, SessionState};
use crate::xact::SessionExt as XACTSessionExt;
use anyhow::ensure;
use std::fs;

// LookupTypeNameExtended
fn get_type_desc(
    state: &mut SessionState,
    typnam: &syn::TypeName<'_>,
) -> anyhow::Result<Option<FormType>> {
    ensure!(typnam.typmods.is_empty(), "typemod is not support now");
    let (schema, typname) = state.deconstruct_qualname(&typnam.names)?;
    let formtype = if let Some(schema) = schema {
        let nsoid = state.get_namespace_oid(schema)?;
        qualname_get_type(state, nsoid, typname)?
    } else {
        state.typname_get_type(typname)?
    };
    return Ok(formtype);
}

// typenameType
fn typname_type(state: &mut SessionState, typnam: &syn::TypeName<'_>) -> anyhow::Result<TypeDesc> {
    let formtype = get_type_desc(state, typnam)?;
    if let Some(formtype) = formtype {
        if !formtype.isdefined {
            kbbail!(
                ERRCODE_UNDEFINED_OBJECT,
                "type {:?} is only a shell",
                typnam
            );
        }
        return Ok(TypeDesc {
            id: formtype.id,
            len: formtype.len,
            align: formtype.align,
            mode: -1,
        });
    }
    kbbail!(ERRCODE_UNDEFINED_OBJECT, "type {:?} does not exist", typnam);
}

// BuildDescForRelation
fn build_desc(
    state: &mut SessionState,
    table_elts: &Vec<syn::ColumnDef<'_>>,
) -> anyhow::Result<TupleDesc> {
    let mut ts = TupleDesc {
        desc: Vec::with_capacity(table_elts.len()),
    };
    for cf in table_elts {
        ts.desc.push(typname_type(state, &cf.typename)?);
    }
    return Ok(ts);
}

pub fn create_table(
    stmt: &syn::CreateTableStmt,
    state: &mut SessionState,
) -> anyhow::Result<Response> {
    state.prevent_in_transblock("CREATE TABLE")?;

    let nsoid = state.rv_get_create_ns(&stmt.relation)?;
    let tableoid = state.new_oid();
    let tupdesc = build_desc(state, &stmt.table_elts)?;
    let xid = state.get_xid()?;

    state.metaconn.execute("begin")?;
    let _rollback = ExecSQLOnDrop::new(&state.metaconn, "rollback");
    let relname: &str = &stmt.relation.relname;
    state.metaconn.execute(format!(
        "insert into kb_class values({}, '{}', {}, false, 114, {}, {})",
        tableoid,
        relname,
        nsoid,
        tupdesc.desc.len(),
        xid
    ))?;
    for attidx in 0..tupdesc.desc.len() {
        let attnum = attidx + 1;
        let typdesc = &tupdesc.desc[attidx];
        let attname: &str = &stmt.table_elts[attidx].colname;
        let sql = format!(
            "insert into kb_attribute values({}, '{}', {}, {}, {}, {}, {}, 0, 0)",
            tableoid, attname, typdesc.id, typdesc.len, typdesc.align, attnum, typdesc.mode
        );
        state.metaconn.execute(sql)?;
    }

    fs::create_dir(format!("base/{}/{}", state.reqdb, tableoid))?;
    sync_dir(format!("base/{}", state.reqdb))?;
    persist(
        sv::get_minafest_path(state.reqdb, tableoid),
        &sv::INIT_MANIFEST_DAT,
    )?;
    state.metaconn.execute("commit")?;
    std::mem::forget(_rollback);

    return Ok(Response {
        resp: None,
        tag: "CREATE TABLE",
    });
}

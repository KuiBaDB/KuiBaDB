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

use super::column_val;
use crate::catalog::{get_oper, get_opers, FormOperator};
use crate::guc;
use crate::parser::syn;
use crate::utils::SessionState;
use crate::{protocol, ErrCode};
use anyhow::{anyhow, Context};
use kuiba::{Oid, OptOid};
use std::sync::Arc;

#[derive(Default)]
pub struct SessionStateExt {
    search_path: Vec<Oid>,
}

// The extension added by catalog/namespace for SessionState
pub trait SessionExt {
    // DeconstructQualifiedName
    fn deconstruct_qualname<'a>(
        &self,
        names: &'a Vec<syn::StrVal>,
    ) -> anyhow::Result<(Option<&'a str>, &'a str)>;
    // QualifiedNameGetCreationNamespace
    fn qualname_get_create_ns<'a>(
        &self,
        names: &'a Vec<syn::StrVal>,
    ) -> anyhow::Result<(Oid, &'a str)>;

    fn get_namespace_oid(&self, nspname: &str) -> anyhow::Result<Oid>;

    // OpernameGetOprid
    fn opername_get_oprid(
        &mut self,
        names: &Vec<syn::StrVal>,
        oprleft: OptOid,
        oprright: Oid,
    ) -> anyhow::Result<FormOperator>;

    fn get_search_path(&mut self) -> &Vec<Oid>;

    fn lookup_explicit_namespace(&self, nspname: &str) -> anyhow::Result<Oid>;
}

impl SessionExt for SessionState {
    fn qualname_get_create_ns<'a>(
        &self,
        names: &'a Vec<syn::StrVal>,
    ) -> anyhow::Result<(Oid, &'a str)> {
        let (nspname, name) = self.deconstruct_qualname(names)?;
        let nspoid = match nspname {
            None => {
                return Err(anyhow!("no schema has been selected to create in"))
                    .context(ErrCode(protocol::ERRCODE_UNDEFINED_SCHEMA))
            }
            Some(nspname) => self.get_namespace_oid(nspname)?,
        };
        Ok((nspoid, name))
    }

    fn deconstruct_qualname<'a>(
        &self,
        names: &'a Vec<syn::StrVal>,
    ) -> anyhow::Result<(Option<&'a str>, &'a str)> {
        match names.len() {
            1 => Ok((None, &*names[0])),
            2 => Ok((Some(&*names[0]), &*names[1])),
            3 => {
                if self.db == &*names[0] {
                    Ok((Some(&*names[1]), &*names[2]))
                } else {
                    Err(anyhow!(
                        "cross-database references are not implemented: {:?}",
                        names
                    ))
                    .context(ErrCode(protocol::ERRCODE_FEATURE_NOT_SUPPORTED))
                }
            }
            _ => Err(anyhow!(
                "improper qualified name (too many dotted names): {:?}",
                names
            ))
            .context(ErrCode(protocol::ERRCODE_SYNTAX_ERROR)),
        }
    }

    fn get_namespace_oid(&self, nspname: &str) -> anyhow::Result<Oid> {
        let mut retdb: anyhow::Result<Oid> = Err(anyhow::anyhow!("not found"));
        self.metaconn.iterate(
            format!("select oid from kb_namespace where nspname = '{}'", nspname),
            |row| {
                retdb = Ok(column_val(row, "oid").unwrap().parse().unwrap());
                true
            },
        )?;
        retdb
    }

    // recomputeNamespacePath
    fn get_search_path(&mut self) -> &Vec<Oid> {
        if self.gucstate.base_search_path_valid {
            return &self.nsstate.search_path;
        }
        self.nsstate.search_path.clear();
        for nspname in guc::get_str(&self.gucstate, guc::SEARCH_PATH).split(',') {
            if let Ok(oid) = self.get_namespace_oid(nspname) {
                self.nsstate.search_path.push(oid);
            }
        }
        Arc::make_mut(&mut self.gucstate).base_search_path_valid = true;
        &self.nsstate.search_path
    }

    fn lookup_explicit_namespace(&self, nspname: &str) -> anyhow::Result<Oid> {
        self.get_namespace_oid(nspname)
    }

    fn opername_get_oprid(
        &mut self,
        names: &Vec<syn::StrVal>,
        oprleft: OptOid,
        oprright: Oid,
    ) -> anyhow::Result<FormOperator> {
        let (schemaname, opername) = self.deconstruct_qualname(names)?;
        if let Some(schemaname) = schemaname {
            let nspoid = self.lookup_explicit_namespace(schemaname)?;
            return get_oper(self, opername, oprleft, oprright, nspoid);
        }
        let opers = get_opers(self, opername, oprleft, oprright)?;
        for &nspoid in self.get_search_path() {
            for oper in &opers {
                if oper.oprnamespace == nspoid {
                    return Ok(*oper);
                }
            }
        }
        let oprleft: u32 = oprleft.into();
        Err(anyhow!(
            "operator does not exist. name={} left={} right={}",
            opername,
            oprleft,
            oprright
        ))
        .context(ErrCode(protocol::ERRCODE_UNDEFINED_FUNCTION))
    }
}

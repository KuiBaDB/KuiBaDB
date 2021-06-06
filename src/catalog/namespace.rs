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
use crate::access::lmgr::LockMode;
use crate::access::lmgr::SessionExt as LMGRSessionExt;
use crate::catalog::{self, get_oper, get_opers, FormOperator};
use crate::catalog::{qualname_get_type, FormType};
use crate::guc;
use crate::parser::syn;
use crate::utils::SessionState;
use crate::{kbanyhow, kbbail, Oid, OptOid};
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

    // RangeVarGetCreationNamespace
    fn rv_get_create_ns(&mut self, rv: &syn::RangeVar<'_>) -> anyhow::Result<Oid>;

    // RangeVarGetAndCheckCreationNamespace
    fn rv_get_and_chk_create_ns(&mut self, rv: &syn::RangeVar<'_>) -> anyhow::Result<Oid>;

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

    // TypenameGetTypidExtended
    fn typname_get_type(&mut self, typname: &str) -> anyhow::Result<Option<FormType>>;

    // RangeVarGetRelidExtended
    fn rv_get_oid(&mut self, rv: &syn::RangeVar<'_>, mode: LockMode) -> anyhow::Result<Oid>;

    // RelnameGetRelid
    fn relname_get_oid(&mut self, n: &str) -> anyhow::Result<Option<Oid>>;
}

fn oid_in_used(sess: &SessionState, oid: Oid, catalog: &str) -> anyhow::Result<bool> {
    let mut isns = false;
    sess.metaconn.iterate(
        format!("select oid from {} where oid = {}", catalog, oid),
        |_row| {
            isns = true;
            true
        },
    )?;
    return Ok(isns);
}

fn oid_is_ns(sess: &SessionState, nsoid: Oid) -> anyhow::Result<bool> {
    return oid_in_used(sess, nsoid, "kb_namespace");
}

impl SessionExt for SessionState {
    fn qualname_get_create_ns<'a>(
        &self,
        names: &'a Vec<syn::StrVal>,
    ) -> anyhow::Result<(Oid, &'a str)> {
        let (nspname, name) = self.deconstruct_qualname(names)?;
        let nspoid = match nspname {
            None => {
                kbbail!(
                    ERRCODE_UNDEFINED_SCHEMA,
                    "no schema has been selected to create in"
                );
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
                    Err(kbanyhow!(
                        ERRCODE_FEATURE_NOT_SUPPORTED,
                        "cross-database references are not implemented: {:?}",
                        names
                    ))
                }
            }
            _ => Err(kbanyhow!(
                ERRCODE_SYNTAX_ERROR,
                "improper qualified name (too many dotted names): {:?}",
                names
            )),
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
        for nspname in guc::get_str(&self.gucstate, guc::SearchPath).split(',') {
            if let Ok(oid) = self.get_namespace_oid(nspname) {
                self.nsstate.search_path.push(oid);
            }
        }
        Arc::make_mut(&mut self.gucstate).base_search_path_valid = true;
        return &self.nsstate.search_path;
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
        kbbail!(
            ERRCODE_UNDEFINED_FUNCTION,
            "operator does not exist. name={} left={} right={}",
            opername,
            oprleft,
            oprright
        );
    }

    fn rv_get_create_ns(&mut self, rv: &syn::RangeVar<'_>) -> anyhow::Result<Oid> {
        if let Some(ref sn) = rv.schemaname {
            return self.get_namespace_oid(sn);
        }
        let oids = self.get_search_path();
        if let Some(&oid) = oids.first() {
            return Ok(oid);
        } else {
            kbbail!(
                ERRCODE_UNDEFINED_SCHEMA,
                "no schema has been selected to create in"
            );
        }
    }

    fn rv_get_and_chk_create_ns(&mut self, rv: &syn::RangeVar<'_>) -> anyhow::Result<Oid> {
        let nsoid = self.rv_get_create_ns(rv)?;
        self.lock_ns(nsoid, LockMode::AccessShare);
        // Oid is never reused! so if the oid is still a namespace, it means that
        // nsoid got by rv_get_create_ns() is still valid.
        if !oid_is_ns(self, nsoid)? {
            kbbail!(
                ERRCODE_UNDEFINED_SCHEMA,
                "no schema has been selected to create in"
            );
        }
        return Ok(nsoid);
    }

    fn typname_get_type(&mut self, typname: &str) -> anyhow::Result<Option<FormType>> {
        self.get_search_path();
        for &nsoid in &self.nsstate.search_path {
            if let Ok(t) = qualname_get_type(self, nsoid, typname) {
                return Ok(t);
            }
        }
        return Ok(None);
    }

    fn relname_get_oid(&mut self, n: &str) -> anyhow::Result<Option<Oid>> {
        self.get_search_path();
        for &nsoid in &self.nsstate.search_path {
            let ret = catalog::relname_get_relid(self, n, nsoid)?;
            if let Some(oid) = ret {
                return Ok(Some(oid));
            }
        }
        return Ok(None);
    }

    fn rv_get_oid(&mut self, rv: &syn::RangeVar<'_>, mode: LockMode) -> anyhow::Result<Oid> {
        let reloid = if let Some(ref schema) = rv.schemaname {
            let nsoid = self.get_namespace_oid(schema)?;
            catalog::relname_get_relid(self, &rv.relname, nsoid)?
        } else {
            self.relname_get_oid(&rv.relname)?
        };
        if let Some(reloid) = reloid {
            if mode == LockMode::NoLock {
                return Ok(reloid);
            }
            self.lock_rel(reloid, mode);
            if oid_in_used(self, reloid, "kb_class")? {
                // Oid is never reused! so if the oid is still in kb_class, it means that
                // nsoid got by relname_get_oid() is still valid.
                return Ok(reloid);
            } else {
                self.unlock_rel(reloid, mode);
            }
        }
        kbbail!(ERRCODE_UNDEFINED_TABLE, "relation {:?} does not exist", rv);
    }
}

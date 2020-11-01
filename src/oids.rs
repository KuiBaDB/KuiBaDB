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
#[repr(u32)]
#[allow(non_camel_case_types)]
pub enum Oid {
    Template0Db = 1,
    KuiBaDb = 2,
    PG_CATALOG_NAMESPACE = 11,
    TypeRelationId = 1247,
    AttributeRelationId = 1249,
    ProcedureRelationId = 1255,
    RelationRelationId = 1259,
    DatabaseRelationId = 1262,
    PG_PUBLIC_NAMESPACE = 2200,
    NamespaceRelationId = 2615,
    OperatorRelationId = 2617,
    MAX_OID = 16384, // The oid of system catalogs should be less than MAX_OID.
}

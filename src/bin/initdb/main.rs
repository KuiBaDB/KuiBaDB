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

#![allow(dead_code)]
use clap::{App, Arg};
use kuiba::*;
use log;
use sqlite;
use std::vec::Vec;

struct Attr {
    name: &'static str,
    desc: &'static str,
    sqlite_type: &'static str,
}

fn attrs_to_ddl(attrs: &[Attr]) -> String {
    let mut attrvec = Vec::new();
    for attr in attrs {
        attrvec.push(format!("{} {}", attr.name, attr.sqlite_type));
    }
    attrvec.join(", ")
}

const KB_DATABASE_ATTRS: [Attr; 5] = [
    Attr {
        name: "oid",
        desc: "u32",
        sqlite_type: "int not null unique",
    },
    Attr {
        name: "datname",
        desc: "varchar(127)",
        sqlite_type: "varchar(127) not null unique",
    },
    Attr {
        name: "datistemplate",
        desc: "bool",
        sqlite_type: "int not null",
    },
    Attr {
        name: "datallowconn",
        desc: "bool",
        sqlite_type: "int not null",
    },
    Attr {
        name: "datfrozenxid",
        desc: "u64",
        sqlite_type: "int not null",
    },
];

const KB_CLASS_ATTRS: [Attr; 7] = [
    Attr {
        name: "oid",
        desc: "u32",
        sqlite_type: "int",
    },
    Attr {
        name: "relname",
        desc: "",
        sqlite_type: "varchar(127)",
    },
    Attr {
        name: "relnamespace",
        desc: "oid type",
        sqlite_type: "int",
    },
    Attr {
        name: "relisshared",
        desc: "bool",
        sqlite_type: "int",
    },
    Attr {
        name: "relkind",
        desc: "int1",
        sqlite_type: "int",
    },
    Attr {
        name: "relnattrs",
        desc: "int2",
        sqlite_type: "int",
    },
    Attr {
        name: "relfrozenxid",
        desc: "u64",
        sqlite_type: "int",
    },
];

const KB_OPERATOR_ATTRS: [Attr; 8] = [
    Attr {
        name: "oid",
        desc: "u32",
        sqlite_type: "int",
    },
    Attr {
        name: "oprname",
        desc: "",
        sqlite_type: "varchar(127)",
    },
    Attr {
        name: "oprnamespace",
        desc: "u32 oid",
        sqlite_type: "int",
    },
    Attr {
        name: "oprkind",
        desc: "int1",
        sqlite_type: "int",
    },
    Attr {
        name: "oprleft",
        desc: "oid",
        sqlite_type: "int",
    },
    Attr {
        name: "oprright",
        desc: "oid",
        sqlite_type: "int",
    },
    Attr {
        name: "oprresult",
        desc: "oid",
        sqlite_type: "int",
    },
    Attr {
        name: "oprcode",
        desc: "oid",
        sqlite_type: "int",
    },
];

const KB_ATTRIBUTE_ATTRS: [Attr; 8] = [
    Attr {
        name: "attrelid",
        desc: "oid",
        sqlite_type: "int",
    },
    Attr {
        name: "attname",
        desc: "",
        sqlite_type: "varchar(127)",
    },
    Attr {
        name: "atttypid",
        desc: "oid",
        sqlite_type: "int",
    },
    Attr {
        name: "attlen",
        desc: "int2",
        sqlite_type: "int",
    },
    Attr {
        name: "attnum",
        desc: "int2",
        sqlite_type: "int",
    },
    Attr {
        name: "atttypmod",
        desc: "int4",
        sqlite_type: "int",
    },
    Attr {
        name: "attnotnull",
        desc: "bool",
        sqlite_type: "int",
    },
    Attr {
        name: "attisdropped",
        desc: "bool",
        sqlite_type: "int",
    },
];

const KB_NAMESPACE_ATTRS: [Attr; 2] = [
    Attr {
        name: "oid",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "nspname",
        desc: "",
        sqlite_type: "varchar(127)",
    },
];

const KB_PROC_ATTRS: [Attr; 10] = [
    Attr {
        name: "oid",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "proname",
        desc: "",
        sqlite_type: "varchar(127)",
    },
    Attr {
        name: "pronamespace",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "prokind",
        desc: "int1",
        sqlite_type: "int",
    },
    Attr {
        name: "provolatile",
        desc: "int1",
        sqlite_type: "int",
    },
    Attr {
        name: "pronargs",
        desc: "",
        sqlite_type: "int2",
    },
    Attr {
        name: "prorettype",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "proargtypes",
        desc: "oid,oid,oid",
        sqlite_type: "varchar(127)",
    },
    Attr {
        name: "prosrc",
        desc: "",
        sqlite_type: "varchar(127)",
    },
    Attr {
        name: "probin",
        desc: "",
        sqlite_type: "varchar(127)",
    },
];

const KB_TYPE_ATTRS: [Attr; 9] = [
    Attr {
        name: "oid",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "typname",
        desc: "",
        sqlite_type: "varchar(127)",
    },
    Attr {
        name: "typnamespace",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "typlen",
        desc: "int2",
        sqlite_type: "int",
    },
    Attr {
        name: "typisdefined",
        desc: "bool",
        sqlite_type: "int",
    },
    Attr {
        name: "typinput",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "typoutput",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "typmodin",
        desc: "",
        sqlite_type: "int",
    },
    Attr {
        name: "typmodout",
        desc: "",
        sqlite_type: "int",
    },
];

// global
fn create_global_metadata() {
    std::fs::create_dir_all("global").unwrap();
    let conn = sqlite::open("global/meta.db").unwrap();
    conn.execute(format!(
        "
    create table kb_database({});
    insert into kb_database values({}, 'template0', 1, 0, 0);
    insert into kb_database values({}, 'kuiba', 0, 1, 0);
    ",
        attrs_to_ddl(&KB_DATABASE_ATTRS),
        Template0Db as u32,
        KuiBaDb as u32
    ))
    .unwrap();
}

// base
fn create_template0_metadata() {
    let template0dir = format!("base/{}", Template0Db as u32);
    std::fs::create_dir_all(&template0dir).unwrap();
    let conn = sqlite::open(format!("{}/meta.db", &template0dir)).unwrap();

    conn.execute(format!(
        "
    create table kb_namespace({});
    insert into kb_namespace values({}, 'pg_catalog');
    insert into kb_namespace values({}, 'public');
    ",
        attrs_to_ddl(&KB_NAMESPACE_ATTRS),
        PG_CATALOG_NAMESPACE as u32,
        PG_PUBLIC_NAMESPACE as u32
    ))
    .unwrap();

    conn.execute(format!(
        "
    create table kb_class({});
    insert into kb_class values({}, 'kb_class', {}, 0, 114, {}, 0);
    insert into kb_class values({}, 'kb_attribute', {}, 0, 114, {}, 0);
    insert into kb_class values({}, 'kb_operator', {}, 0, 114, {}, 0);
    insert into kb_class values({}, 'kb_database', {}, 1, 114, {}, 0);
    insert into kb_class values({}, 'kb_namespace', {}, 0, 114, {}, 0);
    insert into kb_class values({}, 'kb_proc', {}, 0, 114, {}, 0);
    insert into kb_class values({}, 'kb_type', {}, 0, 114, {}, 0);
    ",
        attrs_to_ddl(&KB_CLASS_ATTRS),
        RelationRelationId as u32,
        PG_CATALOG_NAMESPACE as u32,
        KB_CLASS_ATTRS.len(),
        AttributeRelationId as u32,
        PG_CATALOG_NAMESPACE as u32,
        KB_ATTRIBUTE_ATTRS.len(),
        OperatorRelationId as u32,
        PG_CATALOG_NAMESPACE as u32,
        KB_OPERATOR_ATTRS.len(),
        DatabaseRelationId as u32,
        PG_CATALOG_NAMESPACE as u32,
        KB_DATABASE_ATTRS.len(),
        NamespaceRelationId as u32,
        PG_CATALOG_NAMESPACE as u32,
        KB_NAMESPACE_ATTRS.len(),
        ProcedureRelationId as u32,
        PG_CATALOG_NAMESPACE as u32,
        KB_PROC_ATTRS.len(),
        TypeRelationId as u32,
        PG_CATALOG_NAMESPACE as u32,
        KB_TYPE_ATTRS.len(),
    ))
    .unwrap();
}

fn create_kuiba_metadata() {
    let datadir = format!("base/{}", KuiBaDb as u32);
    let template0dir = format!("base/{}", Template0Db as u32);
    std::fs::create_dir_all(&datadir).unwrap();
    std::fs::copy(
        format!("{}/meta.db", &template0dir),
        format!("{}/meta.db", datadir),
    )
    .unwrap();
}

fn main() {
    init_log();
    let cmdline = App::new("initdb initializes a KuiBa database cluster.")
        .version(kuiba::KB_VERSTR)
        .author("盏一 <w@hidva.com>")
        .about("KuiBa Database is another Postgresql written in Rust")
        .arg(
            Arg::with_name("datadir")
                .help("location for this database cluster")
                .index(1)
                .required(true),
        )
        .get_matches();
    let datadir = cmdline.value_of("datadir").unwrap();
    std::fs::create_dir_all(datadir).unwrap();
    std::env::set_current_dir(datadir).unwrap();
    std::fs::write("KB_VERSION", format!("{}\n", kuiba::KB_MAJOR)).unwrap();
    std::fs::write("kuiba.conf", format!("# define your GUC here.\n")).unwrap();
    std::fs::create_dir_all("pg_wal").unwrap();
    std::fs::create_dir_all("pg_xact").unwrap();
    create_global_metadata();
    create_template0_metadata();
    create_kuiba_metadata();
    log::info!("initdb success");
}

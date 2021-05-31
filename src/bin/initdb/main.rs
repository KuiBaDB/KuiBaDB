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

// we **do** need a better way to initdb!!!

use clap::{App, Arg};
use kuiba::access::wal;
use kuiba::utils::{KBSystemTime, Xid};
use kuiba::*;
use log;
use sqlite;
use std::vec::Vec;

#[cfg(target_os = "linux")]
fn sync() {
    use nix::unistd::sync as _sync;
    _sync()
}

#[cfg(target_os = "macos")]
fn sync() {
    println!("sync! sync! sync!")
}

struct Attr {
    name: &'static str,
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
        // "u32",
        sqlite_type: "int not null unique",
    },
    Attr {
        name: "datname",
        // "varchar(127)",
        sqlite_type: "varchar(127) not null unique",
    },
    Attr {
        name: "datistemplate",
        // "bool",
        sqlite_type: "int not null",
    },
    Attr {
        name: "datallowconn",
        // "bool",
        sqlite_type: "int not null",
    },
    Attr {
        name: "datfrozenxid",
        // "u64",
        sqlite_type: "int not null",
    },
];

const KB_CLASS_ATTRS: [Attr; 7] = [
    Attr {
        name: "oid",
        // "u32",
        sqlite_type: "int not null unique",
    },
    Attr {
        name: "relname",
        // "",
        sqlite_type: "varchar(127) not null",
    },
    Attr {
        name: "relnamespace",
        // "oid type",
        sqlite_type: "int not null",
    },
    Attr {
        name: "relisshared",
        // "bool",
        sqlite_type: "int not null",
    },
    Attr {
        name: "relkind",
        // "int1",
        sqlite_type: "int not null",
    },
    Attr {
        name: "relnattrs",
        // "int2",
        sqlite_type: "int not null",
    },
    Attr {
        name: "relfrozenxid",
        // "u64, null is 0",
        sqlite_type: "int not null",
    },
];

const KB_OPERATOR_ATTRS: [Attr; 7] = [
    Attr {
        name: "oid",
        // "u32",
        sqlite_type: "int not null unique",
    },
    Attr {
        name: "oprname",
        // "",
        sqlite_type: "varchar(127) not null ",
    },
    Attr {
        name: "oprnamespace",
        // "u32 oid",
        sqlite_type: "int not null ",
    },
    Attr {
        name: "oprleft",
        // "oid",
        sqlite_type: "int not null ",
    },
    Attr {
        name: "oprright",
        // "oid",
        sqlite_type: "int not null ",
    },
    Attr {
        name: "oprresult",
        // "oid",
        sqlite_type: "int not null ",
    },
    Attr {
        name: "oprcode",
        // "oid",
        sqlite_type: "int not null ",
    },
];

const KB_ATTRIBUTE_ATTRS: [Attr; 9] = [
    Attr {
        name: "attrelid",
        // "oid",
        sqlite_type: "int not null",
    },
    Attr {
        name: "attname",
        // "",
        sqlite_type: "varchar(127) not null",
    },
    Attr {
        name: "atttypid",
        // "oid",
        sqlite_type: "int not null",
    },
    Attr {
        name: "attlen",
        // "int2",
        sqlite_type: "int not null",
    },
    Attr {
        name: "attalign",
        // "int1",
        sqlite_type: "int not null",
    },
    Attr {
        name: "attnum",
        // "int2",
        sqlite_type: "int not null",
    },
    Attr {
        name: "atttypmod",
        // "signed int4",
        sqlite_type: "int not null",
    },
    Attr {
        name: "attnotnull",
        // "bool",
        sqlite_type: "int not null",
    },
    Attr {
        name: "attisdropped",
        // "bool",
        sqlite_type: "int not null",
    },
];

const KB_NAMESPACE_ATTRS: [Attr; 2] = [
    Attr {
        name: "oid",
        // "",
        sqlite_type: "int not null unique",
    },
    Attr {
        name: "nspname",
        // "",
        sqlite_type: "varchar(127) not null unique",
    },
];

const KB_PROC_ATTRS: [Attr; 10] = [
    Attr {
        name: "oid",
        // "",
        sqlite_type: "int not null unique",
    },
    Attr {
        name: "proname",
        // "",
        sqlite_type: "varchar(127) not null",
    },
    Attr {
        name: "pronamespace",
        // "",
        sqlite_type: "int not null",
    },
    Attr {
        name: "prokind",
        // "int1",
        sqlite_type: "int not null",
    },
    Attr {
        name: "provolatile",
        // "int1",
        sqlite_type: "int not null",
    },
    Attr {
        name: "pronargs",
        // "",
        sqlite_type: "int2 not null",
    },
    Attr {
        name: "prorettype",
        // "",
        sqlite_type: "int not null",
    },
    Attr {
        name: "proargtypes",
        // "oid oid oid",
        sqlite_type: "varchar(127) not null",
    },
    Attr {
        name: "prosrc",
        // "",
        sqlite_type: "varchar(127) not null",
    },
    Attr {
        name: "probin",
        // "empty is null",
        sqlite_type: "varchar(127) not null",
    },
];

const KB_TYPE_ATTRS: [Attr; 10] = [
    Attr {
        name: "oid",
        // "",
        sqlite_type: "int not null unique",
    },
    Attr {
        name: "typname",
        // "",
        sqlite_type: "varchar(127) not null",
    },
    Attr {
        name: "typnamespace",
        // "",
        sqlite_type: "int not null",
    },
    Attr {
        name: "typlen",
        // "int2",
        sqlite_type: "int not null",
    },
    Attr {
        name: "typalign",
        // "int1",
        sqlite_type: "int not null",
    },
    Attr {
        name: "typisdefined",
        // "bool",
        sqlite_type: "int not null",
    },
    Attr {
        name: "typinput",
        // "",
        sqlite_type: "int not null",
    },
    Attr {
        name: "typoutput",
        // "",
        sqlite_type: "int not null",
    },
    Attr {
        name: "typmodin",
        // "",
        sqlite_type: "int not null",
    },
    Attr {
        name: "typmodout",
        // "",
        sqlite_type: "int not null",
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
    insert into kb_namespace values({}, 'kb_catalog');
    insert into kb_namespace values({}, 'public');
    ",
        attrs_to_ddl(&KB_NAMESPACE_ATTRS),
        KBCatalogNamespace as u32,
        KBPublicNamespace as u32
    ))
    .unwrap();

    conn.execute(format!(
        "
    create table kb_class({}, unique (relname, relnamespace));
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
        KBCatalogNamespace as u32,
        KB_CLASS_ATTRS.len(),
        AttributeRelationId as u32,
        KBCatalogNamespace as u32,
        KB_ATTRIBUTE_ATTRS.len(),
        OperatorRelationId as u32,
        KBCatalogNamespace as u32,
        KB_OPERATOR_ATTRS.len(),
        DatabaseRelationId as u32,
        KBCatalogNamespace as u32,
        KB_DATABASE_ATTRS.len(),
        NamespaceRelationId as u32,
        KBCatalogNamespace as u32,
        KB_NAMESPACE_ATTRS.len(),
        ProcedureRelationId as u32,
        KBCatalogNamespace as u32,
        KB_PROC_ATTRS.len(),
        TypeRelationId as u32,
        KBCatalogNamespace as u32,
        KB_TYPE_ATTRS.len(),
    ))
    .unwrap();

    conn.execute(format!(
        "create table kb_type({}, unique (typname, typnamespace));",
        attrs_to_ddl(&KB_TYPE_ATTRS)
    ))
    .unwrap();

    conn.execute(format!(
        "
    insert into kb_type values
    ({}, 'bool', {}, 1, 1, 1, {}, {}, 0, 0),
    ({}, 'bytea', {}, -1, 1, 1, {}, {}, 0, 0),
    ({}, 'int8', {}, 8, 8, 1, {}, {}, 0, 0),
    ({}, 'int2', {}, 2, 2, 1, {}, {}, 0, 0),
    ({}, 'int4', {}, 4, 4, 1, {}, {}, 0, 0),
    ({}, 'float4', {}, 4, 4, 1, {}, {}, 0, 0),
    ({}, 'float8', {}, 8, 8, 1, {}, {}, 0, 0),
    ({}, 'varchar', {}, -1, 1, 1, {}, {}, 0, 0);
    ",
        BOOLOID as u32,
        KBCatalogNamespace as u32,
        BoolInProc as u32,
        BoolOutProc as u32,
        BYTEAOID as u32,
        KBCatalogNamespace as u32,
        ByteaInProc as u32,
        ByteaOutProc as u32,
        INT8OID as u32,
        KBCatalogNamespace as u32,
        Int8InProc as u32,
        Int8OutProc as u32,
        INT2OID as u32,
        KBCatalogNamespace as u32,
        Int2InProc as u32,
        Int2OutProc as u32,
        INT4OID as u32,
        KBCatalogNamespace as u32,
        Int4InProc as u32,
        Int4OutProc as u32,
        FLOAT4OID as u32,
        KBCatalogNamespace as u32,
        Float4InProc as u32,
        Float4OutProc as u32,
        FLOAT8OID as u32,
        KBCatalogNamespace as u32,
        Float8InProc as u32,
        Float8OutProc as u32,
        VARCHAROID as u32,
        KBCatalogNamespace as u32,
        VarcharInProc as u32,
        VarcharOutProc as u32,
    ))
    .unwrap();

    conn.execute(format!(
        "create table kb_operator({}, unique (oprname, oprleft, oprright, oprnamespace));",
        attrs_to_ddl(&KB_OPERATOR_ATTRS)
    ))
    .unwrap();
    // psql -d tmp -A -t '-F,' -c
    // select '(' || oid::text, '''' || oprname || '''', oprnamespace, oprleft, oprright, oprresult, oprcode::oid::text || '),'
    // from pg_operator
    // where ((oprleft in (16,17,20,21,23,700,701,1043) or oprleft::int = 0) and (oprright in (16,17,20,21,23,700,701,1043) or oprright::int  = 0));
    conn.execute(format!(
        "insert into kb_operator values
        (15,'=',11,23,20,16,852),
        (36,'<>',11,23,20,16,853),
        (37,'<',11,23,20,16,854),
        (76,'>',11,23,20,16,855),
        (80,'<=',11,23,20,16,856),
        (82,'>=',11,23,20,16,857),
        (58,'<',11,16,16,16,56),
        (59,'>',11,16,16,16,57),
        (85,'<>',11,16,16,16,84),
        (91,'=',11,16,16,16,60),
        (1694,'<=',11,16,16,16,1691),
        (1695,'>=',11,16,16,16,1692),
        (94,'=',11,21,21,16,63),
        (95,'<',11,21,21,16,64),
        (96,'=',11,23,23,16,65),
        (97,'<',11,23,23,16,66),
        (388,'!',11,20,0,1700,111),
        (389,'!!',11,0,20,1700,111),
        (410,'=',11,20,20,16,467),
        (411,'<>',11,20,20,16,468),
        (412,'<',11,20,20,16,469),
        (413,'>',11,20,20,16,470),
        (414,'<=',11,20,20,16,471),
        (415,'>=',11,20,20,16,472),
        (416,'=',11,20,23,16,474),
        (417,'<>',11,20,23,16,475),
        (418,'<',11,20,23,16,476),
        (419,'>',11,20,23,16,477),
        (420,'<=',11,20,23,16,478),
        (430,'>=',11,20,23,16,479),
        (439,'%',11,20,20,20,945),
        (473,'@',11,0,20,20,1230),
        (484,'-',11,0,20,20,462),
        (514,'*',11,23,23,23,141),
        (518,'<>',11,23,23,16,144),
        (519,'<>',11,21,21,16,145),
        (520,'>',11,21,21,16,146),
        (521,'>',11,23,23,16,147),
        (522,'<=',11,21,21,16,148),
        (523,'<=',11,23,23,16,149),
        (524,'>=',11,21,21,16,151),
        (525,'>=',11,23,23,16,150),
        (526,'*',11,21,21,21,152),
        (527,'/',11,21,21,21,153),
        (528,'/',11,23,23,23,154),
        (529,'%',11,21,21,21,155),
        (530,'%',11,23,23,23,156),
        (532,'=',11,21,23,16,158),
        (533,'=',11,23,21,16,159),
        (534,'<',11,21,23,16,160),
        (535,'<',11,23,21,16,161),
        (536,'>',11,21,23,16,162),
        (537,'>',11,23,21,16,163),
        (538,'<>',11,21,23,16,164),
        (539,'<>',11,23,21,16,165),
        (540,'<=',11,21,23,16,166),
        (541,'<=',11,23,21,16,167),
        (542,'>=',11,21,23,16,168),
        (543,'>=',11,23,21,16,169),
        (544,'*',11,21,23,23,170),
        (545,'*',11,23,21,23,171),
        (546,'/',11,21,23,23,172),
        (547,'/',11,23,21,23,173),
        (550,'+',11,21,21,21,176),
        (551,'+',11,23,23,23,177),
        (552,'+',11,21,23,23,178),
        (553,'+',11,23,21,23,179),
        (554,'-',11,21,21,21,180),
        (555,'-',11,23,23,23,181),
        (556,'-',11,21,23,23,182),
        (557,'-',11,23,21,23,183),
        (558,'-',11,0,23,23,212),
        (559,'-',11,0,21,21,213),
        (584,'-',11,0,700,700,206),
        (585,'-',11,0,701,701,220),
        (586,'+',11,700,700,700,204),
        (587,'-',11,700,700,700,205),
        (588,'/',11,700,700,700,203),
        (589,'*',11,700,700,700,202),
        (590,'@',11,0,700,700,207),
        (591,'+',11,701,701,701,218),
        (592,'-',11,701,701,701,219),
        (593,'/',11,701,701,701,217),
        (594,'*',11,701,701,701,216),
        (595,'@',11,0,701,701,221),
        (596,'|/',11,0,701,701,230),
        (597,'||/',11,0,701,701,231),
        (620,'=',11,700,700,16,287),
        (621,'<>',11,700,700,16,288),
        (622,'<',11,700,700,16,289),
        (623,'>',11,700,700,16,291),
        (624,'<=',11,700,700,16,290),
        (625,'>=',11,700,700,16,292),
        (670,'=',11,701,701,16,293),
        (671,'<>',11,701,701,16,294),
        (672,'<',11,701,701,16,295),
        (673,'<=',11,701,701,16,296),
        (674,'>',11,701,701,16,297),
        (675,'>=',11,701,701,16,298),
        (682,'@',11,0,21,21,1253),
        (684,'+',11,20,20,20,463),
        (685,'-',11,20,20,20,464),
        (686,'*',11,20,20,20,465),
        (687,'/',11,20,20,20,466),
        (688,'+',11,20,23,20,1274),
        (689,'-',11,20,23,20,1275),
        (690,'*',11,20,23,20,1276),
        (691,'/',11,20,23,20,1277),
        (692,'+',11,23,20,20,1278),
        (693,'-',11,23,20,20,1279),
        (694,'*',11,23,20,20,1280),
        (695,'/',11,23,20,20,1281),
        (818,'+',11,20,21,20,837),
        (819,'-',11,20,21,20,838),
        (820,'*',11,20,21,20,839),
        (821,'/',11,20,21,20,840),
        (822,'+',11,21,20,20,841),
        (823,'-',11,21,20,20,942),
        (824,'*',11,21,20,20,943),
        (825,'/',11,21,20,20,948),
        (773,'@',11,0,23,23,1251),
        (965,'^',11,701,701,701,232),
        (1116,'+',11,700,701,701,281),
        (1117,'-',11,700,701,701,282),
        (1118,'/',11,700,701,701,280),
        (1119,'*',11,700,701,701,279),
        (1120,'=',11,700,701,16,299),
        (1121,'<>',11,700,701,16,300),
        (1122,'<',11,700,701,16,301),
        (1123,'>',11,700,701,16,303),
        (1124,'<=',11,700,701,16,302),
        (1125,'>=',11,700,701,16,304),
        (1126,'+',11,701,700,701,285),
        (1127,'-',11,701,700,701,286),
        (1128,'/',11,701,700,701,284),
        (1129,'*',11,701,700,701,283),
        (1130,'=',11,701,700,16,305),
        (1131,'<>',11,701,700,16,306),
        (1132,'<',11,701,700,16,307),
        (1133,'>',11,701,700,16,309),
        (1134,'<=',11,701,700,16,308),
        (1135,'>=',11,701,700,16,310),
        (1862,'=',11,21,20,16,1850),
        (1863,'<>',11,21,20,16,1851),
        (1864,'<',11,21,20,16,1852),
        (1865,'>',11,21,20,16,1853),
        (1866,'<=',11,21,20,16,1854),
        (1867,'>=',11,21,20,16,1855),
        (1868,'=',11,20,21,16,1856),
        (1869,'<>',11,20,21,16,1857),
        (1870,'<',11,20,21,16,1858),
        (1871,'>',11,20,21,16,1859),
        (1872,'<=',11,20,21,16,1860),
        (1873,'>=',11,20,21,16,1861),
        (1874,'&',11,21,21,21,1892),
        (1875,'|',11,21,21,21,1893),
        (1876,'#',11,21,21,21,1894),
        (1877,'~',11,0,21,21,1895),
        (1878,'<<',11,21,23,21,1896),
        (1879,'>>',11,21,23,21,1897),
        (1880,'&',11,23,23,23,1898),
        (1881,'|',11,23,23,23,1899),
        (1882,'#',11,23,23,23,1900),
        (1883,'~',11,0,23,23,1901),
        (1884,'<<',11,23,23,23,1902),
        (1885,'>>',11,23,23,23,1903),
        (1886,'&',11,20,20,20,1904),
        (1887,'|',11,20,20,20,1905),
        (1888,'#',11,20,20,20,1906),
        (1889,'~',11,0,20,20,1907),
        (1890,'<<',11,20,23,20,1908),
        (1891,'>>',11,20,23,20,1909),
        (1916,'+',11,0,20,20,1910),
        (1917,'+',11,0,21,21,1911),
        (1918,'+',11,0,23,23,1912),
        (1919,'+',11,0,700,700,1913),
        (1920,'+',11,0,701,701,1914),
        (1955,'=',11,17,17,16,1948),
        (1956,'<>',11,17,17,16,1953),
        (1957,'<',11,17,17,16,1949),
        (1958,'<=',11,17,17,16,1950),
        (1959,'>',11,17,17,16,1951),
        (1960,'>=',11,17,17,16,1952),
        (2016,'~~',11,17,17,16,2005),
        (2017,'!~~',11,17,17,16,2006),
        (2018,'||',11,17,17,17,2011);
    "
    ))
    .unwrap();

    conn.execute(format!(
        "create table kb_proc({}, unique (proname, proargtypes, pronamespace));",
        attrs_to_ddl(&KB_PROC_ATTRS)
    ))
    .unwrap();
    //   SELECT '(' || oid::text, ''''||proname||'''', pronamespace, prokind::int, provolatile::int, pronargs, prorettype, ''''||proargtypes::text||'''', ''''||prosrc||'''', ''''||coalesce(probin,'')||'''),'
    //   FROM pg_proc
    //   WHERE oid IN (
    //       (SELECT oprcode
    //       FROM pg_operator
    //       WHERE ((oprleft IN (16,17,20,21,23,700,701,1043)
    //               OR oprleft::int = 0)
    //               AND (oprright IN (16,17,20,21,23,700,701,1043)
    //               OR oprright::int = 0)))
    //       UNION
    //       all
    //           (SELECT typinput
    //           FROM pg_type
    //           WHERE oid IN (16,17,20,21,23,700,701,1043))
    //           UNION
    //           all
    //               (SELECT typoutput
    //               FROM pg_type
    //               WHERE oid IN (16,17,20,21,23,700,701,1043))
    //               UNION
    //               all
    //                   (SELECT typmodin
    //                   FROM pg_type
    //                   WHERE oid IN (16,17,20,21,23,700,701,1043))
    //                   UNION
    //                   all
    //                       (SELECT typmodout
    //                       FROM pg_type
    //                       WHERE oid IN (16,17,20,21,23,700,701,1043)) )
    //   ORDER BY pg_proc.oid;
    conn.execute(format!(
        "insert into kb_proc values
        (31,'byteaout',11,102,105,1,1043,'17','byteaout',''),
        (38,'int2in',11,102,105,1,21,'1043','int2in',''),
        (39,'int2out',11,102,105,1,1043,'21','int2out',''),
        (42,'int4in',11,102,105,1,23,'1043','int4in',''),
        (43,'int4out',11,102,105,1,1043,'23','int4out',''),
        (56,'boollt',11,102,105,2,16,'16 16','boollt',''),
        (57,'boolgt',11,102,105,2,16,'16 16','boolgt',''),
        (60,'booleq',11,102,105,2,16,'16 16','booleq',''),
        (63,'int2eq',11,102,105,2,16,'21 21','int2eq',''),
        (64,'int2lt',11,102,105,2,16,'21 21','int2lt',''),
        (65,'int4eq',11,102,105,2,16,'23 23','int4eq',''),
        (66,'int4lt',11,102,105,2,16,'23 23','int4lt',''),
        (84,'boolne',11,102,105,2,16,'16 16','boolne',''),
        (111,'numeric_fac',11,102,105,1,1700,'20','numeric_fac',''),
        (141,'int4mul',11,102,105,2,23,'23 23','int4mul',''),
        (144,'int4ne',11,102,105,2,16,'23 23','int4ne',''),
        (145,'int2ne',11,102,105,2,16,'21 21','int2ne',''),
        (146,'int2gt',11,102,105,2,16,'21 21','int2gt',''),
        (147,'int4gt',11,102,105,2,16,'23 23','int4gt',''),
        (148,'int2le',11,102,105,2,16,'21 21','int2le',''),
        (149,'int4le',11,102,105,2,16,'23 23','int4le',''),
        (150,'int4ge',11,102,105,2,16,'23 23','int4ge',''),
        (151,'int2ge',11,102,105,2,16,'21 21','int2ge',''),
        (152,'int2mul',11,102,105,2,21,'21 21','int2mul',''),
        (153,'int2div',11,102,105,2,21,'21 21','int2div',''),
        (154,'int4div',11,102,105,2,23,'23 23','int4div',''),
        (155,'int2mod',11,102,105,2,21,'21 21','int2mod',''),
        (156,'int4mod',11,102,105,2,23,'23 23','int4mod',''),
        (158,'int24eq',11,102,105,2,16,'21 23','int24eq',''),
        (159,'int42eq',11,102,105,2,16,'23 21','int42eq',''),
        (160,'int24lt',11,102,105,2,16,'21 23','int24lt',''),
        (161,'int42lt',11,102,105,2,16,'23 21','int42lt',''),
        (162,'int24gt',11,102,105,2,16,'21 23','int24gt',''),
        (163,'int42gt',11,102,105,2,16,'23 21','int42gt',''),
        (164,'int24ne',11,102,105,2,16,'21 23','int24ne',''),
        (165,'int42ne',11,102,105,2,16,'23 21','int42ne',''),
        (166,'int24le',11,102,105,2,16,'21 23','int24le',''),
        (167,'int42le',11,102,105,2,16,'23 21','int42le',''),
        (168,'int24ge',11,102,105,2,16,'21 23','int24ge',''),
        (169,'int42ge',11,102,105,2,16,'23 21','int42ge',''),
        (170,'int24mul',11,102,105,2,23,'21 23','int24mul',''),
        (171,'int42mul',11,102,105,2,23,'23 21','int42mul',''),
        (172,'int24div',11,102,105,2,23,'21 23','int24div',''),
        (173,'int42div',11,102,105,2,23,'23 21','int42div',''),
        (176,'int2pl',11,102,105,2,21,'21 21','int2pl',''),
        (177,'int4pl',11,102,105,2,23,'23 23','int4pl',''),
        (178,'int24pl',11,102,105,2,23,'21 23','int24pl',''),
        (179,'int42pl',11,102,105,2,23,'23 21','int42pl',''),
        (180,'int2mi',11,102,105,2,21,'21 21','int2mi',''),
        (181,'int4mi',11,102,105,2,23,'23 23','int4mi',''),
        (182,'int24mi',11,102,105,2,23,'21 23','int24mi',''),
        (183,'int42mi',11,102,105,2,23,'23 21','int42mi',''),
        (200,'float4in',11,102,105,1,700,'1043','float4in',''),
        (201,'float4out',11,102,105,1,1043,'700','float4out',''),
        (202,'float4mul',11,102,105,2,700,'700 700','float4mul',''),
        (203,'float4div',11,102,105,2,700,'700 700','float4div',''),
        (204,'float4pl',11,102,105,2,700,'700 700','float4pl',''),
        (205,'float4mi',11,102,105,2,700,'700 700','float4mi',''),
        (206,'float4um',11,102,105,1,700,'700','float4um',''),
        (207,'float4abs',11,102,105,1,700,'700','float4abs',''),
        (212,'int4um',11,102,105,1,23,'23','int4um',''),
        (213,'int2um',11,102,105,1,21,'21','int2um',''),
        (214,'float8in',11,102,105,1,701,'1043','float8in',''),
        (215,'float8out',11,102,105,1,1043,'701','float8out',''),
        (216,'float8mul',11,102,105,2,701,'701 701','float8mul',''),
        (217,'float8div',11,102,105,2,701,'701 701','float8div',''),
        (218,'float8pl',11,102,105,2,701,'701 701','float8pl',''),
        (219,'float8mi',11,102,105,2,701,'701 701','float8mi',''),
        (220,'float8um',11,102,105,1,701,'701','float8um',''),
        (221,'float8abs',11,102,105,1,701,'701','float8abs',''),
        (230,'dsqrt',11,102,105,1,701,'701','dsqrt',''),
        (231,'dcbrt',11,102,105,1,701,'701','dcbrt',''),
        (232,'dpow',11,102,105,2,701,'701 701','dpow',''),
        (279,'float48mul',11,102,105,2,701,'700 701','float48mul',''),
        (280,'float48div',11,102,105,2,701,'700 701','float48div',''),
        (281,'float48pl',11,102,105,2,701,'700 701','float48pl',''),
        (282,'float48mi',11,102,105,2,701,'700 701','float48mi',''),
        (283,'float84mul',11,102,105,2,701,'701 700','float84mul',''),
        (284,'float84div',11,102,105,2,701,'701 700','float84div',''),
        (285,'float84pl',11,102,105,2,701,'701 700','float84pl',''),
        (286,'float84mi',11,102,105,2,701,'701 700','float84mi',''),
        (287,'float4eq',11,102,105,2,16,'700 700','float4eq',''),
        (288,'float4ne',11,102,105,2,16,'700 700','float4ne',''),
        (289,'float4lt',11,102,105,2,16,'700 700','float4lt',''),
        (290,'float4le',11,102,105,2,16,'700 700','float4le',''),
        (291,'float4gt',11,102,105,2,16,'700 700','float4gt',''),
        (292,'float4ge',11,102,105,2,16,'700 700','float4ge',''),
        (293,'float8eq',11,102,105,2,16,'701 701','float8eq',''),
        (294,'float8ne',11,102,105,2,16,'701 701','float8ne',''),
        (295,'float8lt',11,102,105,2,16,'701 701','float8lt',''),
        (296,'float8le',11,102,105,2,16,'701 701','float8le',''),
        (297,'float8gt',11,102,105,2,16,'701 701','float8gt',''),
        (298,'float8ge',11,102,105,2,16,'701 701','float8ge',''),
        (299,'float48eq',11,102,105,2,16,'700 701','float48eq',''),
        (300,'float48ne',11,102,105,2,16,'700 701','float48ne',''),
        (301,'float48lt',11,102,105,2,16,'700 701','float48lt',''),
        (302,'float48le',11,102,105,2,16,'700 701','float48le',''),
        (303,'float48gt',11,102,105,2,16,'700 701','float48gt',''),
        (304,'float48ge',11,102,105,2,16,'700 701','float48ge',''),
        (305,'float84eq',11,102,105,2,16,'701 700','float84eq',''),
        (306,'float84ne',11,102,105,2,16,'701 700','float84ne',''),
        (307,'float84lt',11,102,105,2,16,'701 700','float84lt',''),
        (308,'float84le',11,102,105,2,16,'701 700','float84le',''),
        (309,'float84gt',11,102,105,2,16,'701 700','float84gt',''),
        (310,'float84ge',11,102,105,2,16,'701 700','float84ge',''),
        (460,'int8in',11,102,105,1,20,'1043','int8in',''),
        (461,'int8out',11,102,105,1,1043,'20','int8out',''),
        (462,'int8um',11,102,105,1,20,'20','int8um',''),
        (463,'int8pl',11,102,105,2,20,'20 20','int8pl',''),
        (464,'int8mi',11,102,105,2,20,'20 20','int8mi',''),
        (465,'int8mul',11,102,105,2,20,'20 20','int8mul',''),
        (466,'int8div',11,102,105,2,20,'20 20','int8div',''),
        (467,'int8eq',11,102,105,2,16,'20 20','int8eq',''),
        (468,'int8ne',11,102,105,2,16,'20 20','int8ne',''),
        (469,'int8lt',11,102,105,2,16,'20 20','int8lt',''),
        (470,'int8gt',11,102,105,2,16,'20 20','int8gt',''),
        (471,'int8le',11,102,105,2,16,'20 20','int8le',''),
        (472,'int8ge',11,102,105,2,16,'20 20','int8ge',''),
        (474,'int84eq',11,102,105,2,16,'20 23','int84eq',''),
        (475,'int84ne',11,102,105,2,16,'20 23','int84ne',''),
        (476,'int84lt',11,102,105,2,16,'20 23','int84lt',''),
        (477,'int84gt',11,102,105,2,16,'20 23','int84gt',''),
        (478,'int84le',11,102,105,2,16,'20 23','int84le',''),
        (479,'int84ge',11,102,105,2,16,'20 23','int84ge',''),
        (837,'int82pl',11,102,105,2,20,'20 21','int82pl',''),
        (838,'int82mi',11,102,105,2,20,'20 21','int82mi',''),
        (839,'int82mul',11,102,105,2,20,'20 21','int82mul',''),
        (840,'int82div',11,102,105,2,20,'20 21','int82div',''),
        (841,'int28pl',11,102,105,2,20,'21 20','int28pl',''),
        (852,'int48eq',11,102,105,2,16,'23 20','int48eq',''),
        (853,'int48ne',11,102,105,2,16,'23 20','int48ne',''),
        (854,'int48lt',11,102,105,2,16,'23 20','int48lt',''),
        (855,'int48gt',11,102,105,2,16,'23 20','int48gt',''),
        (856,'int48le',11,102,105,2,16,'23 20','int48le',''),
        (857,'int48ge',11,102,105,2,16,'23 20','int48ge',''),
        (942,'int28mi',11,102,105,2,20,'21 20','int28mi',''),
        (943,'int28mul',11,102,105,2,20,'21 20','int28mul',''),
        (945,'int8mod',11,102,105,2,20,'20 20','int8mod',''),
        (948,'int28div',11,102,105,2,20,'21 20','int28div',''),
        (1046,'varcharin',11,102,105,3,1043,'1043 26 23','varcharin',''),
        (1047,'varcharout',11,102,105,1,1043,'1043','varcharout',''),
        (1230,'int8abs',11,102,105,1,20,'20','int8abs',''),
        (1242,'boolin',11,102,105,1,16,'1043','boolin',''),
        (1243,'boolout',11,102,105,1,1043,'16','boolout',''),
        (1244,'byteain',11,102,105,1,17,'1043','byteain',''),
        (1251,'int4abs',11,102,105,1,23,'23','int4abs',''),
        (1253,'int2abs',11,102,105,1,21,'21','int2abs',''),
        (1274,'int84pl',11,102,105,2,20,'20 23','int84pl',''),
        (1275,'int84mi',11,102,105,2,20,'20 23','int84mi',''),
        (1276,'int84mul',11,102,105,2,20,'20 23','int84mul',''),
        (1277,'int84div',11,102,105,2,20,'20 23','int84div',''),
        (1278,'int48pl',11,102,105,2,20,'23 20','int48pl',''),
        (1279,'int48mi',11,102,105,2,20,'23 20','int48mi',''),
        (1280,'int48mul',11,102,105,2,20,'23 20','int48mul',''),
        (1281,'int48div',11,102,105,2,20,'23 20','int48div',''),
        (1691,'boolle',11,102,105,2,16,'16 16','boolle',''),
        (1692,'boolge',11,102,105,2,16,'16 16','boolge',''),
        (1850,'int28eq',11,102,105,2,16,'21 20','int28eq',''),
        (1851,'int28ne',11,102,105,2,16,'21 20','int28ne',''),
        (1852,'int28lt',11,102,105,2,16,'21 20','int28lt',''),
        (1853,'int28gt',11,102,105,2,16,'21 20','int28gt',''),
        (1854,'int28le',11,102,105,2,16,'21 20','int28le',''),
        (1855,'int28ge',11,102,105,2,16,'21 20','int28ge',''),
        (1856,'int82eq',11,102,105,2,16,'20 21','int82eq',''),
        (1857,'int82ne',11,102,105,2,16,'20 21','int82ne',''),
        (1858,'int82lt',11,102,105,2,16,'20 21','int82lt',''),
        (1859,'int82gt',11,102,105,2,16,'20 21','int82gt',''),
        (1860,'int82le',11,102,105,2,16,'20 21','int82le',''),
        (1861,'int82ge',11,102,105,2,16,'20 21','int82ge',''),
        (1892,'int2and',11,102,105,2,21,'21 21','int2and',''),
        (1893,'int2or',11,102,105,2,21,'21 21','int2or',''),
        (1894,'int2xor',11,102,105,2,21,'21 21','int2xor',''),
        (1895,'int2not',11,102,105,1,21,'21','int2not',''),
        (1896,'int2shl',11,102,105,2,21,'21 23','int2shl',''),
        (1897,'int2shr',11,102,105,2,21,'21 23','int2shr',''),
        (1898,'int4and',11,102,105,2,23,'23 23','int4and',''),
        (1899,'int4or',11,102,105,2,23,'23 23','int4or',''),
        (1900,'int4xor',11,102,105,2,23,'23 23','int4xor',''),
        (1901,'int4not',11,102,105,1,23,'23','int4not',''),
        (1902,'int4shl',11,102,105,2,23,'23 23','int4shl',''),
        (1903,'int4shr',11,102,105,2,23,'23 23','int4shr',''),
        (1904,'int8and',11,102,105,2,20,'20 20','int8and',''),
        (1905,'int8or',11,102,105,2,20,'20 20','int8or',''),
        (1906,'int8xor',11,102,105,2,20,'20 20','int8xor',''),
        (1907,'int8not',11,102,105,1,20,'20','int8not',''),
        (1908,'int8shl',11,102,105,2,20,'20 23','int8shl',''),
        (1909,'int8shr',11,102,105,2,20,'20 23','int8shr',''),
        (1910,'int8up',11,102,105,1,20,'20','int8up',''),
        (1911,'int2up',11,102,105,1,21,'21','int2up',''),
        (1912,'int4up',11,102,105,1,23,'23','int4up',''),
        (1913,'float4up',11,102,105,1,700,'700','float4up',''),
        (1914,'float8up',11,102,105,1,701,'701','float8up',''),
        (1948,'byteaeq',11,102,105,2,16,'17 17','byteaeq',''),
        (1949,'bytealt',11,102,105,2,16,'17 17','bytealt',''),
        (1950,'byteale',11,102,105,2,16,'17 17','byteale',''),
        (1951,'byteagt',11,102,105,2,16,'17 17','byteagt',''),
        (1952,'byteage',11,102,105,2,16,'17 17','byteage',''),
        (1953,'byteane',11,102,105,2,16,'17 17','byteane',''),
        (2005,'bytealike',11,102,105,2,16,'17 17','bytealike',''),
        (2006,'byteanlike',11,102,105,2,16,'17 17','byteanlike',''),
        (2011,'byteacat',11,102,105,2,17,'17 17','byteacat',''),
        (2915,'varchartypmodin',11,102,105,1,23,'1263','varchartypmodin',''),
        (2916,'varchartypmodout',11,102,105,1,1043,'23','varchartypmodout','');
    ",
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

fn create_ctl(gucstate: &guc::GucState) -> anyhow::Result<()> {
    let lsn = wal::Lsn::new(20181218).unwrap();
    let tli = wal::TimeLineID::new(1).unwrap();
    let wals = wal::init(tli, lsn, None, lsn, gucstate)?;

    let ckpt = wal::Ckpt {
        curtli: tli,
        prevtli: tli,
        redo: lsn,
        nextxid: Xid::new(2).unwrap(),
        nextoid: Oid::new(65536).unwrap(),
        time: KBSystemTime::now(),
    };
    let mut rec = wal::new_ckpt_rec(&ckpt);
    wal::finish_record(&mut rec, wal::RmgrId::Xlog, wal::XlogInfo::Ckpt as u8, None);

    wals.fsync(wals.insert_record(rec));

    let ctl = wal::Ctl::new(lsn, ckpt);
    ctl.persist()
}

fn main() {
    init_log();
    let cmdline = App::new("initdb initializes a KuiBaDB cluster.")
        .version(kuiba::KB_VERSTR)
        .author("盏一 <w@hidva.com>")
        .about("KuiBaDB is another Postgresql written in Rust")
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
    std::fs::create_dir_all("kb_wal").unwrap();
    std::fs::create_dir_all("kb_xact").unwrap();
    let mut gucstate = guc::GucState::default();
    guc::load_apply_gucs("kuiba.conf", &mut gucstate).unwrap();
    let gucstate = gucstate;
    log::info!("create global metadata");
    create_global_metadata();
    log::info!("create template0 metadata");
    create_template0_metadata();
    log::info!("create kuiba metadata");
    create_kuiba_metadata();
    log::info!("create control file");
    create_ctl(&gucstate).unwrap();
    log::info!("initdb success");
    // Important things are to be repeated for 3 times~
    sync();
    sync();
    sync();
}

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
use crate::*;
use anyhow;

#[derive(Debug)]
pub struct FormDataDatabase {
    pub oid: Oid,
    pub datname: String,
    pub datistemplate: bool,
    pub datallowconn: bool,
    pub datfrozenxid: Xid,
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
                datfrozenxid: column_val(row, "datfrozenxid").unwrap().parse().unwrap(),
            });
            true
        },
    )?;
    retdb
}

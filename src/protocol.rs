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
use crate::utils::ser;
use crate::{errctx, kbanyhow, kbensure, Oid, OptOid, SockReader, SockWriter};
use crate::{guc, AttrNumber};
use anyhow::Context;
use byteorder::{NetworkEndian, ReadBytesExt};
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{Cursor, Read, Write};
use std::mem::size_of;
use std::str::from_utf8;

mod errcodes;
pub use errcodes::*;

fn read_body(stream: &mut SockReader, content: &mut Vec<u8>) -> std::io::Result<()> {
    let len = stream.read_u32::<NetworkEndian>()?;
    content.resize(len as usize - size_of::<u32>(), 0);
    stream.read_exact(content.as_mut_slice())?;
    return Ok(());
}

pub fn read_message(stream: &mut SockReader) -> std::io::Result<(i8, Vec<u8>)> {
    let mut content = Vec::new();
    let msgtype = stream.read_i8()?;
    read_body(stream, &mut content)?;
    Ok((msgtype, content))
}

pub fn read_startup_message(stream: &mut SockReader, content: &mut Vec<u8>) -> std::io::Result<()> {
    read_body(stream, content)
}

#[derive(Debug)]
pub struct CancelRequest {
    pub sess: u32,
    pub key: u32,
}

impl CancelRequest {
    pub fn deserialize(d: &[u8]) -> Option<Self> {
        if d.len() != 12 || d[..4] != [0x04, 0xd2, 0x16, 0x2e] {
            None
        } else {
            // All unwrap will be erased at release build.
            let sess = u32::from_be_bytes(d[4..8].try_into().unwrap());
            let key = u32::from_be_bytes(d[8..12].try_into().unwrap());
            Some(CancelRequest { sess, key })
        }
    }
}

pub struct SSLRequest {}

impl SSLRequest {
    pub fn deserialize(d: &[u8]) -> Option<Self> {
        if d != [0x04, 0xd2, 0x16, 0x2f] {
            None
        } else {
            Some(SSLRequest {})
        }
    }
}

#[repr(i8)]
pub enum MsgType {
    Query = 'Q' as i8,
    Terminate = 'X' as i8,
    EOF = -1,
}

pub fn write_message<T: Message>(stream: &mut SockWriter, msg: &T) {
    // ignore error, just as PostgreSQL.
    let _ = stream.write_all(&msg.serialize());
}

pub trait Message {
    fn serialize(&self) -> Vec<u8>;
}

const STARTUP_USER_PARAM: &str = "user";
const STARTUP_DATABASE_PARAM: &str = "database";
const STARTUP_CLIENT_ENCODING: &str = "client_encoding";

#[derive(Debug)]
pub struct StartupMessage<'a> {
    pub major_ver: u16,
    pub minor_ver: u16,
    username: &'a str,
    pub params: HashMap<&'a str, &'a str>,
}

fn find(d: &[u8], start: usize, val: u8) -> i64 {
    for (idx, &dv) in d[start..].iter().enumerate() {
        if dv == val {
            return (idx + start) as i64;
        }
    }
    -1
}

fn read_cstr<'a>(cursor: &mut Cursor<&'a [u8]>) -> anyhow::Result<&'a str> {
    let data = cursor.get_ref();
    let idx = find(data, cursor.position() as usize, 0);
    kbensure!(
        idx >= 0,
        ERRCODE_PROTOCOL_VIOLATION,
        "invalid string in message"
    );
    let cstrdata = &data[cursor.position() as usize..idx as usize];
    let retstr = from_utf8(cstrdata).with_context(|| {
        errctx!(
            ERRCODE_PROTOCOL_VIOLATION,
            "invalid UTF-8 string in message"
        )
    })?;
    cursor.set_position(idx as u64 + 1);
    Ok(retstr)
}

impl StartupMessage<'_> {
    pub fn deserialize(d: &[u8]) -> anyhow::Result<StartupMessage<'_>> {
        //log::trace!("StartupMessage deserialize. d={:?}", d);
        let mut cursor = Cursor::new(d);
        let major_ver = cursor.read_u16::<NetworkEndian>()?;
        let minor_ver = cursor.read_u16::<NetworkEndian>()?;
        let mut params = HashMap::new();
        loop {
            let name = read_cstr(&mut cursor)?;
            if name.is_empty() {
                break;
            }
            let val = read_cstr(&mut cursor)?;
            params.insert(name, val);
        }
        let user = params
            .get(&STARTUP_USER_PARAM)
            .ok_or_else(|| kbanyhow!(ERRCODE_PROTOCOL_VIOLATION, "StartupMessage: no user key"))?;
        return Ok(StartupMessage {
            major_ver,
            minor_ver,
            username: user,
            params,
        });
    }

    pub fn user(&self) -> &str {
        self.username
    }

    pub fn database(&self) -> &str {
        self.params
            .get(&STARTUP_DATABASE_PARAM)
            .map_or_else(|| self.user(), |v| *v)
    }

    pub fn check_client_encoding(&self, expected: &str) -> bool {
        self.params.get(&STARTUP_CLIENT_ENCODING).map_or(
            true, /* pgbench don't send STARTUP_CLIENT_ENCODING */
            |v| v.eq_ignore_ascii_case(expected),
        )
    }
}

// See https://www.postgresql.org/docs/devel/protocol-error-fields.html for details.
#[derive(Default)]
pub struct ErrFields<'a> {
    pub severity: Option<&'a str>,
    pub code: Option<&'a str>,
    pub msg: Option<&'a str>,
    // pub V: Option<&'a str>,
    // pub D: Option<&'a str>,
    // pub H: Option<&'a str>,
    // pub P: Option<&'a str>,
    // pub p: Option<&'a str>,
    // pub q: Option<&'a str>,
    // pub W: Option<&'a str>,
    // pub s: Option<&'a str>,
    // pub t: Option<&'a str>,
    // pub c: Option<&'a str>,
    // pub d: Option<&'a str>,
    // pub n: Option<&'a str>,
    // pub F: Option<&'a str>,
    // pub L: Option<&'a str>,
    // pub R: Option<&'a str>,
}

fn serialize_errmsg(typ: u8, fields: &ErrFields) -> Vec<u8> {
    let mut out = Vec::<u8>::with_capacity(32);
    out.resize(5, typ);
    macro_rules! write_field {
        ($field: ident, $fieldtype: literal) => {
            if let Some(v) = fields.$field {
                out.push($fieldtype as u8);
                ser::ser_cstr(&mut out, v);
            }
        };
    }
    write_field!(severity, 'S');
    write_field!(code, 'C');
    write_field!(msg, 'M');
    // write_field!(V, 'V');
    // write_field!(D, 'D');
    // write_field!(H, 'H');
    // write_field!(P, 'P');
    // write_field!(p, 'p');
    // write_field!(q, 'q');
    // write_field!(W, 'W');
    // write_field!(s, 's');
    // write_field!(t, 't');
    // write_field!(c, 'c');
    // write_field!(d, 'd');
    // write_field!(n, 'n');
    // write_field!(F, 'F');
    // write_field!(L, 'L');
    // write_field!(R, 'R');
    out.push(0);
    let msglen = out.len() - 1;
    ser::ser_be_u32_at(&mut out, 1, msglen as u32);
    return out;
}

pub const SEVERITY_ERR: &str = "ERROR";
pub const SEVERITY_FATAL: &str = "FATAL";

pub struct ErrorResponse<'a> {
    pub fields: ErrFields<'a>,
}

impl<'a> ErrorResponse<'a> {
    pub fn new<'b: 'a, 'c: 'a, 'd: 'a>(
        severity: &'b str,
        code: &'c str,
        msg: &'d str,
    ) -> ErrorResponse<'a> {
        ErrorResponse {
            fields: ErrFields {
                severity: Some(severity),
                code: Some(code),
                msg: Some(msg),
                ..ErrFields::default()
            },
        }
    }
}

impl<'a> Message for ErrorResponse<'a> {
    fn serialize(&self) -> Vec<u8> {
        serialize_errmsg('E' as u8, &self.fields)
    }
}

pub struct AuthenticationOk {}

impl Message for AuthenticationOk {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push('R' as u8);
        ser::ser_be_u32(&mut out, 8);
        ser::ser_be_u32(&mut out, 0);
        return out;
    }
}

pub struct BackendKeyData {
    backendid: u32,
    key: u32,
}

impl BackendKeyData {
    pub fn new(backendid: u32, key: u32) -> BackendKeyData {
        BackendKeyData { backendid, key }
    }
}

impl Message for BackendKeyData {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 4 * 3);
        out.push('K' as u8);
        ser::ser_be_u32(&mut out, 12);
        ser::ser_be_u32(&mut out, self.backendid);
        ser::ser_be_u32(&mut out, self.key);
        return out;
    }
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum XactStatus {
    NotInBlock = 'I' as u8,
    InBlock = 'T' as u8,
    Failed = 'E' as u8,
}

pub struct ReadyForQuery {
    status: XactStatus,
}

impl ReadyForQuery {
    pub fn new(status: XactStatus) -> ReadyForQuery {
        ReadyForQuery { status }
    }
}

impl Message for ReadyForQuery {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 * 2 + 4);
        out.push('Z' as u8);
        ser::ser_be_u32(&mut out, 5);
        out.push(self.status as u8);
        return out;
    }
}

#[derive(Debug)]
pub struct Query<'a> {
    pub query: &'a str,
}

impl Query<'_> {
    pub fn deserialize(d: &[u8]) -> anyhow::Result<Query<'_>> {
        kbensure!(
            !d.is_empty(),
            ERRCODE_PROTOCOL_VIOLATION,
            "Query string is empty"
        );
        let qstr = from_utf8(&d[..d.len() - 1])
            .with_context(|| errctx!(ERRCODE_PROTOCOL_VIOLATION, "Query string is not UTF-8"))?;
        return Ok(Query { query: qstr });
    }
}

pub struct CommandComplete<'a> {
    pub tag: &'a str,
}

impl Message for CommandComplete<'_> {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        out.resize(5, 'C' as u8);
        ser::ser_cstr(&mut out, self.tag);
        let msglen = out.len() - 1;
        ser::ser_be_u32_at(&mut out, 1, msglen as u32);
        return out;
    }
}

pub struct ParameterStatus<'a> {
    name: &'a str,
    value: &'a str,
}

impl<'a> ParameterStatus<'a> {
    fn new<'b: 'a, 'c: 'a>(name: &'b str, value: &'c str) -> ParameterStatus<'a> {
        ParameterStatus { name, value }
    }
}

impl Message for ParameterStatus<'_> {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.resize(5, 'S' as u8);
        ser::ser_cstr(&mut out, self.name);
        ser::ser_cstr(&mut out, self.value);
        let msglen = out.len() - 1;
        ser::ser_be_u32_at(&mut out, 1, msglen as u32);
        return out;
    }
}

pub fn report_guc(
    name: &str,
    gucvals: &guc::GucState,
    gucidx: guc::GucIdx,
    stream: &mut SockWriter,
) {
    let gen = guc::get_guc_generic(gucidx);
    if !gen.should_report() {
        return;
    }
    let value = guc::show(gen, gucvals, gucidx);
    log::trace!("report guc. name={} value={}", name, value);
    let msg = ParameterStatus::new(name, &value);
    write_message(stream, &msg);
}

pub fn report_all_gucs(gucvals: &guc::GucState, stream: &mut SockWriter) {
    for (&name, &gucidx) in guc::GUC_NAMEINFO_MAP.iter() {
        report_guc(name, gucvals, gucidx, stream)
    }
}

pub struct EmptyQueryResponse {}

impl Message for EmptyQueryResponse {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 4);
        out.push('I' as u8);
        ser::ser_be_u32(&mut out, 4);
        return out;
    }
}

#[derive(Clone, Copy)]
enum Format {
    Text = 0,
}

pub struct FieldDesc<'a> {
    name: &'a str,
    reloid: OptOid,
    typoid: Oid,
    typmod: i32,
    attnum: Option<AttrNumber>,
    typlen: i16,
    format: Format,
}

impl FieldDesc<'_> {
    pub const fn new(name: &str, typoid: Oid, typmod: i32, typlen: i16) -> FieldDesc<'_> {
        FieldDesc {
            name,
            reloid: OptOid(None),
            typoid,
            typmod,
            attnum: None,
            typlen,
            format: Format::Text,
        }
    }
}

pub struct RowDescription<'a, 'b> {
    pub fields: &'b [FieldDesc<'a>],
}

impl Message for RowDescription<'_, '_> {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.resize(5, 'T' as u8);
        ser::ser_be_u16(&mut out, self.fields.len() as u16);
        for field in self.fields {
            let attnum: u16 = match field.attnum {
                None => 0,
                Some(v) => v.get(),
            };
            ser::ser_cstr(&mut out, field.name);
            ser::ser_be_u32(&mut out, field.reloid.into());
            ser::ser_be_u16(&mut out, attnum);
            ser::ser_be_u32(&mut out, field.typoid.get());
            ser::ser_be_i16(&mut out, field.typlen.into());
            ser::ser_be_i32(&mut out, field.typmod.into());
            ser::ser_be_u16(&mut out, field.format as u16);
        }
        let msglen = out.len() - 1;
        ser::ser_be_u32_at(&mut out, 1, msglen as u32);
        return out;
    }
}

pub struct DataRow<'a, 'b> {
    pub data: &'b [Option<&'a [u8]>],
}

impl Message for DataRow<'_, '_> {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.resize(5, 'D' as u8);
        ser::ser_be_u16(&mut out, self.data.len() as u16);
        for &col in self.data {
            match col {
                None => {
                    ser::ser_be_i32(&mut out, -1);
                }
                Some(dataval) => {
                    // no need for trailing '\0'
                    ser::ser_be_i32(&mut out, dataval.len() as i32);
                    out.extend_from_slice(dataval);
                }
            }
        }
        let msglen = out.len() - 1;
        ser::ser_be_u32_at(&mut out, 1, msglen as u32);
        return out;
    }
}

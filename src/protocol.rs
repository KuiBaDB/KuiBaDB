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
use crate::guc;
use crate::utils::{ser, AttrNumber};
use crate::{errctx, kbanyhow, kbensure, Oid, OptOid, Sock};
use anyhow::Context;
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{self, Cursor};
use std::mem::size_of;
use std::str::from_utf8;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::trace;

mod errcodes;
pub(crate) use errcodes::*;

async fn read_body(stream: &mut Sock, content: &mut Vec<u8>) -> io::Result<()> {
    let len = stream.s.read_u32().await?;
    let msglen = len as usize - size_of::<u32>();
    content.reserve(msglen);
    unsafe {
        content.set_len(msglen);
    }
    stream.s.read_exact(content.as_mut_slice()).await?;
    return Ok(());
}

pub(crate) async fn read_message(stream: &mut Sock, content: &mut Vec<u8>) -> io::Result<i8> {
    let msgtype = stream.s.read_i8().await?;
    read_body(stream, content).await?;
    Ok(msgtype)
}

pub(crate) async fn read_startup_message(
    stream: &mut Sock,
    content: &mut Vec<u8>,
) -> io::Result<()> {
    read_body(stream, content).await
}

#[derive(Debug)]
pub(crate) struct CancelRequest {
    pub(crate) sess: u32,
    pub(crate) key: u32,
}

impl CancelRequest {
    pub(crate) fn deserialize(d: &[u8]) -> Option<Self> {
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

pub(crate) struct SSLRequest {}

impl SSLRequest {
    pub(crate) fn deserialize(d: &[u8]) -> Option<Self> {
        if d != [0x04, 0xd2, 0x16, 0x2f] {
            None
        } else {
            Some(SSLRequest {})
        }
    }
}

#[repr(i8)]
pub(crate) enum MsgType {
    Query = 'Q' as i8,
    Terminate = 'X' as i8,
    EOF = -1,
}

pub(crate) async fn write_message<T: Message>(stream: &mut Sock, msg: &T) {
    // ignore error, just as PostgreSQL.
    msg.serialize(&mut stream.serbuf);
    let _ = stream.s.write_all(&stream.serbuf).await;
    return;
}

pub(crate) trait Message {
    fn serialize(&self, buf: &mut Vec<u8>);
}

const STARTUP_USER_PARAM: &str = "user";
const STARTUP_DATABASE_PARAM: &str = "database";
const STARTUP_CLIENT_ENCODING: &str = "client_encoding";

#[derive(Debug)]
pub(crate) struct StartupMessage<'a> {
    pub(crate) major_ver: u16,
    pub(crate) minor_ver: u16,
    username: &'a str,
    pub(crate) params: HashMap<&'a str, &'a str>,
}

fn find(d: &[u8], start: usize, val: u8) -> i64 {
    d[start..]
        .iter()
        .position(|&v| v == val)
        .map_or(-1, |idx| (idx + start) as i64)
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
    pub(crate) fn deserialize(d: &[u8]) -> anyhow::Result<StartupMessage<'_>> {
        //log::trace!("StartupMessage deserialize. d={:?}", d);
        kbensure!(
            d.len() >= 4,
            ERRCODE_PROTOCOL_VIOLATION,
            "invalid StartupMessage"
        );
        let minor_ver = u16::from_le_bytes([d[3], d[2]]);
        let major_ver = u16::from_le_bytes([d[1], d[0]]);
        let mut cursor = Cursor::new(&d[4..]);
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

    pub(crate) fn user(&self) -> &str {
        self.username
    }

    pub(crate) fn database(&self) -> &str {
        self.params
            .get(&STARTUP_DATABASE_PARAM)
            .map_or_else(|| self.user(), |v| *v)
    }

    pub(crate) fn check_client_encoding(&self, expected: &str) -> bool {
        self.params.get(&STARTUP_CLIENT_ENCODING).map_or(
            true, /* pgbench don't send STARTUP_CLIENT_ENCODING */
            |v| v.eq_ignore_ascii_case(expected),
        )
    }
}

// See https://www.postgresql.org/docs/devel/protocol-error-fields.html for details.
#[derive(Default)]
pub(crate) struct ErrFields<'a> {
    pub(crate) severity: Option<&'a str>,
    pub(crate) code: Option<&'a str>,
    pub(crate) msg: Option<&'a str>,
    // pub(crate) V: Option<&'a str>,
    // pub(crate) D: Option<&'a str>,
    // pub(crate) H: Option<&'a str>,
    // pub(crate) P: Option<&'a str>,
    // pub(crate) p: Option<&'a str>,
    // pub(crate) q: Option<&'a str>,
    // pub(crate) W: Option<&'a str>,
    // pub(crate) s: Option<&'a str>,
    // pub(crate) t: Option<&'a str>,
    // pub(crate) c: Option<&'a str>,
    // pub(crate) d: Option<&'a str>,
    // pub(crate) n: Option<&'a str>,
    // pub(crate) F: Option<&'a str>,
    // pub(crate) L: Option<&'a str>,
    // pub(crate) R: Option<&'a str>,
}

fn serialize_errmsg(typ: u8, fields: &ErrFields, out: &mut Vec<u8>) {
    out.reserve(32);
    out.clear();
    out.resize(5, typ);
    macro_rules! write_field {
        ($field: ident, $fieldtype: literal) => {
            if let Some(v) = fields.$field {
                out.push($fieldtype as u8);
                ser::ser_cstr(out, v);
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
    ser::ser_be_u32_at(out, 1, msglen as u32);
    return;
}

pub(crate) const SEVERITY_ERR: &str = "ERROR";
pub(crate) const SEVERITY_FATAL: &str = "FATAL";

pub(crate) struct ErrorResponse<'a> {
    pub(crate) fields: ErrFields<'a>,
}

impl<'a> ErrorResponse<'a> {
    pub(crate) fn new<'b: 'a, 'c: 'a, 'd: 'a>(
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
    fn serialize(&self, buff: &mut Vec<u8>) {
        serialize_errmsg('E' as u8, &self.fields, buff)
    }
}

pub(crate) struct AuthenticationOk {}

impl Message for AuthenticationOk {
    fn serialize(&self, out: &mut Vec<u8>) {
        out.reserve(9);
        out.clear();
        out.push('R' as u8);
        ser::ser_be_u32(out, 8);
        ser::ser_be_u32(out, 0);
        return;
    }
}

pub(crate) struct BackendKeyData {
    backendid: u32,
    key: u32,
}

impl BackendKeyData {
    pub(crate) fn new(backendid: u32, key: u32) -> BackendKeyData {
        BackendKeyData { backendid, key }
    }
}

impl Message for BackendKeyData {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.reserve(1 + 4 * 3);
        buff.clear();
        buff.push('K' as u8);
        ser::ser_be_u32(buff, 12);
        ser::ser_be_u32(buff, self.backendid);
        ser::ser_be_u32(buff, self.key);
        return;
    }
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub(crate) enum XactStatus {
    NotInBlock = 'I' as u8,
    InBlock = 'T' as u8,
    Failed = 'E' as u8,
}

pub(crate) struct ReadyForQuery {
    status: XactStatus,
}

impl ReadyForQuery {
    pub(crate) fn new(status: XactStatus) -> ReadyForQuery {
        ReadyForQuery { status }
    }
}

impl Message for ReadyForQuery {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.reserve(1 * 2 + 4);
        buff.clear();
        buff.push('Z' as u8);
        ser::ser_be_u32(buff, 5);
        buff.push(self.status as u8);
        return;
    }
}

#[derive(Debug)]
pub(crate) struct Query<'a> {
    pub(crate) query: &'a str,
}

impl Query<'_> {
    pub(crate) fn deserialize(d: &[u8]) -> anyhow::Result<Query<'_>> {
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

pub(crate) struct CommandComplete<'a> {
    pub(crate) tag: &'a str,
}

impl Message for CommandComplete<'_> {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.reserve(32);
        buff.clear();
        buff.resize(5, 'C' as u8);
        ser::ser_cstr(buff, self.tag);
        let msglen = buff.len() - 1;
        ser::ser_be_u32_at(buff, 1, msglen as u32);
        return;
    }
}

pub(crate) struct ParameterStatus<'a> {
    name: &'a str,
    value: &'a str,
}

impl<'a> ParameterStatus<'a> {
    fn new<'b: 'a, 'c: 'a>(name: &'b str, value: &'c str) -> ParameterStatus<'a> {
        ParameterStatus { name, value }
    }
}

impl Message for ParameterStatus<'_> {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.reserve(64);
        buff.clear();
        buff.resize(5, 'S' as u8);
        ser::ser_cstr(buff, self.name);
        ser::ser_cstr(buff, self.value);
        let msglen = buff.len() - 1;
        ser::ser_be_u32_at(buff, 1, msglen as u32);
        return;
    }
}

pub(crate) async fn report_guc(
    name: &str,
    gucvals: &guc::GucState,
    gucidx: guc::GucIdx,
    stream: &mut Sock,
) {
    let gen = guc::get_guc_generic(gucidx);
    if !gen.should_report() {
        return;
    }
    let value = guc::show(gen, gucvals, gucidx);
    trace!("report guc. name={} value={}", name, value);
    let msg = ParameterStatus::new(name, &value);
    write_message(stream, &msg).await;
    return;
}

pub(crate) async fn report_all_gucs(gucvals: &guc::GucState, stream: &mut Sock) {
    for (&name, &gucidx) in guc::GUC_NAMEINFO_MAP.iter() {
        report_guc(name, gucvals, gucidx, stream).await
    }
}

pub(crate) struct EmptyQueryResponse {}

impl Message for EmptyQueryResponse {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.reserve(1 + 4);
        buff.clear();
        buff.push('I' as u8);
        ser::ser_be_u32(buff, 4);
        return;
    }
}

#[derive(Clone, Copy)]
enum Format {
    Text = 0,
}

pub(crate) struct FieldDesc<'a> {
    name: &'a str,
    reloid: OptOid,
    typoid: Oid,
    typmod: i32,
    attnum: Option<AttrNumber>,
    typlen: i16,
    format: Format,
}

impl FieldDesc<'_> {
    pub(crate) const fn new(name: &str, typoid: Oid, typmod: i32, typlen: i16) -> FieldDesc<'_> {
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

pub(crate) struct RowDescription<'a, 'b> {
    pub(crate) fields: &'b [FieldDesc<'a>],
}

impl Message for RowDescription<'_, '_> {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.reserve(64);
        buff.clear();
        buff.resize(5, 'T' as u8);
        ser::ser_be_u16(buff, self.fields.len() as u16);
        for field in self.fields {
            let attnum: u16 = match field.attnum {
                None => 0,
                Some(v) => v.get(),
            };
            ser::ser_cstr(buff, field.name);
            ser::ser_be_u32(buff, field.reloid.into());
            ser::ser_be_u16(buff, attnum);
            ser::ser_be_u32(buff, field.typoid.get());
            ser::ser_be_i16(buff, field.typlen.into());
            ser::ser_be_i32(buff, field.typmod.into());
            ser::ser_be_u16(buff, field.format as u16);
        }
        let msglen = buff.len() - 1;
        ser::ser_be_u32_at(buff, 1, msglen as u32);
        return;
    }
}

pub(crate) struct DataRow<'a, 'b> {
    pub(crate) data: &'b [Option<&'a [u8]>],
}

impl Message for DataRow<'_, '_> {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.reserve(64);
        buff.clear();
        buff.resize(5, 'D' as u8);
        ser::ser_be_u16(buff, self.data.len() as u16);
        for &col in self.data {
            match col {
                None => {
                    ser::ser_be_i32(buff, -1);
                }
                Some(dataval) => {
                    // no need for trailing '\0'
                    ser::ser_be_i32(buff, dataval.len() as i32);
                    buff.extend_from_slice(dataval);
                }
            }
        }
        let msglen = buff.len() - 1;
        ser::ser_be_u32_at(buff, 1, msglen as u32);
        return;
    }
}

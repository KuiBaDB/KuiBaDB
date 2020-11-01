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
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;

mod errcodes;
pub use errcodes::*;

fn read_body(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let len = stream.read_u32::<NetworkEndian>()?;
    let mut content = Vec::<u8>::new();
    content.resize(len as usize - std::mem::size_of::<u32>(), 0);
    stream.read_exact(content.as_mut_slice())?;
    Ok(content)
}

pub fn read_message(stream: &mut TcpStream) -> std::io::Result<(i8, Vec<u8>)> {
    let msgtype = stream.read_i8()?;
    let content = read_body(stream)?;
    Ok((msgtype, content))
}

pub fn read_startup_message(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    read_body(stream)
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
            let mut cursor = Cursor::new(&d[4..]);
            let sess = cursor.read_u32::<NetworkEndian>().unwrap();
            let key = cursor.read_u32::<NetworkEndian>().unwrap();
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

pub fn handle_ssl_request(stream: &mut TcpStream, msg: Vec<u8>) -> std::io::Result<Vec<u8>> {
    match SSLRequest::deserialize(&msg) {
        Some(_) => {
            stream.write_u8('N' as u8)?;
            read_startup_message(stream)
        }
        None => Ok(msg),
    }
}

#[repr(i8)]
pub enum MsgType {
    Query = 'Q' as i8,
    Terminate = 'X' as i8,
    EOF = -1,
}

pub fn write_message<T: Message>(stream: &mut TcpStream, msg: &T) {
    let d = msg.serialize();
    let _ = stream.write_all(d.as_slice());
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

fn read_cstr<'a>(cursor: &mut Cursor<&'a [u8]>) -> std::io::Result<&'a str> {
    let data = cursor.get_ref();
    let idx = find(data, cursor.position() as usize, 0);
    if idx < 0 {
        return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "onho"));
    }
    let cstrdata = &data[cursor.position() as usize..idx as usize];
    if !cstrdata.is_ascii() {
        return Err(std::io::Error::new(ErrorKind::InvalidData, "onho"));
    }
    let retstr = unsafe { std::str::from_utf8_unchecked(cstrdata) };
    cursor.set_position(idx as u64 + 1);
    Ok(retstr)
}

fn write_cstr<T: Write>(writer: &mut T, buf: &[u8]) -> std::io::Result<()> {
    writer.write_all(buf)?;
    writer.write_u8(0)
}

impl StartupMessage<'_> {
    pub fn deserialize(d: &[u8]) -> std::io::Result<StartupMessage<'_>> {
        //log::trace!("StartupMessage deserialize. d={:?}", d);
        let mut cursor = Cursor::new(d);
        let major_ver = cursor.read_u16::<NetworkEndian>()?;
        let minor_ver = cursor.read_u16::<NetworkEndian>()?;
        let mut startup_msg = StartupMessage {
            major_ver,
            minor_ver,
            params: HashMap::new(),
        };

        loop {
            let name = read_cstr(&mut cursor)?;
            if name.is_empty() {
                break;
            }
            let val = read_cstr(&mut cursor)?;
            startup_msg.params.insert(name, val);
        }

        if !startup_msg.params.contains_key(&STARTUP_USER_PARAM) {
            Err(std::io::Error::new(ErrorKind::InvalidData, "no user key"))
        } else {
            Ok(startup_msg)
        }
    }

    pub fn user(&self) -> &str {
        *self.params.get(&STARTUP_USER_PARAM).unwrap()
    }

    pub fn database(&self) -> &str {
        self.params
            .get(&STARTUP_DATABASE_PARAM)
            .map_or_else(|| self.user(), |v| *v)
    }

    pub fn check_client_encoding(&self, expected: &str) -> bool {
        self.params
            .get(&STARTUP_CLIENT_ENCODING)
            .map_or(false, |v| v.eq_ignore_ascii_case(expected))
    }
}

// See https://www.postgresql.org/docs/devel/protocol-error-fields.html for details.
#[derive(Default)]
#[allow(non_snake_case)]
pub struct ErrFields<'a> {
    pub S: Option<&'a str>,
    pub V: Option<&'a str>,
    pub C: Option<&'a str>,
    pub M: Option<&'a str>,
    pub D: Option<&'a str>,
    pub H: Option<&'a str>,
    pub P: Option<&'a str>,
    pub p: Option<&'a str>,
    pub q: Option<&'a str>,
    pub W: Option<&'a str>,
    pub s: Option<&'a str>,
    pub t: Option<&'a str>,
    pub c: Option<&'a str>,
    pub d: Option<&'a str>,
    pub n: Option<&'a str>,
    pub F: Option<&'a str>,
    pub L: Option<&'a str>,
    pub R: Option<&'a str>,
}

fn serialize_errmsg(typ: u8, fields: &ErrFields) -> Vec<u8> {
    let mut writer = Cursor::new(Vec::new());
    writer.seek(SeekFrom::Start(5)).unwrap();
    macro_rules! write_field {
        ($field: ident, $fieldtype: literal) => {
            if let Some(v) = fields.$field {
                writer.write_u8($fieldtype as u8).unwrap();
                write_cstr(&mut writer, v.as_bytes()).unwrap();
            }
        };
    }
    write_field!(S, 'S');
    write_field!(V, 'V');
    write_field!(C, 'C');
    write_field!(M, 'M');
    write_field!(D, 'D');
    write_field!(H, 'H');
    write_field!(P, 'P');
    write_field!(p, 'p');
    write_field!(q, 'q');
    write_field!(W, 'W');
    write_field!(s, 's');
    write_field!(t, 't');
    write_field!(c, 'c');
    write_field!(d, 'd');
    write_field!(n, 'n');
    write_field!(F, 'F');
    write_field!(L, 'L');
    write_field!(R, 'R');
    writer.write_u8(0).unwrap();
    let msglen = writer.position() - 1;
    writer.seek(SeekFrom::Start(0)).unwrap();
    writer.write_u8(typ).unwrap();
    writer.write_u32::<NetworkEndian>(msglen as u32).unwrap();
    writer.into_inner()
}

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
                S: Some(severity),
                C: Some(code),
                M: Some(msg),
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

// use constant generic to refactor NoticeResponse and ErrorResponse
pub struct NoticeResponse<'a> {
    pub fields: ErrFields<'a>,
}

impl<'a> NoticeResponse<'a> {
    pub fn new<'b: 'a, 'c: 'a, 'd: 'a>(
        severity: &'b str,
        code: &'c str,
        msg: &'d str,
    ) -> NoticeResponse<'a> {
        NoticeResponse {
            fields: ErrFields {
                S: Some(severity),
                C: Some(code),
                M: Some(msg),
                ..ErrFields::default()
            },
        }
    }
}

impl Message for NoticeResponse<'_> {
    fn serialize(&self) -> Vec<u8> {
        serialize_errmsg('N' as u8, &self.fields)
    }
}

pub struct AuthenticationOk {}

impl Message for AuthenticationOk {
    fn serialize(&self) -> Vec<u8> {
        let mut writer = Cursor::new(Vec::new());
        writer.write_u8('R' as u8).unwrap();
        writer.write_u32::<NetworkEndian>(8).unwrap();
        writer.write_u32::<NetworkEndian>(0).unwrap();
        writer.into_inner()
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
        let mut writer = Cursor::new(Vec::new());
        writer.write_u8('K' as u8).unwrap();
        writer.write_u32::<NetworkEndian>(12).unwrap();
        writer.write_u32::<NetworkEndian>(self.backendid).unwrap();
        writer.write_u32::<NetworkEndian>(self.key).unwrap();
        writer.into_inner()
    }
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum XactStatus {
    IDLE = 'I' as u8,
    INXACT = 'T' as u8,
    FAILED = 'F' as u8,
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
        let mut writer = Cursor::new(Vec::new());
        writer.write_u8('Z' as u8).unwrap();
        writer.write_u32::<NetworkEndian>(5).unwrap();
        writer.write_u8(self.status as u8).unwrap();
        writer.into_inner()
    }
}

#[derive(Debug)]
pub struct Query<'a> {
    pub query: &'a str,
}

impl Query<'_> {
    pub fn deserialize(d: &[u8]) -> std::io::Result<Query<'_>> {
        if d.is_empty() {
            return Err(std::io::Error::new(ErrorKind::InvalidData, "invalid Query"));
        }
        if let Ok(query) = std::str::from_utf8(&d[..d.len() - 1]) {
            Ok(Query { query })
        } else {
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "from_utf8 failed",
            ))
        }
    }
}

pub struct CommandComplete<'a> {
    pub tag: &'a str,
}

impl Message for CommandComplete<'_> {
    fn serialize(&self) -> Vec<u8> {
        let mut writer = Cursor::new(Vec::new());
        writer.seek(SeekFrom::Start(5)).unwrap();
        write_cstr(&mut writer, self.tag.as_bytes()).unwrap();
        let msglen = writer.position() - 1;
        writer.seek(SeekFrom::Start(0)).unwrap();
        writer.write_u8('C' as u8).unwrap();
        writer.write_u32::<NetworkEndian>(msglen as u32).unwrap();
        writer.into_inner()
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
        let mut writer = Cursor::new(Vec::new());
        writer.seek(SeekFrom::Start(5)).unwrap();
        write_cstr(&mut writer, self.name.as_bytes()).unwrap();
        write_cstr(&mut writer, self.value.as_bytes()).unwrap();
        let msglen = writer.position() - 1;
        writer.seek(SeekFrom::Start(0)).unwrap();
        writer.write_u8('S' as u8).unwrap();
        writer.write_u32::<NetworkEndian>(msglen as u32).unwrap();
        writer.into_inner()
    }
}

pub fn report_guc(
    name: &str,
    gucvals: &guc::GucState,
    gucidx: guc::GucIdx,
    stream: &mut TcpStream,
) {
    let gen = guc::get_guc_generic(gucidx);
    if !gen.should_report() {
        return;
    }
    let value = guc::show(gen, gucvals, gucidx);
    log::trace!("report guc. name={} value={}", name, value);
    let msg = ParameterStatus::new(name, &value);
    write_message(stream, &msg)
}

pub fn report_all_gucs(gucvals: &guc::GucState, stream: &mut TcpStream) {
    for (&name, &gucidx) in guc::GUC_NAMEINFO_MAP.iter() {
        report_guc(name, gucvals, gucidx, stream)
    }
}

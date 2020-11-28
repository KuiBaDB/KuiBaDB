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
use crate::utils::{AttrNumber, TypLen, TypMod, Xid};
use anyhow;
use clap::{App, Arg};
use kuiba::{init_log, Oid, VarcharOid};
use log;
use rand;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Mutex};
use std::thread;

mod catalog;
mod common;
mod guc;
mod parser;
mod protocol;
mod utility;
mod utils;

use utils::SessionState;

struct CancelState {
    key: u32,
    termreq: Arc<AtomicBool>,
}

type CancelMap = HashMap<u32, CancelState>;

fn insert_cancel_map(cancelmap: &Mutex<CancelMap>, sessid: u32, key: u32) -> Arc<AtomicBool> {
    let termreq: Arc<AtomicBool> = Arc::default();
    let cancel_state = CancelState {
        key,
        termreq: termreq.clone(),
    };
    let mut map = cancelmap.lock().unwrap();
    map.insert(sessid, cancel_state);
    termreq
}

fn new_sessid(lastused: &mut u32) -> u32 {
    *lastused += 1;
    *lastused
}

struct SessionDroper<'a> {
    map: &'a Mutex<CancelMap>,
    id: u32,
}

impl SessionDroper<'_> {
    fn new(map: &Mutex<CancelMap>, id: u32) -> SessionDroper<'_> {
        SessionDroper { map, id }
    }
}

impl Drop for SessionDroper<'_> {
    fn drop(&mut self) {
        let mut map = self.map.lock().unwrap();
        map.remove(&self.id).unwrap();
    }
}

fn do_session_fatal(stream: &mut TcpStream, code: &str, msg: &str) {
    log::error!("{}", msg);
    let msg = protocol::ErrorResponse::new("FATAL", code, msg);
    protocol::write_message(stream, &msg);
}

macro_rules! session_fatal {
    ($stream: expr, $code: expr, $fmt: expr, $($arg:tt)*) => {
        let ___session_fatal_errmsg = format!($fmt, $($arg)*);
        do_session_fatal($stream, $code, &___session_fatal_errmsg);
    };
    ($stream: expr, $code: expr, $fmt: expr) => {
        let ___session_fatal_errmsg = format!($fmt);
        do_session_fatal($stream, $code, &___session_fatal_errmsg);
    };
}

fn handle_cancel_request(cancelmap: &Mutex<CancelMap>, cancel_req: protocol::CancelRequest) {
    log::info!("Receive cancel request. req={:?}", cancel_req);
    let mut done = "done";
    {
        let map = cancelmap.lock().unwrap(); // read lock
        match map.get(&cancel_req.sess) {
            None => {
                done = "cannot find the backend";
            }
            Some(CancelState { key, termreq }) => {
                if *key == cancel_req.key {
                    termreq.store(true, Ordering::Relaxed);
                } else {
                    done = "unexpected key";
                }
            }
        }
    }
    log::info!("execute cancel request. done={}", done);
}

fn postgres_main(
    cancelmap: Arc<Mutex<CancelMap>>,
    gucstate: Arc<guc::GucState>,
    mut streamv: TcpStream,
    sessid: u32,
) {
    let stream = &mut streamv;
    log::info!(
        "receive connection. sessid={} remote={}",
        sessid,
        stream
            .peer_addr()
            .map_or("UNKNOWN ADDR".to_string(), |v| v.to_string())
    );

    let startup_msgdata: Vec<u8>;
    let startup_msg = match protocol::read_startup_message(stream) {
        Err(err) => {
            panic!("read_message failed. err={}", err);
        }
        Ok(msgdata) => match protocol::CancelRequest::deserialize(&msgdata) {
            Some(cancel_req) => {
                handle_cancel_request(&cancelmap, cancel_req);
                return;
            }
            None => match protocol::handle_ssl_request(stream, msgdata) {
                Err(err) => {
                    panic!("read_message failed. err={}", err);
                }
                Ok(msgdata) => {
                    startup_msgdata = msgdata;
                    match protocol::StartupMessage::deserialize(&startup_msgdata) {
                        Err(err) => {
                            session_fatal!(
                                stream,
                                protocol::ERRCODE_PROTOCOL_VIOLATION,
                                "unexpected startup msg. err={:?}",
                                err
                            );
                            return;
                        }
                        Ok(v) => v,
                    }
                }
            },
        },
    };
    log::info!("receive startup message. msg={:?}", &startup_msg);

    // validate
    let expected_client_encoding = guc::get_str(&gucstate, guc::CLIENT_ENCODING);
    if !startup_msg.check_client_encoding(expected_client_encoding) {
        session_fatal!(
            stream,
            protocol::ERRCODE_PROTOCOL_VIOLATION,
            "Unsupported client encoding. expected={}",
            expected_client_encoding
        );
        return;
    }
    let reqdb = match catalog::get_database(&startup_msg.database()) {
        Err(err) => {
            session_fatal!(
                stream,
                protocol::ERRCODE_UNDEFINED_DATABASE,
                "database \"{}\" does not exist. err={}",
                &startup_msg.database(),
                err
            );
            return;
        }
        Ok(db) => db.oid,
    };
    log::info!("connect database. dboid={}", reqdb);

    // post-validate
    let sesskey = rand::random();
    let termreq = insert_cancel_map(&cancelmap, sessid, sesskey);
    let _droper = SessionDroper::new(&cancelmap, sessid);
    let mut state = SessionState {
        sessid: sessid,
        reqdb: reqdb,
        termreq: termreq,
        gucstate: gucstate,
        cli: streamv,
        dead: false,
    };
    // post-validate for client-side
    state.write_message(&protocol::AuthenticationOk {});
    protocol::report_all_gucs(&state.gucstate, &mut state.cli);
    state.write_message(&protocol::BackendKeyData::new(sessid, sesskey));
    macro_rules! check_termreq {
        () => {
            if state.received_termreq() {
                return;
            }
        };
    }
    macro_rules! fatal {
        ($code: expr, $fmt: expr, $($arg:tt)*) => {
            let ___session_fatal_errmsg = format!($fmt, $($arg)*);
            state.fatal($code, &___session_fatal_errmsg);
            return;
        };
        ($code: expr, $fmt: expr) => {
            state.fatal($code, $fmt);
            return;
        };
    }

    loop {
        check_termreq!();
        state.write_message(&protocol::ReadyForQuery::new(protocol::XactStatus::IDLE));
        let (msgtype, msgdata) = match state.read_message() {
            Err(err) => {
                fatal!(
                    protocol::ERRCODE_CONNECTION_FAILURE,
                    "read_message failed. err={}",
                    err
                );
            }
            Ok(v) => v,
        };
        check_termreq!();
        if msgtype == protocol::MsgType::EOF as i8 || msgtype == protocol::MsgType::Terminate as i8
        {
            log::info!("end connection");
            return;
        }
        if msgtype != protocol::MsgType::Query as i8 {
            fatal!(
                protocol::ERRCODE_PROTOCOL_VIOLATION,
                "unexpected msg. expected=Q actual={}",
                msgtype
            );
        }
        let query = match protocol::Query::deserialize(&msgdata) {
            Err(err) => {
                fatal!(
                    protocol::ERRCODE_PROTOCOL_VIOLATION,
                    "unexpected query msg. err={:?}",
                    err
                );
            }
            Ok(query) => query,
        };
        exec_simple_query(query.query, &mut state);
        if state.dead {
            return;
        }
    }
}

#[derive(Debug)]
struct ErrCode(&'static str);

impl std::fmt::Display for ErrCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn get_errcode(err: &anyhow::Error) -> &'static str {
    err.downcast_ref::<ErrCode>()
        .map_or(protocol::ERRCODE_INTERNAL_ERROR, |v| v.0)
}

fn write_str_response(resp: &utility::StrResp, session: &mut SessionState) {
    session.write_message(&protocol::RowDescription {
        fields: &[protocol::FieldDesc::new(
            &resp.name,
            VarcharOid.into(),
            TypMod(None),
            TypLen::Var,
        )],
    });
    session.write_message(&protocol::DataRow {
        data: &[Some(&resp.val)],
    });
}

fn write_cmd_complete(tag: &str, session: &mut SessionState) {
    session.write_message(&protocol::CommandComplete { tag });
}

fn exec_utility(stmt: &parser::sem::UtilityStmt, session: &mut SessionState) {
    let resp = match utility::process_utility(stmt, session) {
        Ok(v) => v,
        Err(ref err) => {
            session.on_error(err);
            return;
        }
    };
    if let Some(ref strresp) = resp.resp {
        write_str_response(strresp, session);
    }
    write_cmd_complete(&resp.tag, session);
}

fn exec_simple_query(query: &str, session: &mut SessionState) {
    log::info!("receive query. {}", query.replace("\n", " "));
    let ast = match parser::parse(query) {
        Ok(v) => v,
        Err(err) => {
            session.error(
                protocol::ERRCODE_SYNTAX_ERROR,
                &format!("parse query failed. err={}", err),
            );
            return;
        }
    };
    log::trace!("parse query. ast={:?}", ast);
    if let parser::syn::Stmt::Empty = ast {
        session.write_message(&protocol::EmptyQueryResponse {});
        return;
    }

    let query = match parser::sem::analyze(&ast) {
        Ok(v) => v,
        Err(ref err) => {
            session.on_error(err);
            return;
        }
    };

    match query {
        parser::sem::Stmt::Utility(ref stmt) => exec_utility(stmt, session),
    }
}

fn main() {
    init_log();
    let mut gucstate: Arc<guc::GucState> = Arc::default();
    let cancelmap: Arc<Mutex<CancelMap>> = Arc::default();
    let mut lastused_sessid = 20181218;
    log::set_max_level(gucstate.loglvl);
    let cmdline = App::new("KuiBa(魁拔) Database")
        .version(kuiba::KB_VERSTR)
        .author("盏一 <w@hidva.com>")
        .about("KuiBa Database is another Postgresql written in Rust")
        .arg(
            Arg::with_name("datadir")
                .short("D")
                .long("datadir")
                .required(true)
                .takes_value(true),
        )
        .get_matches();
    let datadir = cmdline
        .value_of("datadir")
        .expect("You must specify the -D invocation option!");
    guc::load_apply_gucs(
        &format!("{}/kuiba.conf", datadir),
        Arc::make_mut(&mut gucstate),
    )
    .unwrap();
    std::env::set_current_dir(datadir).unwrap();
    let port = guc::get_int(&gucstate, guc::PORT) as u16;
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    log::info!("listen. port={}", port);
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let cancelmap = cancelmap.clone();
        let gucstate = gucstate.clone();
        let sessid = new_sessid(&mut lastused_sessid);
        thread::spawn(move || {
            postgres_main(cancelmap, gucstate, stream, sessid);
        });
    }
}

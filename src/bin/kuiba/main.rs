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
use crate::utils::{AttrNumber, TypLen, TypMod};
use access::clog;
use anyhow;
use clap::{App, Arg};
use kuiba::{init_log, Oid, VARCHAROID};
use log;
use rand;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{atomic::AtomicBool, atomic::AtomicU32, atomic::Ordering, Arc, Mutex};
use std::thread;
use thread_local::ThreadLocal;

mod access;
mod catalog;
mod commands;
mod common;
mod datumblock;
mod executor;
mod guc;
mod optimizer;
mod parser;
mod protocol;
mod utility;
mod utils;

#[cfg(test)]
mod test;

use utils::{SessionState, WorkerCache};

pub struct CancelState {
    pub key: u32,
    pub termreq: Arc<AtomicBool>,
}

pub type CancelMap = HashMap<u32, CancelState>;

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

fn postgres_main(global_state: GlobalState, mut streamv: TcpStream, sessid: u32) {
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
                handle_cancel_request(&global_state.cancelmap, cancel_req);
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
    let expected_client_encoding = guc::get_str(&global_state.gucstate, guc::ClientEncoding);
    if !startup_msg.check_client_encoding(expected_client_encoding) {
        session_fatal!(
            stream,
            protocol::ERRCODE_PROTOCOL_VIOLATION,
            "Unsupported client encoding. expected={}",
            expected_client_encoding
        );
        return;
    }
    let (reqdb, dbname) = match catalog::get_database(&startup_msg.database()) {
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
        Ok(db) => (db.oid, db.datname),
    };
    log::info!("connect database. dboid={}", reqdb);
    let metaconn = match sqlite::open(format!("base/{}/meta.db", reqdb)) {
        Err(err) => {
            session_fatal!(
                stream,
                protocol::ERRCODE_INTERNAL_ERROR,
                "connt open metaconn. err={}",
                err
            );
            return;
        }
        Ok(conn) => conn,
    };

    // post-validate
    let sesskey = rand::random();
    let termreq = insert_cancel_map(&global_state.cancelmap, sessid, sesskey);
    let _droper = SessionDroper::new(&global_state.cancelmap, sessid);
    let mut state = SessionState::new(sessid, reqdb, dbname, termreq, metaconn, global_state);
    // post-validate for client-side
    protocol::write_message(stream, &protocol::AuthenticationOk {});
    protocol::report_all_gucs(&state.gucstate, stream);
    protocol::write_message(stream, &protocol::BackendKeyData::new(sessid, sesskey));
    macro_rules! check_termreq {
        () => {
            if state.termreq.load(Ordering::Relaxed) {
                session_fatal!(
                    stream,
                    protocol::ERRCODE_ADMIN_SHUTDOWN,
                    "terminating connection due to administrator command"
                );
                return;
            }
        };
    }

    loop {
        check_termreq!();
        protocol::write_message(
            stream,
            &protocol::ReadyForQuery::new(protocol::XactStatus::IDLE),
        );
        let (msgtype, msgdata) = match protocol::read_message(stream) {
            Err(err) => {
                session_fatal!(
                    stream,
                    protocol::ERRCODE_CONNECTION_FAILURE,
                    "read_message failed. err={}",
                    err
                );
                return;
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
            session_fatal!(
                stream,
                protocol::ERRCODE_PROTOCOL_VIOLATION,
                "unexpected msg. expected=Q actual={}",
                msgtype
            );
            return;
        }
        let query = match protocol::Query::deserialize(&msgdata) {
            Err(err) => {
                session_fatal!(
                    stream,
                    protocol::ERRCODE_PROTOCOL_VIOLATION,
                    "unexpected query msg. err={:?}",
                    err
                );
                return;
            }
            Ok(query) => query,
        };
        exec_simple_query(query.query, &mut state, stream);
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

fn write_str_response(resp: &utility::StrResp, stream: &mut TcpStream) {
    protocol::write_message(
        stream,
        &protocol::RowDescription {
            fields: &[protocol::FieldDesc::new(
                &resp.name,
                VARCHAROID.into(),
                TypMod::none(),
                TypLen::Var,
            )],
        },
    );
    protocol::write_message(
        stream,
        &protocol::DataRow {
            data: &[Some(resp.val.as_bytes())],
        },
    );
}

fn write_cmd_complete(tag: &str, stream: &mut TcpStream) {
    protocol::write_message(stream, &protocol::CommandComplete { tag });
}

fn exec_utility(
    stmt: &parser::sem::UtilityStmt,
    session: &mut SessionState,
    stream: &mut TcpStream,
) -> anyhow::Result<()> {
    let resp = utility::process_utility(stmt, session)?;
    if let Some(ref strresp) = resp.resp {
        write_str_response(strresp, stream);
    }
    write_cmd_complete(&resp.tag, stream);
    Ok(())
}

fn exec_optimizable(
    stmt: &parser::sem::Query,
    session: &mut SessionState,
    stream: &mut TcpStream,
) -> anyhow::Result<()> {
    let plannedstmt = optimizer::planner(session, stmt)?;
    let mut dest_remote = access::DestRemote::new(session, stream);
    executor::exec_select(&plannedstmt, session, &mut dest_remote)?;
    write_cmd_complete(format!("SELECT {}", dest_remote.processed).as_str(), stream);
    Ok(())
}

fn exec_simple_query(query: &str, session: &mut SessionState, stream: &mut TcpStream) {
    log::info!("receive query. {}", query.replace("\n", " "));
    let ast = match parser::parse(query) {
        Ok(v) => v,
        Err(err) => {
            SessionState::error(
                protocol::ERRCODE_SYNTAX_ERROR,
                &format!("parse query failed. err={}", err),
                stream,
            );
            return;
        }
    };
    log::trace!("parse query. ast={:?}", ast);
    if let parser::syn::Stmt::Empty = ast {
        protocol::write_message(stream, &protocol::EmptyQueryResponse {});
        return;
    }

    let query = match parser::sem::kb_analyze(session, &ast) {
        Ok(v) => v,
        Err(ref err) => {
            SessionState::on_error(err, stream);
            return;
        }
    };

    let ret = match query {
        parser::sem::Stmt::Utility(ref stmt) => exec_utility(stmt, session, stream),
        parser::sem::Stmt::Optimizable(ref stmt) => exec_optimizable(stmt, session, stream),
    };
    if let Err(ref err) = ret {
        SessionState::on_error(err, stream);
    }
}

#[derive(Clone)]
pub struct GlobalState {
    pub fmgr_builtins: &'static HashMap<Oid, utils::fmgr::KBFunction>,
    pub clog: &'static clog::GlobalStateExt,
    pub cancelmap: &'static Mutex<CancelMap>,
    pub oid_creator: &'static AtomicU32,
    pub gucstate: Arc<guc::GucState>,
    pub worker_cache: &'static ThreadLocal<RefCell<WorkerCache>>,
}

impl GlobalState {
    fn new(gucstate: Arc<guc::GucState>) -> GlobalState {
        GlobalState {
            fmgr_builtins: Box::leak(Box::new(utils::fmgr::get_fmgr_builtins())),
            cancelmap: Box::leak(Box::new(Mutex::<CancelMap>::default())),
            oid_creator: Box::leak(Box::new(AtomicU32::new(std::u32::MAX))),
            clog: Box::leak(Box::new(clog::init(&gucstate))),
            gucstate: gucstate,
            worker_cache: Box::leak(Box::new(ThreadLocal::new())),
        }
    }

    fn init(datadir: &str) -> GlobalState {
        let mut gucstate = Arc::<guc::GucState>::default();
        std::env::set_current_dir(datadir).unwrap();
        guc::load_apply_gucs("kuiba.conf", Arc::make_mut(&mut gucstate)).unwrap();
        GlobalState::new(gucstate)
    }
}

fn main() {
    init_log();
    let cmdline = App::new("KuiBaDB(魁拔)")
        .version(kuiba::KB_VERSTR)
        .author("盏一 <w@hidva.com>")
        .about("KuiBaDB is another Postgresql written in Rust")
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
    let global_state = GlobalState::init(&datadir);
    let port = guc::get_int(&global_state.gucstate, guc::Port) as u16;
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    log::info!("listen. port={}", port);
    let mut lastused_sessid = 20181218;
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let global_state = global_state.clone();
        let sessid = new_sessid(&mut lastused_sessid);
        thread::spawn(move || {
            postgres_main(global_state, stream, sessid);
        });
    }
}

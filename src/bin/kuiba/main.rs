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
use anyhow;
use clap::{App, Arg};
use kuiba::*;
use log;
use rand;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Mutex};
use std::thread;

mod catalog;
pub mod common;
mod guc;
mod protocol;

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

type Oid = std::num::NonZeroU32;
// type Xid = std::num::NonZeroU64;
type Xid = u64;

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

macro_rules! _session_fatal {
    ($stream: expr, $code: expr, $errmsg: expr) => {{
        log::error!("{}", &$errmsg);
        let msg = protocol::ErrorResponse::new("FATAL", $code, &$errmsg);
        protocol::write_message($stream, &msg);
    }};
}

macro_rules! session_fatal {
    ($stream: expr, $code: expr, $fmt: expr, $($arg:tt)*) => {
        let ___session_fatal_errmsg = format!($fmt, $($arg)*);
        _session_fatal!($stream, $code, ___session_fatal_errmsg);
    };
    ($stream: expr, $code: expr, $fmt: expr) => {
        let ___session_fatal_errmsg = format!($fmt);
        _session_fatal!($stream, $code, ___session_fatal_errmsg);
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
        "receive connection. remote={}",
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

    let sesskey = rand::random();
    let termreq = insert_cancel_map(&cancelmap, sessid, sesskey);
    let _droper = SessionDroper::new(&cancelmap, sessid);
    macro_rules! check_termreq {
        () => {
            if termreq.load(Ordering::Relaxed) {
                session_fatal!(
                    stream,
                    protocol::ERRCODE_ADMIN_SHUTDOWN,
                    "terminating connection due to administrator command"
                );
                return;
            }
        };
    }

    protocol::write_message(stream, &protocol::AuthenticationOk {});
    protocol::report_all_gucs(&gucstate, stream);
    protocol::write_message(stream, &protocol::BackendKeyData::new(sessid, sesskey));

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
        log::info!("receive query. {}", query.query);
        thread::sleep(std::time::Duration::from_millis(3000));
        protocol::write_message(stream, &protocol::CommandComplete { tag: "KUIBA" });
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

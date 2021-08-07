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
use access::csmvcc::{MVCCBufCtx, TabMVCC};
use access::lmgr;
use access::sv;
use access::{ckpt, clog, wal, xact, xact::SessionExt as xact_sess_ext};
use anyhow::Context;
use log;
use rand;
use static_assertions::const_assert;
use std::cmp::Ordering as cmpord;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::debug_assert;
use std::io::{BufReader, BufWriter, Write};
use std::iter::Iterator;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex};
use stderrlog::{ColorChoice, Timestamp};
use utils::sb;
use utils::{err::errcode, AttrNumber, SessionState};

pub mod access;
pub mod catalog;
pub mod commands;
pub mod common;
pub mod datums;
pub mod executor;
pub mod guc;
pub mod optimizer;
pub mod parser;
pub mod protocol;
pub mod utility;
pub mod utils;

#[cfg(test)]
mod test;

pub const KB_MAJOR: i32 = 0;
pub const KB_MINOR: i32 = 0;
pub const KB_PATCH: i32 = 1;
pub const KB_VER: i32 = KB_MAJOR * 100 * 100 + KB_MINOR * 100 + KB_PATCH;
// change the server_version in gucdef.yaml and Cargo.toml TOO!
pub const KB_VERSTR: &str = "0.0.1";
pub const KB_BLCKSZ: usize = 8192;
const_assert!((KB_BLCKSZ & (KB_BLCKSZ - 1)) == 0); // KB_BLCKSZ should be 2^n!

pub fn init_log() {
    stderrlog::new()
        .verbosity(33)
        .timestamp(Timestamp::Microsecond)
        .color(ColorChoice::Never)
        .init()
        .unwrap();
}

mod oids;

pub use oids::*;
pub type FileId = std::num::NonZeroU32;

pub struct SelectedSliceIter<'a, T, IdxIter> {
    d: &'a [T],
    idx_iter: IdxIter,
}

impl<'a, T, IdxIter> Iterator for SelectedSliceIter<'a, T, IdxIter>
where
    IdxIter: Iterator,
    IdxIter::Item: std::convert::Into<usize>,
{
    type Item = (&'a T, usize);

    fn next(&mut self) -> Option<Self::Item> {
        match self.idx_iter.next() {
            None => None,
            Some(idx) => {
                let idx = idx.into();
                Some((&self.d[idx], idx))
            }
        }
    }
}

impl<'a, T, IdxIter> SelectedSliceIter<'a, T, IdxIter>
where
    IdxIter: Iterator,
    IdxIter::Item: std::convert::Into<usize>,
{
    pub fn new(d: &'a [T], idx_iter: IdxIter) -> SelectedSliceIter<'a, T, IdxIter> {
        SelectedSliceIter { d, idx_iter }
    }
}

// It took me 45min to name it, I did my best...
// Progresstracker is used to track what we have done. I try to explain ProgressTracker with the following scenario:
// 1. create a file.
// 2. Start 4 concurrent tasks to write data to [0, 100), [100, 200), [200, 300), [300, 400) respectively.
// 3. Task 3 is done so we know that data in [300, 400) is written.
// 4. Task 0 is done so we know that data in [0, 100) is written, it means that all data before 100 has been written.
// 5. Task 1 is done, it means that all data before 200 has been written.
// 6. Task 2 is done so we know that data in [200, 300) is written, and all data before 400 has been written.
pub struct ProgressTracker {
    // activity on all offset less than inflight[0].1 has been done
    inflight: Vec<(u64, u64)>,
}

impl ProgressTracker {
    pub fn new(d: u64) -> ProgressTracker {
        ProgressTracker {
            inflight: vec![(0, d)],
        }
    }

    // activity on all offset less than has_done() has been done
    fn has_done(&self) -> u64 {
        self.inflight[0].1
    }

    // Return new value of self.d if self.d has changed, return None otherwise.
    pub fn done(&mut self, start: u64, end: u64) -> Option<u64> {
        // debug_assert!(self.inflight.is_sorted());
        if start >= end {
            return None;
        }
        let s_idx = match self.inflight.binary_search_by_key(&start, |&(_, e)| e) {
            Ok(i) | Err(i) => i,
        };
        if s_idx >= self.inflight.len() {
            self.inflight.push((start, end));
            return None;
        }
        // e_idx is the first element whose start is greater than end.
        let e_idx = match self.inflight.binary_search_by(|&(s, _)| {
            if s <= end {
                cmpord::Less
            } else {
                cmpord::Greater
            }
        }) {
            Ok(i) | Err(i) => i,
        };
        debug_assert!(e_idx > 0 && s_idx <= e_idx);
        // v[s_idx - 1].end < start <= v[s_idx].end
        // v[e_idx - 1].start <= end < v[e_idx].start
        if s_idx == e_idx {
            self.inflight.insert(s_idx, (start, end));
            return None;
        }
        let donebefore = self.has_done();
        self.inflight[s_idx].0 = min(start, self.inflight[s_idx].0);
        self.inflight[s_idx].1 = max(end, self.inflight[e_idx - 1].1);
        self.inflight.drain(s_idx + 1..e_idx);
        let doneafter = self.has_done();
        debug_assert!(donebefore <= doneafter);
        if donebefore < doneafter {
            Some(doneafter)
        } else {
            None
        }
    }
}

pub struct Progress {
    curbak: AtomicU64,
    cur: Mutex<u64>,
    cond: Condvar,
}

impl Progress {
    pub fn new(cur: u64) -> Progress {
        Progress {
            cur: Mutex::new(cur),
            curbak: AtomicU64::new(cur),
            cond: Condvar::new(),
        }
    }

    pub fn set(&self, new_progress: u64) {
        {
            let mut cur = self.cur.lock().unwrap();
            *cur = new_progress;
            self.curbak.store(new_progress, Relaxed);
        }
        self.cond.notify_all();
    }

    pub fn get(&self) -> u64 {
        self.curbak.load(Relaxed)
    }

    pub fn wait(&self, progress: u64) {
        if progress <= self.get() {
            return;
        }
        let mut cur = self.cur.lock().unwrap();
        loop {
            if progress <= *cur {
                return;
            }
            cur = self.cond.wait(cur).unwrap();
        }
    }
}

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

type SockReader<'a> = BufReader<&'a TcpStream>;
type SockWriter<'a> = BufWriter<&'a TcpStream>;

fn on_error(level: &str, err: &anyhow::Error, writer: &mut SockWriter) {
    let ec = errcode(err);
    let msg = format!("{:#}", err);
    log::error!("msglvl={} code={} {}", level, ec, &msg);
    // ignore error, just as send_message_to_frontend().
    protocol::write_message(writer, &protocol::ErrorResponse::new(level, ec, &msg));
    let _ = writer.flush();
    return;
}

const NOSSL: [u8; 1] = ['N' as u8];

fn do_postgres_main(
    global_state: GlobalState,
    sockreader: &mut SockReader<'_>,
    sockwriter: &mut SockWriter<'_>,
    sessid: u32,
) -> anyhow::Result<()> {
    log::info!(
        "receive connection. sessid={} remote={}",
        sessid,
        sockwriter
            .get_ref()
            .peer_addr()
            .map_or("UNKNOWN ADDR".to_string(), |v| v.to_string())
    );
    let mut msg = Vec::new();
    protocol::read_startup_message(sockreader, &mut msg)?;
    if let Some(req) = protocol::CancelRequest::deserialize(&msg) {
        handle_cancel_request(&global_state.cancelmap, req);
        return Ok(());
    }
    if let Some(_) = protocol::SSLRequest::deserialize(&msg) {
        sockwriter.write_all(&NOSSL)?;
        sockwriter.flush()?;
        protocol::read_startup_message(sockreader, &mut msg)?;
    }
    let startup = protocol::StartupMessage::deserialize(&msg).with_context(|| {
        errctx!(
            ERRCODE_PROTOCOL_VIOLATION,
            "unexpected startup msg. msg={:?}",
            msg
        )
    })?;
    log::info!("receive startup message. msg={:?}", &startup);
    let expected_client_encoding = guc::get_str(&global_state.gucstate, guc::ClientEncoding);
    // validate
    kbensure!(
        startup.check_client_encoding(expected_client_encoding),
        ERRCODE_PROTOCOL_VIOLATION,
        "Unsupported client encoding. expected={}",
        expected_client_encoding
    );
    // post-validate
    let sesskey = rand::random();
    let termreq = insert_cancel_map(&global_state.cancelmap, sessid, sesskey);
    let _droper = SessionDroper::new(&global_state.cancelmap, sessid);
    let mut state = global_state.new_session(&startup.database(), sessid, termreq)?;
    log::info!("connect database. dboid={}", state.reqdb);
    // post-validate for client-side
    protocol::write_message(sockwriter, &protocol::AuthenticationOk {});
    protocol::report_all_gucs(&state.gucstate, sockwriter);
    protocol::write_message(sockwriter, &protocol::BackendKeyData::new(sessid, sesskey));
    state.init_thread_locals();
    loop {
        state.check_termreq()?;
        protocol::write_message(
            sockwriter,
            &protocol::ReadyForQuery::new(state.xact_status()),
        );
        sockwriter.flush()?;
        let (msgtype, msgdata) = protocol::read_message(sockreader)
            .with_context(|| errctx!(ERRCODE_CONNECTION_FAILURE, "read_message failed"))?;
        state.check_termreq()?;
        if msgtype == protocol::MsgType::EOF as i8 || msgtype == protocol::MsgType::Terminate as i8
        {
            log::info!("end connection");
            return Ok(());
        }
        kbensure!(
            msgtype == protocol::MsgType::Query as i8,
            ERRCODE_PROTOCOL_VIOLATION,
            "unexpected msg. expected=Q actual={}",
            msgtype
        );
        state.update_stmt_startts();
        let query = protocol::Query::deserialize(&msgdata).with_context(|| {
            errctx!(
                ERRCODE_PROTOCOL_VIOLATION,
                "unexpected query msg. msg={:?}",
                msgdata
            )
        })?;
        exec_simple_query(query.query, &mut state, sockwriter);
        if state.dead {
            return Ok(());
        }
    }
}

const SOCK_SEND_BUF_SIZE: usize = 8192;
const SOCK_RECV_BUF_SIZE: usize = 8192;

pub fn postgres_main(global_state: GlobalState, streamv: TcpStream, sessid: u32) {
    let mut sockreader = BufReader::with_capacity(SOCK_RECV_BUF_SIZE, &streamv);
    let mut sockwriter = BufWriter::with_capacity(SOCK_SEND_BUF_SIZE, &streamv);
    let res = do_postgres_main(global_state, &mut sockreader, &mut sockwriter, sessid);
    if let Err(err) = res {
        on_error(protocol::SEVERITY_FATAL, &err, &mut sockwriter);
    }
    let _ = sockwriter.flush(); // ignore error, just as ReadyForQuery
    return;
}

fn write_str_response(resp: &utility::StrResp, stream: &mut SockWriter) {
    protocol::write_message(
        stream,
        &protocol::RowDescription {
            fields: &[protocol::FieldDesc::new(
                &resp.name,
                VARCHAROID.into(),
                -1,
                -1,
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

fn write_cmd_complete(tag: &str, stream: &mut SockWriter) {
    protocol::write_message(stream, &protocol::CommandComplete { tag });
}

fn exec_utility(
    stmt: &parser::sem::UtilityStmt,
    session: &mut SessionState,
    stream: &mut SockWriter,
) -> anyhow::Result<String> {
    let resp = utility::process_utility(stmt, session)?;
    if let Some(ref strresp) = resp.resp {
        write_str_response(strresp, stream);
    }
    return Ok(resp.tag.to_string());
}

fn exec_optimizable(
    stmt: &parser::sem::Query,
    session: &mut SessionState,
    stream: &mut SockWriter,
) -> anyhow::Result<String> {
    let plannedstmt = optimizer::planner(session, stmt)?;
    let mut dest_remote = access::DestRemote::new(stream);
    executor::exec_select(&plannedstmt, session, &mut dest_remote)?;
    return Ok(format!("SELECT {}", dest_remote.processed));
}

fn do_exec_simple_query(
    query: &str,
    session: &mut SessionState,
    stream: &mut SockWriter,
) -> anyhow::Result<()> {
    // We dont want a multi-line log.
    log::info!("receive query. {}", query /* .replace("\n", " ") */);
    session.start_tran_cmd()?;
    let ast = parser::parse(query)
        .with_context(|| errctx!(ERRCODE_SYNTAX_ERROR, "parse query failed"))?;
    kbensure!(
        !session.is_aborted() || ast.is_tran_exit(),
        ERRCODE_IN_FAILED_SQL_TRANSACTION,
        "current transaction is aborted, commands ignored until end of transaction block"
    );
    if let parser::syn::Stmt::Empty = ast {
        session.commit_tran_cmd()?;
        protocol::write_message(stream, &protocol::EmptyQueryResponse {});
        return Ok(());
    }
    let query = parser::sem::kb_analyze(session, &ast)?;
    let cmdtag = match query {
        parser::sem::Stmt::Utility(ref stmt) => exec_utility(stmt, session, stream),
        parser::sem::Stmt::Optimizable(ref stmt) => exec_optimizable(stmt, session, stream),
    }?;
    session.commit_tran_cmd()?;
    write_cmd_complete(&cmdtag, stream);
    return Ok(());
}

fn exec_simple_query(query: &str, session: &mut SessionState, stream: &mut SockWriter) {
    if let Err(ref err) = do_exec_simple_query(query, session, stream) {
        session.on_error(err, stream);
        session.abort_cur_tran().unwrap();
    }
}

fn make_static<T>(v: T) -> &'static T {
    Box::leak(Box::new(v))
}

fn free_static<T>(v: &'static T) {
    unsafe {
        Box::from_raw(v as *const T as *mut T);
    }
}

#[derive(Clone)]
pub struct GlobalState {
    pub fmgr_builtins: &'static HashMap<Oid, utils::fmgr::KBFunction>,
    pub lmgr: &'static lmgr::GlobalStateExt,
    pub clog: &'static clog::GlobalStateExt,
    pub cancelmap: &'static Mutex<CancelMap>,
    pub gucstate: Arc<guc::GucState>,
    pub wal: Option<&'static wal::GlobalStateExt>,
    pub xact: Option<&'static xact::GlobalStateExt>,
    pub oid_creator: Option<&'static AtomicU32>, // nextoid
    pub pending_fileops: &'static ckpt::PendingFileOps,
    pub tabsv: &'static sv::TabSupVer,
    pub tabmvcc: &'static TabMVCC,
}

#[cfg(test)]
const TEST_SESSID: u32 = 0;
const REDO_SESSID: u32 = 1;
pub const LAST_INTERNAL_SESSID: u32 = 20181218;

impl GlobalState {
    fn new(gucstate: Arc<guc::GucState>) -> GlobalState {
        let pending_fileops = make_static(ckpt::PendingFileOps::new());
        let table_sv_cap = guc::get_int(&gucstate, guc::TableSvCap) as usize;
        let tabsv = sb::new_lru_sb(table_sv_cap, sv::SVCommonData::new(pending_fileops, None));
        let tabsv = make_static(tabsv);
        let table_mvcc_cap = guc::get_int(&gucstate, guc::TableMvccCap) as usize;
        let tabmvccctx = MVCCBufCtx::new(pending_fileops, None);
        let tabmvcc = sb::new_lru_sb(table_mvcc_cap, tabmvccctx);
        let tabmvcc = make_static(tabmvcc);
        GlobalState {
            fmgr_builtins: make_static(utils::fmgr::get_fmgr_builtins()),
            cancelmap: make_static(Mutex::<CancelMap>::default()),
            clog: make_static(clog::init(&gucstate, pending_fileops)),
            lmgr: make_static(lmgr::GlobalStateExt::new()),
            gucstate: gucstate,
            oid_creator: None,
            wal: None,
            xact: None,
            pending_fileops,
            tabsv,
            tabmvcc,
        }
    }

    fn renew(&mut self) {
        debug_assert!(self.wal.is_some());
        free_static(self.tabsv);
        let table_sv_cap = guc::get_int(&self.gucstate, guc::TableSvCap) as usize;
        let svdata = sv::SVCommonData::new(self.pending_fileops, self.wal);
        let tabsv = sb::new_lru_sb(table_sv_cap, svdata);
        self.tabsv = make_static(tabsv);

        free_static(self.tabmvcc);
        let table_mvcc_cap = guc::get_int(&self.gucstate, guc::TableMvccCap) as usize;
        let tabmvccctx = MVCCBufCtx::new(self.pending_fileops, self.wal);
        let tabmvcc = sb::new_lru_sb(table_mvcc_cap, tabmvccctx);
        self.tabmvcc = make_static(tabmvcc);
        return;
    }

    fn init(datadir: &str) -> GlobalState {
        std::env::set_current_dir(datadir).unwrap();
        let gucstate = guc::load("kuiba.conf").unwrap();
        GlobalState::new(Arc::new(gucstate))
    }

    fn new_session(
        self,
        dbname: &str,
        sessid: u32,
        termreq: Arc<AtomicBool>,
    ) -> anyhow::Result<SessionState> {
        let reqdb = catalog::get_database(dbname).with_context(|| {
            errctx!(
                ERRCODE_UNDEFINED_DATABASE,
                "database \"{}\" does not exist.",
                dbname
            )
        })?;
        let metaconn = sqlite::open(format!("base/{}/meta.db", reqdb.oid))
            .with_context(|| errctx!(ERRCODE_INTERNAL_ERROR, "connt open metaconn."))?;
        Ok(SessionState::new(
            sessid,
            reqdb.oid,
            reqdb.datname,
            termreq,
            metaconn,
            self,
        ))
    }

    fn internal_session(self, sessid: u32) -> anyhow::Result<SessionState> {
        debug_assert!(sessid <= 20181218);
        self.new_session("kuiba", sessid, Arc::<AtomicBool>::default())
    }
}

#[cfg(test)]
mod progress_test {
    use super::{Progress, ProgressTracker};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use std::{assert, assert_eq, thread};

    #[test]
    fn progress_tracker_test() {
        let mut pt = ProgressTracker::new(33);
        assert_eq!(None, pt.done(33, 33));
        assert_eq!(None, pt.done(44, 77));
        assert_eq!(Some(40), pt.done(33, 40));
        assert_eq!(Some(77), pt.done(40, 44));
        assert_eq!(&[(0, 77)], pt.inflight.as_slice());

        assert_eq!(None, pt.done(100, 200));
        assert_eq!(None, pt.done(200, 300));
        assert_eq!(2, pt.inflight.len());
        assert_eq!(None, pt.done(400, 500));
        assert_eq!(3, pt.inflight.len());

        assert_eq!(None, pt.done(90, 100));
        assert_eq!(3, pt.inflight.len());

        assert_eq!(None, pt.done(80, 85));
        assert_eq!(4, pt.inflight.len());

        assert_eq!(None, pt.done(86, 88));
        assert_eq!(
            &[(0, 77), (80, 85), (86, 88), (90, 300), (400, 500)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(89, 90));
        assert_eq!(
            &[(0, 77), (80, 85), (86, 88), (89, 300), (400, 500)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(88, 89));
        assert_eq!(
            &[(0, 77), (80, 85), (86, 300), (400, 500)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(300, 333));
        assert_eq!(
            &[(0, 77), (80, 85), (86, 333), (400, 500)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(85, 86));
        assert_eq!(&[(0, 77), (80, 333), (400, 500)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(333, 400));
        assert_eq!(&[(0, 77), (80, 500)], pt.inflight.as_slice());
        assert_eq!(Some(500), pt.done(77, 80));
        assert_eq!(&[(0, 500)], pt.inflight.as_slice());
    }

    #[test]
    fn progress_tracker_test2() {
        let mut pt = ProgressTracker::new(33);
        assert_eq!(&[(0, 33)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(77, 88));
        assert_eq!(&[(0, 33), (77, 88)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(88, 99));
        assert_eq!(&[(0, 33), (77, 99)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(200, 203));
        assert_eq!(&[(0, 33), (77, 99), (200, 203)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(102, 105));
        assert_eq!(
            &[(0, 33), (77, 99), (102, 105), (200, 203)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(119, 122));
        assert_eq!(
            &[(0, 33), (77, 99), (102, 105), (119, 122), (200, 203)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(108, 111));
        assert_eq!(
            &[
                (0, 33),
                (77, 99),
                (102, 105),
                (108, 111),
                (119, 122),
                (200, 203)
            ],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(113, 116));
        assert_eq!(
            &[
                (0, 33),
                (77, 99),
                (102, 105),
                (108, 111),
                (113, 116),
                (119, 122),
                (200, 203)
            ],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(107, 177));
        assert_eq!(
            &[(0, 33), (77, 99), (102, 105), (107, 177), (200, 203)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(77, 203));
        assert_eq!(&[(0, 33), (77, 203)], pt.inflight.as_slice());
        assert_eq!(Some(233), pt.done(23, 233));
        assert_eq!(&[(0, 233)], pt.inflight.as_slice());
    }

    #[test]
    fn progress_test() {
        let p = Progress::new(33);
        p.wait(11);
    }

    #[test]
    fn progress_test2() {
        let p = Arc::new(Progress::new(33));
        let p1 = p.clone();
        let t = thread::spawn(move || {
            thread::sleep(Duration::from_secs(7));
            p1.set(55);
            thread::sleep(Duration::from_secs(7));
            p1.set(100);
        });
        let wp = Instant::now();
        p.wait(77);
        let d = wp.elapsed();
        assert!(d >= Duration::from_secs(11));
        t.join().unwrap();
    }
}

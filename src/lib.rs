// Copyright 2020 <盏一 w@hidva.com>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use crate::guc::GucState;
use crate::io::Stream;
use crate::utils::err::errcode;
use anyhow::Context;
use kbio::FdGuard;
use kbio::Uring;
pub use oids::*;
#[cfg(debug_assertions)]
use std::io::Stdout;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufStream};
use tracing::{error, info, trace};
#[cfg(not(debug_assertions))]
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::{DefaultFields, FmtSpan, Format};
use tracing_subscriber::fmt::Formatter;
use tracing_subscriber::reload::Handle;

mod common;
pub mod guc;
mod io;
mod oids;
mod protocol;
mod utils;

fn make_static<T>(v: T) -> &'static T {
    Box::leak(Box::new(v))
}

#[cfg(not(debug_assertions))]
type HandleType = Handle<EnvFilter, Formatter<DefaultFields, Format, NonBlocking>>;

#[cfg(debug_assertions)]
type HandleType = Handle<EnvFilter, Formatter<DefaultFields, Format, fn() -> Stdout>>;

// SAFETY:
// LOG_FILTER_RELOAD_HANDLER is initialized by init_log(), which is called at the entry point of the process.
static mut LOG_FILTER_RELOAD_HANDLER: Option<&'static HandleType> = None;

// change the server_version in gucdef.yaml and Cargo.toml TOO!
pub const KB_VERSTR: &str = "0.0.1";

// called at the entry point of the process.
fn init_log(#[cfg(not(debug_assertions))] lines_limit: usize) {
    let env_filter = EnvFilter::new("trace");

    // We do not need the non_blocking::WorkerGuard because we will abort on panic.
    #[cfg(not(debug_assertions))]
    let (non_blocking, _) = NonBlockingBuilder::default()
        .buffered_lines_limit(lines_limit)
        .lossy(false)
        .finish(std::io::stdout());

    #[cfg(debug_assertions)]
    let builder = tracing_subscriber::fmt()
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(env_filter)
        .with_span_events(FmtSpan::NONE)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_filter_reloading();

    #[cfg(not(debug_assertions))]
    let builder = tracing_subscriber::fmt()
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(env_filter)
        .with_span_events(FmtSpan::NONE)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_writer(non_blocking)
        .with_filter_reloading();

    let handler = builder.reload_handle();
    unsafe { LOG_FILTER_RELOAD_HANDLER = Some(make_static(handler)) };
    builder.init();
    return;
}

// Anything we should do before we enter the async runtime.
pub fn init(_lines_limit: usize, datadir: &str) -> anyhow::Result<GucState> {
    init_log(
        #[cfg(not(debug_assertions))]
        _lines_limit,
    );
    std::env::set_current_dir(datadir)?;
    let gucstate = guc::load("kuiba.conf")?;
    return Ok(gucstate);
}

pub struct Urings {
    iopolls: Vec<Uring>,
    non_iopolls: Vec<Uring>,
    seq: AtomicU64,
    nseq: AtomicU64,
}

impl Urings {
    pub fn new(gucstate: &GucState) -> anyhow::Result<Self> {
        let iopoll_num = guc::get_int(&gucstate, guc::IopollUringNum) as usize;
        let iopoll_depth = guc::get_int(&gucstate, guc::IopollUringDepth) as u32;
        let iopoll_idle = guc::get_int(&gucstate, guc::IopollUringSqThreadIdle) as u32;
        let niopoll_num = guc::get_int(&gucstate, guc::NonIopollUringNum) as usize;
        debug_assert!(niopoll_num > 0);
        let niopoll_depth = guc::get_int(&gucstate, guc::NonIopollUringDepth) as u32;
        let niopoll_idle = guc::get_int(&gucstate, guc::NonIopollUringSqThreadIdle) as u32;
        let mut iourings = Vec::with_capacity(iopoll_num);
        for _i in 0..iopoll_num {
            let iou = Uring::start(iopoll_depth, iopoll_idle, kbio::IORING_SETUP_IOPOLL)?;
            iourings.push(iou);
        }
        let mut nious = Vec::with_capacity(niopoll_num);
        for _i in 0..niopoll_num {
            let iou = Uring::start(niopoll_depth, niopoll_idle, 0)?;
            nious.push(iou);
        }
        return Ok(Urings {
            iopolls: iourings,
            non_iopolls: nious,
            seq: AtomicU64::new(0),
            nseq: AtomicU64::new(0),
        });
    }

    pub fn iopoll(&self) -> &Uring {
        if self.iopolls.is_empty() {
            return self.non_iopoll();
        }
        let seq = self.seq.fetch_add(1, Relaxed);
        let idx = seq as usize % self.iopolls.len();
        return unsafe { self.iopolls.get_unchecked(idx) };
    }

    pub fn non_iopoll(&self) -> &Uring {
        debug_assert!(!self.non_iopolls.is_empty());
        let seq = self.nseq.fetch_add(1, Relaxed);
        let idx = seq as usize % self.non_iopolls.len();
        return unsafe { self.non_iopolls.get_unchecked(idx) };
    }
}

#[derive(Clone)]
pub struct GlobalState {
    pub gucstate: Arc<guc::GucState>,
    pub urings: &'static Urings,
}

impl GlobalState {
    pub fn new(gucstate: Arc<guc::GucState>) -> anyhow::Result<GlobalState> {
        let urings = make_static(Urings::new(&gucstate)?);
        return Ok(GlobalState { gucstate, urings });
    }
}

type KBStream = BufStream<Stream<'static>>;

struct Sock {
    s: KBStream,
    // PostgreSQL put the length field at the header of Message,
    // we can't use one-pass write, so we have to use serbuf as the buffer.
    serbuf: Vec<u8>,
}

impl Sock {
    fn new(s: KBStream) -> Self {
        Self { s, serbuf: vec![] }
    }
}

/*
impl std::ops::Deref for Sock {
    type Target = KBStream;
    fn deref(&self) -> &Self::Target {
        &self.sock
    }
}

impl std::ops::DerefMut for Sock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sock
    }
}
*/

async fn write_cmd_complete(tag: &str, stream: &mut Sock) {
    protocol::write_message(stream, &protocol::CommandComplete { tag }).await;
}

async fn on_error(level: &str, err: &anyhow::Error, writer: &mut Sock) {
    let ec = errcode(err);
    let msg = format!("{:#}", err);
    error!("msglvl={} code={} {}", level, ec, &msg);
    // ignore error, just as send_message_to_frontend().
    protocol::write_message(writer, &protocol::ErrorResponse::new(level, ec, &msg)).await;
    let _ = writer.s.flush().await;
    return;
}

const NOSSL: [u8; 1] = ['N' as u8];

async fn do_postgres_main(gstate: GlobalState, sock: &mut Sock) -> anyhow::Result<()> {
    let mut inmsgbuf = Vec::new();
    protocol::read_startup_message(sock, &mut inmsgbuf).await?;
    if let Some(req) = protocol::CancelRequest::deserialize(&inmsgbuf) {
        trace!("receive CancelRequest. CancelRequest={:?}", req);
        todo!();
    }
    if let Some(_) = protocol::SSLRequest::deserialize(&inmsgbuf) {
        sock.s.write_all(&NOSSL).await?;
        sock.s.flush().await?;
        protocol::read_startup_message(sock, &mut inmsgbuf).await?;
    }
    let startup = protocol::StartupMessage::deserialize(&inmsgbuf).with_context(|| {
        errctx!(
            ERRCODE_PROTOCOL_VIOLATION,
            "unexpected startup msg. msg={:?}",
            inmsgbuf
        )
    })?;
    info!("receive startup message. msg={:?}", &startup);
    let expected_client_encoding = guc::get_str(&gstate.gucstate, guc::ClientEncoding);
    // validate
    kbensure!(
        startup.check_client_encoding(expected_client_encoding),
        ERRCODE_PROTOCOL_VIOLATION,
        "Unsupported client encoding. expected={}",
        expected_client_encoding
    );
    // post-validate
    // let sesskey = rand::random();
    // let termreq = insert_cancel_map(&global_state.cancelmap, sessid, sesskey);
    // let _droper = SessionDroper::new(&global_state.cancelmap, sessid);
    // let mut state = global_state.new_session(&startup.database(), sessid, termreq)?;
    // log::info!("connect database. dboid={}", state.reqdb);
    // post-validate for client-side
    protocol::write_message(sock, &protocol::AuthenticationOk {}).await;
    protocol::report_all_gucs(&gstate.gucstate, sock).await;
    protocol::write_message(sock, &protocol::BackendKeyData::new(0, 0 /* todo! */)).await;
    // state.init_thread_locals();
    loop {
        // state.check_termreq()?;
        protocol::write_message(
            sock,
            &protocol::ReadyForQuery::new(protocol::XactStatus::NotInBlock /* todo!() */),
        )
        .await;
        sock.s.flush().await?;
        let msgtype = protocol::read_message(sock, &mut inmsgbuf)
            .await
            .with_context(|| errctx!(ERRCODE_CONNECTION_FAILURE, "read_message failed"))?;
        // state.check_termreq()?;
        if msgtype == protocol::MsgType::EOF as i8 || msgtype == protocol::MsgType::Terminate as i8
        {
            info!("end connection");
            return Ok(());
        }
        kbensure!(
            msgtype == protocol::MsgType::Query as i8,
            ERRCODE_PROTOCOL_VIOLATION,
            "unexpected msg. expected=Q actual={}",
            msgtype
        );
        // state.update_stmt_startts();
        let query = protocol::Query::deserialize(&inmsgbuf).with_context(|| {
            errctx!(
                ERRCODE_PROTOCOL_VIOLATION,
                "unexpected query msg. msg={:?}",
                inmsgbuf
            )
        })?;
        info!("receive query. query={:?}", query);
        // exec_simple_query(query.query, &mut state, sockwriter);
        write_cmd_complete("HELLOWORLD", sock).await;
        // if state.dead {
        //     return Ok(());
        // }
    }
}

const SOCK_SEND_BUF_SIZE: usize = 8192;
const SOCK_RECV_BUF_SIZE: usize = 8192;

pub async fn postgres_main(gstate: GlobalState, srvfd: i32, cliaddr: SocketAddr) {
    info!("receive connection. remote={}", cliaddr);
    let _guard = FdGuard::new(srvfd);
    let uring = gstate.urings.non_iopoll();
    let mut stream = Sock::new(BufStream::with_capacity(
        SOCK_RECV_BUF_SIZE,
        SOCK_SEND_BUF_SIZE,
        Stream::new(uring, srvfd),
    ));
    let res = do_postgres_main(gstate, &mut stream).await;
    if let Err(err) = res {
        on_error(protocol::SEVERITY_FATAL, &err, &mut stream).await;
    }
    let _ = stream.s.flush().await; // ignore error, just as ReadyForQuery
    return;
}

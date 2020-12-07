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
use crate::catalog::namespace::SessionStateExt as NameSpaceSessionStateExt;
use crate::{get_errcode, guc, protocol, GlobalState, TcpStream};
use kuiba::Oid;
use std::debug_assert;
use std::num::NonZeroU16;
use std::sync::{atomic::AtomicBool, Arc};

pub mod fmgr;

pub struct WorkerState {
    pub fmgr_builtins: &'static fmgr::FmgrBuiltinsMap,
    pub sessid: u32,
    pub reqdb: Oid,
    pub termreq: Arc<AtomicBool>,
    pub gucstate: Arc<guc::GucState>,
}

impl WorkerState {
    pub fn new(session: &SessionState) -> WorkerState {
        WorkerState {
            fmgr_builtins: session.fmgr_builtins,
            sessid: session.sessid,
            reqdb: session.reqdb,
            termreq: session.termreq.clone(),
            gucstate: session.gucstate.clone(),
        }
    }
}

pub struct SessionState {
    pub fmgr_builtins: &'static fmgr::FmgrBuiltinsMap,
    pub sessid: u32,
    pub reqdb: Oid,
    pub db: String,
    pub termreq: Arc<AtomicBool>,
    pub gucstate: Arc<guc::GucState>,
    pub metaconn: sqlite::Connection,

    pub dead: bool,
    pub nsstate: NameSpaceSessionStateExt,
}

impl SessionState {
    pub fn new(
        sessid: u32,
        reqdb: Oid,
        db: String,
        termreq: Arc<AtomicBool>,
        metaconn: sqlite::Connection,
        gstate: GlobalState,
    ) -> Self {
        Self {
            sessid,
            reqdb,
            db,
            termreq,
            fmgr_builtins: gstate.fmgr_builtins,
            gucstate: gstate.gucstate,
            metaconn,
            dead: false,
            nsstate: NameSpaceSessionStateExt::default(),
        }
    }

    pub fn error(code: &str, msg: &str, stream: &mut TcpStream) {
        log::error!("{}", msg);
        protocol::write_message(stream, &protocol::ErrorResponse::new("ERROR", code, msg))
    }

    pub fn on_error(err: &anyhow::Error, stream: &mut TcpStream) {
        // We dont want a multi-line log.
        // and `{:?}` will only display the context saved in err!
        let errmsg = format!("{:#?}", err).replace("\n", " ");
        SessionState::error(
            get_errcode(err),
            &format!("session error. err={}", errmsg),
            stream,
        );
    }
}

// The maximum length of a fixed length type is 2 ^ 15 - 1, so this will never overflow
#[derive(Copy, Clone, Debug)]
pub enum TypLen {
    Var,
    Fixed(NonZeroU16),
}

impl std::convert::From<i16> for TypLen {
    fn from(val: i16) -> Self {
        if val < 0 {
            TypLen::Var
        } else {
            TypLen::Fixed(NonZeroU16::new(val as u16).unwrap())
        }
    }
}

impl std::convert::From<TypLen> for i16 {
    fn from(val: TypLen) -> Self {
        match val {
            TypLen::Var => -1,
            TypLen::Fixed(v) => v.get() as i16,
        }
    }
}

// We don't want to increase the size of the TypMod
#[derive(Clone, Copy, Debug)]
pub struct TypMod(i32);

impl TypMod {
    pub fn none() -> TypMod {
        TypMod(-1)
    }

    pub fn is_none(self) -> bool {
        self.0 < 0
    }

    pub fn get(self) -> i32 {
        debug_assert!(!self.is_none());
        self.0
    }
}

impl std::convert::From<i32> for TypMod {
    fn from(val: i32) -> Self {
        Self(val)
    }
}

impl std::convert::From<TypMod> for i32 {
    fn from(val: TypMod) -> i32 {
        val.0
    }
}

pub type AttrNumber = NonZeroU16;
// type Xid = std::num::NonZeroU64;
pub type Xid = u64;

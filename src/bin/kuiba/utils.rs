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
use crate::{do_session_fatal, get_errcode, guc, protocol, TcpStream};
use kuiba::Oid;
use std::debug_assert;
use std::num::NonZeroU16;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

pub mod fmgr;

pub struct SessionState {
    pub sessid: u32,
    pub reqdb: Oid,
    pub db: String,
    pub termreq: Arc<AtomicBool>,
    pub gucstate: Arc<guc::GucState>,
    pub cli: TcpStream,
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
        gucstate: Arc<guc::GucState>,
        cli: TcpStream,
        metaconn: sqlite::Connection,
    ) -> Self {
        Self {
            sessid,
            reqdb,
            db,
            termreq,
            gucstate,
            cli,
            metaconn,
            dead: false,
            nsstate: NameSpaceSessionStateExt::default(),
        }
    }

    pub fn fatal(&mut self, code: &str, msg: &str) {
        self.dead = true;
        do_session_fatal(&mut self.cli, code, msg);
    }

    pub fn error(&mut self, code: &str, msg: &str) {
        log::error!("{}", msg);
        self.write_message(&protocol::ErrorResponse::new("ERROR", code, msg))
    }

    pub fn received_termreq(&mut self) -> bool {
        if self.termreq.load(Ordering::Relaxed) {
            self.fatal(
                protocol::ERRCODE_ADMIN_SHUTDOWN,
                "terminating connection due to administrator command",
            );
            true
        } else {
            false
        }
    }

    pub fn write_message<T: protocol::Message>(&mut self, msg: &T) {
        protocol::write_message(&mut self.cli, msg)
    }

    pub fn read_message(&mut self) -> std::io::Result<(i8, Vec<u8>)> {
        protocol::read_message(&mut self.cli)
    }

    pub fn on_error(&mut self, err: &anyhow::Error) {
        let errmsg = format!("{:#?}", err).replace("\n", " "); // We dont want a multi-line log.
        self.error(get_errcode(err), &format!("session error. err={}", errmsg));
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

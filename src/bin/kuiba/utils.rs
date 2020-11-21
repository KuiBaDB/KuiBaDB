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
use crate::*;
use std::num::{NonZeroU16, NonZeroU32};

pub struct SessionState {
    pub sessid: u32,
    pub reqdb: Oid,
    pub termreq: Arc<AtomicBool>,
    pub gucstate: Arc<guc::GucState>,
    pub cli: TcpStream,

    pub dead: bool,
    pub pgdialect: PostgreSqlDialect,
}

impl SessionState {
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

#[derive(Copy, Clone)]
pub enum TypLen {
    Var,
    Fixed(NonZeroU16),
}

impl std::convert::From<i16> for TypLen {
    fn from(val: i16) -> Self {
        if val < 0 {
            // should be -1
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
            TypLen::Fixed(v) => v.get() as i16, // will never overflow.
        }
    }
}

#[derive(Clone, Copy)]
pub struct TypMod(pub Option<NonZeroU32>);

impl std::convert::From<i32> for TypMod {
    fn from(val: i32) -> Self {
        if val < 0 {
            TypMod(None)
        } else {
            TypMod(NonZeroU32::new(val as u32 + 1))
        }
    }
}

impl std::convert::From<TypMod> for i32 {
    fn from(val: TypMod) -> i32 {
        match val.0 {
            None => -1,
            Some(v) => (v.get() - 1) as i32,
        }
    }
}

pub type AttrNumber = NonZeroU16;
// type Xid = std::num::NonZeroU64;
pub type Xid = u64;

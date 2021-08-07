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
#[derive(Debug)]
pub struct ErrCtx {
    pub code: &'static str,
    pub msg: String,
}

// crate::on_error() has already output `code`,
// so there is no need to output `code` here.
impl std::fmt::Display for ErrCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

pub fn errcode(err: &anyhow::Error) -> &'static str {
    if let Some(errctx) = err.downcast_ref::<ErrCtx>() {
        errctx.code
    } else {
        crate::protocol::ERRCODE_INTERNAL_ERROR
    }
}

#[macro_export]
macro_rules! errctx {
    ($code:ident, $msg:literal $(,)?) => {
        $crate::utils::err::ErrCtx {
            code: $crate::protocol::$code,
            msg: $msg.to_string(),
        }
    };
    ($code:ident, $fmt:expr, $($arg:tt)*) => {
        $crate::utils::err::ErrCtx {
            code: $crate::protocol::$code,
            msg: format!($fmt, $($arg)*),
        }
    };
}

#[macro_export]
macro_rules! kbanyhow {
    ($code:ident, $msg:literal $(,)?) => {
        anyhow::anyhow!("").context($crate::errctx!($code, $msg))
    };
    ($code:ident, $fmt:expr, $($arg:tt)*) => {
        anyhow::anyhow!("").context($crate::errctx!($code, $fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! kbbail {
    ($code:ident, $msg:literal $(,)?) => {
        return Err($crate::kbanyhow!($code, $msg))
    };
    ($code:ident, $fmt:expr, $($arg:tt)*) => {
        return Err($crate::kbanyhow!($code, $fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! kbensure {
    ($cond:expr, $code:ident, $msg:literal $(,)?) => {
        if !$cond {
            return Err($crate::kbanyhow!($code, $msg))
        }
    };
    ($cond:expr, $code:ident, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err($crate::kbanyhow!($code, $fmt, $($arg)*))
        }
    };
}

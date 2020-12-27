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
use stderrlog::{ColorChoice, Timestamp};

pub const KB_MAJOR: i32 = 0;
pub const KB_MINOR: i32 = 0;
pub const KB_PATCH: i32 = 1;
pub const KB_VER: i32 = KB_MAJOR * 100 * 100 + KB_MINOR * 100 + KB_PATCH;
// change the server_version in gucdef.yaml and Cargo.toml TOO!
pub const KB_VERSTR: &str = "0.0.1";
pub const KB_BLCKSZ: usize = 8192;

pub fn init_log() {
    stderrlog::new()
        .verbosity(33)
        .timestamp(Timestamp::Microsecond)
        .color(ColorChoice::Never)
        .init()
        .unwrap();
}

mod oids;

pub use oids::OidEnum::*;
pub use oids::{Oid, OptOid};

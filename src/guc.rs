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
mod gucdef;
use crate::common;
use crate::LOG_FILTER_RELOAD_HANDLER;
pub use gucdef::B::*;
pub use gucdef::I::*;
pub use gucdef::R::*;
pub use gucdef::S::*;
pub use gucdef::{GucIdx, GucVals, BOOL_GUCS, GUC_NAMEINFO_MAP, INT_GUCS, REAL_GUCS, STR_GUCS};
use tracing::warn;
use tracing_subscriber::filter::EnvFilter;
use yaml_rust::Yaml;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd)]
enum Context {
    Internal,
    KuiBaDB,
    SigHup,
    SuSet,
    UserSet,
}

// bit values in "flags" of a GUC variable
const REPORT: u32 = 0x0010;

#[derive(Clone)]
pub struct GucState {
    pub vals: GucVals,
    // other state derived from guc should be placed here.
    pub base_search_path_valid: bool,
}

impl Default for GucState {
    fn default() -> Self {
        GucState {
            vals: GucVals::default(),
            base_search_path_valid: false,
        }
    }
}

pub struct Generic {
    context: Context,
    flags: u32,
    show: Option<fn(&GucState) -> String>,
}

impl Generic {
    pub fn should_report(&self) -> bool {
        (self.flags & REPORT) != 0
    }
}

pub struct Guc<F> {
    gen: Generic,
    preassign: Option<fn(&mut F, &mut GucState) -> bool>,
}

pub type Bool = Guc<bool>;
pub type Int = Guc<i32>;
pub type Real = Guc<f64>;
pub type Str = Guc<String>;

enum GucMeta {
    B(&'static Bool),
    I(&'static Int),
    R(&'static Real),
    S(&'static Str),
}

#[derive(Debug)]
pub enum Source {
    FILE, // kuiba.conf
    SET,  // SET command
}

pub fn get_gucidx(name: &str) -> Option<GucIdx> {
    GUC_NAMEINFO_MAP.get(name).map(|v| *v)
}

fn get_gucmeta(idx: GucIdx) -> GucMeta {
    match idx {
        GucIdx::I(idx) => GucMeta::I(&INT_GUCS[idx as usize]),
        GucIdx::S(idx) => GucMeta::S(&STR_GUCS[idx as usize]),
        GucIdx::R(idx) => GucMeta::R(&REAL_GUCS[idx as usize]),
        GucIdx::B(idx) => GucMeta::B(&BOOL_GUCS[idx as usize]),
    }
}

pub fn get_guc_generic(idx: GucIdx) -> &'static Generic {
    match get_gucmeta(idx) {
        GucMeta::I(meta) => &meta.gen,
        GucMeta::S(meta) => &meta.gen,
        GucMeta::R(meta) => &meta.gen,
        GucMeta::B(meta) => &meta.gen,
    }
}

fn default_int_show(gucvals: &GucState, idx: gucdef::I) -> String {
    gucvals.vals.int_vals[idx as usize].to_string()
}

fn default_str_show(gucvals: &GucState, idx: gucdef::S) -> String {
    gucvals.vals.str_vals[idx as usize].to_string()
}

fn default_bool_show(gucvals: &GucState, idx: gucdef::B) -> String {
    (if gucvals.vals.bool_vals[idx as usize] {
        "on"
    } else {
        "off"
    })
    .to_string()
}

fn default_real_show(gucvals: &GucState, idx: gucdef::R) -> String {
    gucvals.vals.real_vals[idx as usize].to_string()
}

pub fn show(gen: &Generic, gucvals: &GucState, gucidx: GucIdx) -> String {
    match gen.show {
        Some(f) => f(gucvals),
        None => match gucidx {
            GucIdx::I(idx) => default_int_show(gucvals, idx),
            GucIdx::S(idx) => default_str_show(gucvals, idx),
            GucIdx::R(idx) => default_real_show(gucvals, idx),
            GucIdx::B(idx) => default_bool_show(gucvals, idx),
        },
    }
}

fn preassign(gucgen: &Generic, gucsrc: Source) -> bool {
    let ret = match gucsrc {
        Source::FILE => gucgen.context != Context::Internal,
        Source::SET => gucgen.context >= Context::SuSet,
    };
    if !ret {
        warn!(
            "common preassign returns false. gucctx={:?} gucsrc={:?}",
            gucgen.context, gucsrc
        );
    }
    ret
}

macro_rules! def_apply_fn {
    ($fnname: ident, $valty: ident, $valarr: ident, $metaarr: ident) => {
        fn $fnname(idx: usize, mut val: $valty, gucstate: &mut GucState, gucsrc: Source) {
            let meta = &$metaarr[idx];
            if preassign(&meta.gen, gucsrc)
                && meta.preassign.map_or(true, |v| v(&mut val, gucstate))
            {
                let gucvalptr = &mut gucstate.vals.$valarr[idx];
                *gucvalptr = val;
            }
        }
    };
}

def_apply_fn!(apply_int_guc, i32, int_vals, INT_GUCS);
def_apply_fn!(apply_bool_guc, bool, bool_vals, BOOL_GUCS);
def_apply_fn!(apply_real_guc, f64, real_vals, REAL_GUCS);
def_apply_fn!(apply_str_guc, String, str_vals, STR_GUCS);

pub fn set_int_guc(idx: gucdef::I, val: i32, gucstate: &mut GucState) {
    apply_int_guc(idx as usize, val, gucstate, Source::SET);
}

pub fn set_str_guc(idx: gucdef::S, val: String, gucstate: &mut GucState) {
    apply_str_guc(idx as usize, val, gucstate, Source::SET);
}

pub fn set_real_guc(idx: gucdef::R, val: f64, gucstate: &mut GucState) {
    apply_real_guc(idx as usize, val, gucstate, Source::SET);
}

pub fn set_bool_guc(idx: gucdef::B, val: bool, gucstate: &mut GucState) {
    apply_bool_guc(idx as usize, val, gucstate, Source::SET);
}

fn load_guc(gucstate: &mut GucState, guckey: &str, gucval: &Yaml) {
    macro_rules! apply_guc {
        ($yamlto: ident, $apply: ident, $idx: expr) => {
            if let Some(val) = common::$yamlto(gucval) {
                $apply($idx as usize, val, gucstate, Source::FILE);
            } else {
                warn!(
                    "invalid guc val. expected={}, guckey={:?} gucval={:?}",
                    stringify!(yamlto),
                    guckey,
                    gucval
                );
            }
        };
    }
    match get_gucidx(guckey) {
        Some(GucIdx::B(idx)) => apply_guc!(yaml_try_tobool, apply_bool_guc, idx),
        Some(GucIdx::I(idx)) => apply_guc!(yaml_try_toi32, apply_int_guc, idx),
        Some(GucIdx::S(idx)) => apply_guc!(yaml_try_tostr, apply_str_guc, idx),
        Some(GucIdx::R(idx)) => apply_guc!(yaml_try_tof64, apply_real_guc, idx),
        _ => warn!("Unknown gucname. can't find the guc. guckey={:?}", guckey),
    }
}

pub fn load(inputpath: &str) -> anyhow::Result<GucState> {
    let mut gucstate = GucState::default();
    let yamldata = common::load_yaml(inputpath)?;
    if let Some(yamldoc) = yamldata.first() {
        let yamlhash = yamldoc
            .as_hash()
            .ok_or(anyhow::anyhow!("Unknown yaml. yamldata={:?}", yamldata))?;
        for (gucname, gucval) in yamlhash {
            let guckey = common::yaml_try_tostr(gucname);
            if let Some(guckey) = guckey {
                load_guc(&mut gucstate, &guckey, gucval);
            } else {
                warn!(
                    "Unknown gucname. yaml_try_tostr failed. gucname={:?}",
                    gucname
                );
            }
        }
    }
    return Ok(gucstate);
}

pub fn get_int(gucvals: &GucState, guckey: gucdef::I) -> i32 {
    gucvals.vals.int_vals[guckey as usize]
}

pub fn get_bool(gucvals: &GucState, guckey: gucdef::B) -> bool {
    gucvals.vals.bool_vals[guckey as usize]
}

pub fn get_str(gucvals: &GucState, guckey: gucdef::S) -> &str {
    gucvals.vals.str_vals[guckey as usize].as_str()
}

// ========== hook =======

fn log_min_messages_preassign(val: &mut String, _gucstate: &mut GucState) -> bool {
    let (level, lvlfilter) = match val.as_str() {
        "OFF" => ("off", log::LevelFilter::Off),
        "ERROR" => ("error", log::LevelFilter::Error),
        "WARNING" => ("warn", log::LevelFilter::Warn),
        "INFO" => ("info", log::LevelFilter::Info),
        "DEBUG1" => ("debug", log::LevelFilter::Debug),
        "DEBUG2" => ("trace", log::LevelFilter::Trace),
        _ => return false,
    };
    if let Err(err) = unsafe { LOG_FILTER_RELOAD_HANDLER.unwrap() }.reload(EnvFilter::new(level)) {
        warn!("log_min_messages_preassign failed. val={} err={}", val, err);
        return false;
    }
    log::set_max_level(lvlfilter);
    true
}

fn search_path_preassign(_val: &mut String, gucstate: &mut GucState) -> bool {
    gucstate.base_search_path_valid = false;
    true
}

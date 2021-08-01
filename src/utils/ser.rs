// Copyright 2021 <盏一 w@hidva.com>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::mem::size_of;
pub fn as_bytes<T>(val: &T) -> &[u8] {
    unsafe {
        let ptr = val as *const T as *const u8;
        let d = std::slice::from_raw_parts(ptr, size_of::<T>());
        return d;
    }
}

fn ser<T>(out: &mut Vec<u8>, val: T) {
    out.extend_from_slice(as_bytes(&val));
}

fn ser_at<T>(out: &mut Vec<u8>, idx: usize, val: T) {
    let area = &mut out.as_mut_slice()[idx..idx + size_of::<T>()];
    area.copy_from_slice(as_bytes(&val));
    return;
}

pub trait ToBe {
    // big-endian
    fn to_be(self) -> Self; // or not to be?
}

macro_rules! DefToBe {
    ($t: ty) => {
        impl ToBe for $t {
            fn to_be(self) -> Self {
                // or not to be?
                self.to_be()
            }
        }
    };
}

// NetworkEndian, big-endian
fn ser_be<T: ToBe>(out: &mut Vec<u8>, val: T) {
    ser(out, val.to_be());
}

// NetworkEndian, big-endian
fn ser_be_at<T: ToBe>(out: &mut Vec<u8>, idx: usize, val: T) {
    ser_at(out, idx, val.to_be());
}

pub fn ser_cstr(out: &mut Vec<u8>, buf: &str) {
    out.extend_from_slice(buf.as_bytes());
    out.push(0);
    return;
}

// Generated manually. Rewritten when concat_idents is standardized.
DefToBe!(u32);
DefToBe!(u64);
DefToBe!(i32);
DefToBe!(u16);
DefToBe!(i16);

pub fn ser_i32(out: &mut Vec<u8>, val: i32) {
    ser(out, val);
}

pub fn ser_i32_at(out: &mut Vec<u8>, idx: usize, val: i32) {
    ser_at(out, idx, val);
}

pub fn ser_be_i32(out: &mut Vec<u8>, val: i32) {
    ser_be(out, val);
}

pub fn ser_be_i32_at(out: &mut Vec<u8>, idx: usize, val: i32) {
    ser_be_at(out, idx, val);
}

pub fn ser_u32(out: &mut Vec<u8>, val: u32) {
    ser(out, val);
}

pub fn ser_u32_at(out: &mut Vec<u8>, idx: usize, val: u32) {
    ser_at(out, idx, val);
}

pub fn ser_be_u32(out: &mut Vec<u8>, val: u32) {
    ser_be(out, val);
}

pub fn ser_be_u32_at(out: &mut Vec<u8>, idx: usize, val: u32) {
    ser_be_at(out, idx, val);
}

pub fn ser_u64(out: &mut Vec<u8>, val: u64) {
    ser(out, val);
}

pub fn ser_u64_at(out: &mut Vec<u8>, idx: usize, val: u64) {
    ser_at(out, idx, val);
}

pub fn ser_be_u64(out: &mut Vec<u8>, val: u64) {
    ser_be(out, val);
}

pub fn ser_be_u64_at(out: &mut Vec<u8>, idx: usize, val: u64) {
    ser_be_at(out, idx, val);
}

pub fn ser_u16(out: &mut Vec<u8>, val: u16) {
    ser(out, val);
}

pub fn ser_u16_at(out: &mut Vec<u8>, idx: usize, val: u16) {
    ser_at(out, idx, val);
}

pub fn ser_be_u16(out: &mut Vec<u8>, val: u16) {
    ser_be(out, val);
}

pub fn ser_be_u16_at(out: &mut Vec<u8>, idx: usize, val: u16) {
    ser_be_at(out, idx, val);
}

pub fn ser_i16(out: &mut Vec<u8>, val: i16) {
    ser(out, val);
}

pub fn ser_i16_at(out: &mut Vec<u8>, idx: usize, val: i16) {
    ser_at(out, idx, val);
}

pub fn ser_be_i16(out: &mut Vec<u8>, val: i16) {
    ser_be(out, val);
}

pub fn ser_be_i16_at(out: &mut Vec<u8>, idx: usize, val: i16) {
    ser_be_at(out, idx, val);
}

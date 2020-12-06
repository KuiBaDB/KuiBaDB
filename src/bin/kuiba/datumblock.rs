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

use std::debug_assert;
use std::mem::size_of;

#[derive(Debug)]
pub struct DatumBlockSingle {
    data: Option<Vec<u8>>,
}

macro_rules! def_func {
    ($t: ty, $from: ident, $to: ident) => {
        pub fn $from(val: $t) -> Self {
            DatumBlockSingle::from(val)
        }

        pub fn $to(&self) -> Option<$t> {
            self.to()
        }
    };
}

impl DatumBlockSingle {
    pub fn new_null() -> Self {
        Self { data: None }
    }

    // let i = 20181218;
    // from(i), from(&i) is all valid, and we may not notice this mistake, so do not use from directly.
    fn from<T: Copy>(val: T) -> Self {
        let mut v = Vec::with_capacity(size_of::<T>());
        unsafe {
            std::ptr::write_unaligned(v.as_mut_ptr() as *mut T, val);
            v.set_len(size_of::<T>());
        }
        Self { data: Some(v) }
    }

    fn to<T: Copy>(&self) -> Option<T> {
        if let Some(ref v) = self.data {
            debug_assert!(v.len() == size_of::<T>());
            Some(unsafe { std::ptr::read_unaligned::<T>(v.as_ptr() as *const T) })
        } else {
            None
        }
    }

    def_func!(i8, from_i8, to_i8);
    def_func!(i16, from_i16, to_i16);
    def_func!(i32, from_i32, to_i32);
    def_func!(i64, from_i64, to_i64);
    def_func!(u8, from_u8, to_u8);
    def_func!(u16, from_u16, to_u16);
    def_func!(u32, from_u32, to_u32);
    def_func!(u64, from_u64, to_u64);
    def_func!(f32, from_f32, to_f32);
    def_func!(f64, from_f64, to_f64);

    pub fn new_bytes(val: &[u8]) -> Self {
        let mut v = Vec::with_capacity(val.len());
        v.extend_from_slice(val);
        Self { data: Some(v) }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.data.as_ref().map(|v| v.as_slice())
    }
}

pub enum DatumBlock {
    Single(DatumBlockSingle),
}

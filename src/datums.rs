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

use static_assertions::const_assert;
use std::alloc::Layout;
use std::mem::{align_of, size_of};
use std::ptr::copy_nonoverlapping as memcpy;
use std::ptr::NonNull;
use std::slice;
use std::str::{self, from_utf8};

fn valid_layout(size: usize, align: usize) -> bool {
    // align is the typalign that has been checked at CRAETE TYPE.
    debug_assert!(align.is_power_of_two());
    size <= usize::MAX - (align - 1)
}

// Use std::alloc::Allocator instead.
// Just like Vec and HashMap, out-of-memory is not considered here..
fn doalloc(mut size: usize, align: usize) -> NonNull<u8> {
    if size == 0 {
        // GlobalAlloc: undefined behavior can result if the caller does not ensure
        // that layout has non-zero size.
        size = 2;
    }
    debug_assert!(Layout::from_size_align(size, align).is_ok());
    let ret = unsafe { std::alloc::alloc(Layout::from_size_align_unchecked(size, align)) };
    return NonNull::new(ret).expect("alloc failed");
}

fn alloc(size: usize, align: usize) -> NonNull<u8> {
    assert!(
        valid_layout(size, align),
        "valid_layout failed: size: {}, align: {}",
        size,
        align
    );
    return doalloc(size, align);
}

fn dealloc(ptr: NonNull<u8>, mut size: usize, align: usize) {
    if size == 0 {
        size = 2;
    }
    debug_assert!(Layout::from_size_align(size, align).is_ok());
    unsafe {
        std::alloc::dealloc(ptr.as_ptr(), Layout::from_size_align_unchecked(size, align));
    }
}

fn realloc(ptr: NonNull<u8>, align: usize, mut osize: usize, mut nsize: usize) -> NonNull<u8> {
    if osize == 0 {
        osize = 2;
    }
    if nsize == 0 {
        nsize = 2;
    }
    assert!(
        valid_layout(nsize, align),
        "valid_layout failed: size: {}, align: {}",
        nsize,
        align
    );
    debug_assert!(Layout::from_size_align(osize, align).is_ok());
    let ret = unsafe {
        std::alloc::realloc(
            ptr.as_ptr(),
            Layout::from_size_align_unchecked(osize, align),
            nsize,
        )
    };
    return NonNull::new(ret).expect("realloc failed");
}

const FIXEDLEN_MAX_SIZE: usize = 32767; /* 2 ** 16 - 1 */
// max size for Variable-length datatypes.
const VARLENA_MAX_SIZE: usize = 1073741823; /* 2 ** 30 - 1 */
const NDATUM_MAX: u32 = 2147483647; /* 2 ** 31 - 1 */

const LEN_MASK: u32 = 0b0011_1111_11111111_11111111_11111111;
const SINGLE_NULL_MASK: u32 = 0b0100_0000_00000000_00000000_00000000;
const SINGLE: u32 = 0b1000_0000_00000000_00000000_00000000;

const_assert!(size_of::<*mut u8>() == 8);
const_assert!(size_of::<usize>() == 8);
const_assert!(align_of::<usize>() == 8);
const_assert!(align_of::<f64>() <= align_of::<usize>());
const_assert!(align_of::<i64>() <= align_of::<usize>());
const_assert!(align_of::<i32>() <= align_of::<usize>());
#[derive(Debug)]
pub struct Datums {
    ndatum: u32,
    datums: Option<NonNull<u8>>,
    blob: Option<NonNull<u8>>,
    // datums_cap, blob_cap is only used when datums, blob is valid.
    // We don't want to store *_cap, but we have to do it.
    // To call dealloc(), we have to provide the layout which is used at alloc().
    // We store the *cap to construct the layout.
    datums_cap: usize,
    datums_align: usize,
    blob_cap: usize,
    // blob_align is always align_of::<u8>().
    null: bit_vec::BitVec,
}

impl std::default::Default for Datums {
    fn default() -> Self {
        return Datums::new();
    }
}

fn valid_varchar(v: &[u8]) -> bool {
    return v.len() <= VARLENA_MAX_SIZE && from_utf8(v).is_ok();
}

fn as_varchar(v: &[u8]) -> &str {
    debug_assert!(valid_varchar(v));
    return unsafe { str::from_utf8_unchecked(v) };
}

impl Datums {
    pub fn new() -> Datums {
        Datums {
            ndatum: 0,
            datums: None,
            datums_cap: 0,
            datums_align: 0,
            blob_cap: 0,
            blob: None,
            null: bit_vec::BitVec::new(),
        }
    }

    pub fn new_single_i32(v: i32) -> Datums {
        let mut d = Datums::new();
        d.set_single_i32(v);
        return d;
    }

    pub fn new_single_i64(v: i64) -> Datums {
        let mut d = Datums::new();
        d.set_single_i64(v);
        return d;
    }
    pub fn new_single_f64(v: f64) -> Datums {
        let mut d = Datums::new();
        d.set_single_f64(v);
        return d;
    }

    pub fn new_single_varchar(v: &[u8]) -> Datums {
        debug_assert!(valid_varchar(v));
        let mut d = Datums::new();
        d.set_single_varchar(v);
        return d;
    }

    pub fn new_single_null() -> Datums {
        let mut d = Datums::new();
        d.set_single_null();
        return d;
    }

    pub fn is_single(&self) -> bool {
        return (self.ndatum & SINGLE) != 0;
    }

    pub fn set_single_null(&mut self) {
        self.ndatum = SINGLE_NULL_MASK | SINGLE;
    }

    pub fn set_single_i32(&mut self, v: i32) {
        debug_assert!(self.blob.is_none());
        self.ndatum = SINGLE;
        self.blob_cap = (v as u32) as usize;
        return;
    }

    pub fn set_single_i64(&mut self, v: i64) {
        debug_assert!(self.blob.is_none());
        self.ndatum = SINGLE;
        self.blob_cap = v as u64 as usize;
        return;
    }

    pub fn set_single_f64(&mut self, v: f64) {
        debug_assert!(self.blob.is_none());
        self.ndatum = SINGLE;
        self.blob_cap = v.to_bits() as usize;
        return;
    }

    pub fn set_single_varchar(&mut self, v: &[u8]) {
        debug_assert!(valid_varchar(v));
        self.reserve_blob(v.len());
        self.set_blob_at(0, v);
        self.ndatum = SINGLE | (v.len() as u32);
        return;
    }

    pub fn is_single_null(&self) -> bool {
        debug_assert!(self.is_single());
        return (self.ndatum & SINGLE_NULL_MASK) != 0;
    }

    pub fn get_single_i32(&self) -> i32 {
        debug_assert!(self.is_single());
        debug_assert!(!self.is_single_null());
        return self.blob_cap as u32 as i32;
    }

    pub fn get_single_i64(&self) -> i64 {
        debug_assert!(self.is_single());
        debug_assert!(!self.is_single_null());
        return self.blob_cap as u64 as i64;
    }

    pub fn get_single_f64(&self) -> f64 {
        debug_assert!(self.is_single());
        debug_assert!(!self.is_single_null());
        return f64::from_bits(self.blob_cap as u64);
    }

    pub fn get_single_varchar(&self) -> &str {
        debug_assert!(self.is_single());
        debug_assert!(!self.is_single_null());
        let reallen = (self.ndatum & LEN_MASK) as usize;
        let rawdata = self.get_blob_at(0, reallen);
        return as_varchar(rawdata);
    }

    fn reserve_datums(&mut self, ndatum: usize, typlen: usize, typalign: usize) {
        let datum_cap = typlen * ndatum;
        if let Some(datums) = self.datums {
            if self.datums_cap < datum_cap {
                self.datums = Some(realloc(datums, typalign, self.datums_cap, datum_cap));
                self.datums_cap = datum_cap;
                debug_assert!(self.datums_align == typalign);
            }
        } else {
            self.datums = Some(alloc(datum_cap, typalign));
            self.datums_cap = datum_cap;
            self.datums_align = typalign;
        }
        return;
    }

    pub fn resize_fixedlen(&mut self, ndatum: u32, typlen: usize, typalign: usize) {
        debug_assert!(typlen > 0 && typlen <= FIXEDLEN_MAX_SIZE);
        debug_assert!(ndatum <= NDATUM_MAX);
        debug_assert!(self.blob.is_none());
        self.ndatum = ndatum;
        self.reserve_datums(ndatum as usize, typlen, typalign);
        return;
    }

    fn datums_at<T>(&self, idx: isize) -> *mut T {
        debug_assert!(!self.is_single());
        // Use datums.unwrap_unchecked() instead
        return unsafe { self.datums.unwrap().cast::<T>().as_ptr().offset(idx) };
    }

    fn set_datums_at<T: Copy>(&self, idx: isize, val: T) {
        unsafe {
            *self.datums_at(idx) = val;
        }
    }

    fn get_datums_at<T: Copy>(&self, idx: isize) -> T {
        return unsafe { *self.datums_at(idx) };
    }

    pub fn resize_varlen(&mut self, ndatum: u32) {
        debug_assert!(ndatum <= NDATUM_MAX);
        self.reserve_datums(ndatum as usize + 1, size_of::<usize>(), align_of::<usize>());
        self.ndatum = ndatum;
        self.set_datums_at(0, 0usize);
        return;
    }

    pub fn set_i32_at(&mut self, idx: isize, val: i32) {
        debug_assert!(!self.is_single());
        debug_assert!(idx < self.ndatum as isize);
        debug_assert!(self.blob.is_none());
        self.set_datums_at(idx, val);
        return;
    }

    pub fn get_i32_at(&self, idx: isize) -> i32 {
        debug_assert!(!self.is_single());
        debug_assert!(idx < self.ndatum as isize);
        debug_assert!(self.blob.is_none());
        return self.get_datums_at(idx);
    }

    fn reserve_blob(&mut self, ncap: usize) {
        if let Some(blobp) = self.blob {
            if self.blob_cap < ncap {
                self.blob = Some(realloc(blobp, align_of::<u8>(), self.blob_cap, ncap));
                self.blob_cap = ncap;
            }
        } else {
            self.blob = Some(alloc(ncap, align_of::<u8>()));
            self.blob_cap = ncap;
        }
        return;
    }

    fn blob_at(&self, idx: isize) -> *mut u8 {
        debug_assert!((idx as usize) < self.blob_cap);
        return unsafe {
            // Use datums.unwrap_unchecked() instead
            self.blob.unwrap().as_ptr().offset(idx)
        };
    }

    fn set_blob_at(&mut self, idx: isize, val: &[u8]) {
        debug_assert!((idx as usize) + val.len() <= self.blob_cap);
        unsafe {
            memcpy(val.as_ptr(), self.blob_at(idx), val.len());
        }
    }

    fn get_blob_at(&self, s: usize, e: usize) -> &[u8] {
        debug_assert!(s <= e && e <= self.blob_cap);
        return unsafe { slice::from_raw_parts(self.blob_at(s as isize), e - s) };
    }

    pub fn set_varchar_at(&mut self, idx: isize, val: &[u8]) {
        debug_assert!(!self.is_single());
        debug_assert!(idx < self.ndatum as isize);
        debug_assert!(self.get_datums_at::<usize>(0) == 0usize);
        let blob_used: usize = self.get_datums_at(idx);
        let newcap: usize = blob_used + val.len();
        self.reserve_blob(newcap);
        self.set_blob_at(blob_used as isize, val);
        self.set_datums_at(idx + 1, newcap);
        return;
    }

    pub fn get_varchar_at(&self, idx: isize) -> &str {
        debug_assert!(!self.is_single());
        debug_assert!(idx < self.ndatum as isize);
        debug_assert!(self.get_datums_at::<usize>(0) == 0usize);
        let rawdata = self.get_blob_at(self.get_datums_at(idx), self.get_datums_at(idx + 1));
        return as_varchar(rawdata);
    }

    pub fn try_get_varchar_at(&self, idx: isize) -> Option<&str> {
        if self.is_single() {
            if self.is_single_null() {
                None
            } else {
                Some(self.get_single_varchar())
            }
        } else {
            debug_assert!(idx < self.len() as isize);
            if self.is_null_at(idx) {
                None
            } else {
                Some(self.get_varchar_at(idx))
            }
        }
    }

    pub fn len(&self) -> u32 {
        debug_assert!(!self.is_single());
        self.ndatum
    }

    pub fn is_null_at(&self, idx: isize) -> bool {
        let idx = idx as usize;
        return match self.null.get(idx) {
            None => false,
            Some(v) => v,
        };
    }

    pub fn set_null_all(&mut self) {
        debug_assert!(!self.is_single());
        let rownum = self.len() as usize;
        self.null.reserve(rownum);
        unsafe {
            self.null.set_len(rownum);
        }
        self.null.set_all();
        return;
    }

    pub fn set_notnull_all(&mut self) {
        unsafe {
            self.null.set_len(0);
        }
    }

    pub fn set_null_at(&mut self, idx: isize) {
        let idx = idx as usize;
        if idx < self.null.len() {
            self.null.set(idx, true);
            return;
        }
        if idx > self.null.len() {
            self.null.grow(idx - self.null.len(), false);
        }
        self.null.push(true);
        return;
    }

    pub fn set_null_to(&mut self, other: &Self) {
        self.null = other.null.clone();
    }

    pub fn clonerc(v: &std::rc::Rc<Datums>) -> std::rc::Rc<Datums> {
        v.clone()
    }
}

impl Drop for Datums {
    fn drop(&mut self) {
        if let Some(blobp) = self.blob {
            dealloc(blobp, self.blob_cap, align_of::<u8>());
        }
        if let Some(datumsp) = self.datums {
            dealloc(datumsp, self.datums_cap, self.datums_align);
        }
    }
}

impl Clone for Datums {
    fn clone(&self) -> Self {
        let mut d = Datums {
            ndatum: self.ndatum,
            datums: self.datums,
            blob: self.blob,
            datums_cap: self.datums_cap,
            datums_align: self.datums_align,
            blob_cap: self.blob_cap,
            null: self.null.clone(),
        };
        if self.blob.is_some() {
            d.blob = Some(doalloc(self.blob_cap, align_of::<u8>()));
        }
        if self.datums.is_some() {
            d.datums = Some(doalloc(self.datums_cap, self.datums_align));
        }
        return d;
    }
}

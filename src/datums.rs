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
use crate::access::rel;
use crate::utils::{alloc, dealloc, doalloc, realloc};
use static_assertions::const_assert;
use std::mem::{align_of, size_of, transmute_copy};
use std::ptr::copy_nonoverlapping as memcpy;
use std::ptr::NonNull;
use std::rc::Rc;
use std::slice;
use std::str::{self, from_utf8};

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
    // null.len() is either 0 or ndatum,
    // and if null.len() is ndatum, null.any() must be true.
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

unsafe impl Send for Datums {}

unsafe impl Sync for Datums {}

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

    pub fn new_single_varchar(v: &[u8]) -> Datums {
        debug_assert!(valid_varchar(v));
        let mut d = Datums::new();
        d.set_single_varchar(v);
        return d;
    }

    pub fn new_single_fixedlen<T: Copy>(v: T) -> Datums {
        let mut d = Datums::new();
        d.set_single_fixedlen(v);
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

    pub fn set_single_fixedlen<T: Copy>(&mut self, v: T) {
        let size_t = size_of::<T>();
        debug_assert!(size_t == 1 || size_t == 2 || size_t == 4 || size_t == 8);
        debug_assert!(self.blob.is_none());
        self.ndatum = SINGLE;
        self.blob_cap = unsafe {
            match size_t {
                1 => transmute_copy::<T, u8>(&v) as usize,
                2 => transmute_copy::<T, u16>(&v) as usize,
                4 => transmute_copy::<T, u32>(&v) as usize,
                8 => transmute_copy::<T, u64>(&v) as usize,
                _ => unreachable!("set_single_fixedlen: invalid size_of<T>: {}", size_t),
            }
        };
        return;
    }

    pub fn get_single_fixedlen<T: Copy>(&self) -> T {
        let size_t = size_of::<T>();
        debug_assert!(size_t == 1 || size_t == 2 || size_t == 4 || size_t == 8);
        debug_assert!(self.is_single());
        debug_assert!(!self.is_single_null());
        debug_assert!(self.blob.is_none());
        unsafe {
            match size_t {
                1 => transmute_copy(&(self.blob_cap as u8)),
                2 => transmute_copy(&(self.blob_cap as u16)),
                4 => transmute_copy(&(self.blob_cap as u32)),
                8 => transmute_copy(&(self.blob_cap as u64)),
                _ => unreachable!("get_single_fixedlen: invalid size_of<T>: {}", size_t),
            }
        }
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
        debug_assert!(self.datums.is_some());
        debug_assert!(isize::checked_mul(idx, size_of::<T>() as isize).is_some());
        debug_assert!(idx >= 0);
        debug_assert!((idx + 1) >= 0);
        debug_assert!(isize::checked_mul(idx + 1, size_of::<T>() as isize).is_some());
        debug_assert!(self.datums_cap >= (idx as usize + 1) * size_of::<T>());
        debug_assert!(self.datums_align == align_of::<T>());
        // Use datums.unwrap_unchecked() instead
        return unsafe { self.datums.unwrap().cast::<T>().as_ptr().offset(idx) };
    }

    fn datums_as_bytes(&self, from: isize, len: usize) -> &[u8] {
        debug_assert!(self.datums.is_some());
        debug_assert!(from as usize + len <= self.datums_cap);
        unsafe {
            let ptr = self.datums.unwrap().as_ptr().offset(from);
            slice::from_raw_parts(ptr, len)
        }
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

    pub fn set_fixedlen_at<T: Copy>(&mut self, idx: isize, val: T) {
        debug_assert!(!self.is_single());
        debug_assert!(idx < self.ndatum as isize);
        debug_assert!(self.blob.is_none());
        self.set_datums_at(idx, val);
        return;
    }

    pub fn get_fixedlen_at<T: Copy>(&self, idx: isize) -> T {
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

    pub fn set_empty_at(&mut self, idx: isize) {
        // self.set_varchar_at(idx, "".as_bytes());
        let blob_used: usize = self.get_datums_at(idx);
        self.set_datums_at(idx + 1, blob_used);
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

    pub fn set_len(&mut self, newlen: u32) {
        debug_assert!(!self.is_single());
        debug_assert!(newlen <= NDATUM_MAX);
        self.ndatum = newlen;
        debug_assert!(!self.is_single());
        return;
    }

    // #[cfg(debug_assertions)]
    fn null_is_valid(&self) -> bool {
        if self.null.is_empty() {
            return true;
        }
        return self.len() as usize == self.null.len() && self.null.any();
    }

    pub fn is_null_at(&self, idx: isize) -> bool {
        debug_assert!(self.null_is_valid());
        let idx = idx as usize;
        return match self.null.get(idx) {
            None => false,
            Some(v) => v,
        };
    }

    pub fn set_null_all(&mut self) {
        debug_assert!(self.null_is_valid());
        debug_assert!(!self.is_single());
        let rownum = self.len() as usize;
        self.null.reserve(rownum);
        unsafe {
            self.null.set_len(rownum);
        }
        self.null.set_all();
        debug_assert!(self.null_is_valid());
        return;
    }

    pub fn set_notnull_all(&mut self) {
        debug_assert!(self.null_is_valid());
        unsafe {
            self.null.set_len(0);
        }
        debug_assert!(self.null_is_valid());
    }

    pub fn set_null_at(&mut self, idx: isize) {
        debug_assert!(self.null_is_valid());
        debug_assert!(!self.is_single());
        debug_assert!(idx < self.ndatum as isize);
        debug_assert!(idx >= 0);
        let idx = idx as usize;
        if self.null.is_empty() {
            self.null.grow(self.len() as usize, false);
        }
        debug_assert_eq!(self.null.len(), self.len() as usize);
        self.null.set(idx, true);
        debug_assert!(self.null_is_valid());
        return;
    }

    pub fn set_null_to(&mut self, other: &Self) {
        debug_assert!(self.null_is_valid());
        self.null = other.null.clone();
        debug_assert!(self.null_is_valid());
    }

    pub fn has_null(&self) -> bool {
        if self.is_single() {
            return self.is_single_null();
        }
        debug_assert!(self.null_is_valid());
        return !self.null.is_empty();
    }

    // ret.null = left.null | right.null
    pub fn set_null_or(&mut self, left: &Self, right: &Self) {
        debug_assert!(self.null_is_valid());
        debug_assert!(!left.is_single());
        debug_assert!(!right.is_single());
        debug_assert_eq!(left.len(), right.len());
        debug_assert_eq!(self.len(), left.len());
        if left.null.is_empty() {
            self.set_null_to(right);
            debug_assert!(self.null_is_valid());
            return;
        }
        self.set_null_to(left);
        if right.null.is_empty() {
            debug_assert!(self.null_is_valid());
            return;
        }
        debug_assert_eq!(left.null.len(), right.null.len());
        debug_assert_eq!(left.null.len(), left.len() as usize);
        self.null.or(&right.null);
        debug_assert!(self.null_is_valid());
        return;
    }

    pub fn clonerc(v: &Rc<Datums>) -> Rc<Datums> {
        v.clone()
    }

    pub fn resize_bits<const BLEN: u8>(&mut self, ndatum: u32) {
        debug_assert!(BLEN == 1 || BLEN == 2 || BLEN == 4);
        debug_assert!(self.blob.is_none());
        debug_assert!(ndatum <= NDATUM_MAX);
        let datum_per_byte = (size_of::<u8>() as u8 * 8 / BLEN) as u32;
        let cap = (ndatum + (datum_per_byte - 1)) / datum_per_byte;
        self.reserve_datums(cap as usize, size_of::<u8>(), align_of::<u8>());
        self.ndatum = ndatum;
        return;
    }

    pub fn set_bits_at<const BLEN: u8>(&mut self, idx: isize, v: u8) {
        debug_assert!(BLEN == 1 || BLEN == 2 || BLEN == 4);
        debug_assert!(!self.is_single());
        debug_assert!(idx < self.ndatum as isize);
        debug_assert!(!self.datums.is_none());
        debug_assert!(self.blob.is_none());
        debug_assert_eq!(self.datums_align, align_of::<u8>());
        let datum_per_byte = (size_of::<u8>() as u8 * 8 / BLEN) as isize;
        let byte_idx = idx / datum_per_byte;
        let bits_idx = (idx % datum_per_byte) as usize;
        let bits_shift = bits_idx * BLEN as usize;
        let mut bval: u8 = self.get_datums_at(byte_idx);
        bval &= !((((1 << BLEN) - 1) as u8) << bits_shift);
        bval |= v << bits_shift;
        self.set_datums_at(byte_idx, bval);
        return;
    }

    pub fn get_bits_at<const BLEN: u8>(&self, idx: isize) -> u8 {
        debug_assert!(BLEN == 1 || BLEN == 2 || BLEN == 4);
        debug_assert!(!self.is_single());
        debug_assert!(idx < self.ndatum as isize);
        debug_assert!(!self.datums.is_none());
        debug_assert!(self.blob.is_none());
        debug_assert_eq!(self.datums_align, align_of::<u8>());
        let datum_per_byte = (size_of::<u8>() as u8 * 8 / BLEN) as isize;
        let byte_idx = idx / datum_per_byte;
        let bits_idx = (idx % datum_per_byte) as usize;
        let bits_shift = bits_idx * BLEN as usize;
        let bval: u8 = self.get_datums_at(byte_idx);
        return (bval >> bits_shift) & (((1 << BLEN) - 1) as u8);
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

fn ser_fixed_nonull(
    out: &mut Vec<u8>,
    typlen: i16,
    rownum: u32,
    colidx: usize,
    input: &[(Vec<Rc<Datums>>, u32)],
) {
    debug_assert!(typlen > 0);
    debug_assert!(rownum <= NDATUM_MAX);
    let datumlen = typlen as usize * rownum as usize;
    let cap = size_of::<u32>()  /* ndatum */
        + size_of::<u32>() /* nullbitmap_len */
        + datumlen;
    out.reserve(cap);
    let outlen = out.len();

    out.extend_from_slice(&rownum.to_ne_bytes());
    out.extend_from_slice(&0u32.to_ne_bytes());
    for (cols, colrownum) in input {
        let colrownum = *colrownum;
        let col = &cols[colidx];
        debug_assert!(!col.has_null());
        if col.is_single() {
            let blobdat = col.blob_cap.to_ne_bytes();
            let item = if typlen <= 8 {
                &blobdat[..typlen as usize]
            } else {
                col.datums_as_bytes(0, typlen as usize)
            };
            for _idx in 0..colrownum {
                out.extend_from_slice(item);
            }
        } else {
            debug_assert_eq!(colrownum, col.len());
            let collen = colrownum as usize * typlen as usize;
            out.extend_from_slice(col.datums_as_bytes(0, collen));
        }
    }
    debug_assert_eq!(out.len(), outlen + cap);
    return;
}

pub fn ser(
    out: &mut Vec<u8>,
    rel: &rel::Rel,
    rownum: u32,
    hasnull: &[bool],
    input: &[(Vec<Rc<Datums>>, u32)],
) {
    for colidx in 0..rel.attrs.len() {
        let typlen = rel.attrs[colidx].typ.len;
        if typlen > 0 {
            if hasnull[colidx] {
                unimplemented!();
            } else {
                ser_fixed_nonull(out, typlen, rownum, colidx, input);
            }
        } else {
            unimplemented!();
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn f() {
        const BLEN: u8 = 2;
        let mut d = super::Datums::new();
        d.resize_bits::<BLEN>(16);
        d.set_bits_at::<BLEN>(0, 1);
        assert_eq!(d.get_bits_at::<BLEN>(0), 1);
        d.set_bits_at::<BLEN>(1, 2);
        assert_eq!(d.get_bits_at::<BLEN>(0), 1);
        assert_eq!(d.get_bits_at::<BLEN>(1), 2);
        d.set_bits_at::<BLEN>(2, 3);
        assert_eq!(d.get_bits_at::<BLEN>(0), 1);
        assert_eq!(d.get_bits_at::<BLEN>(1), 2);
        assert_eq!(d.get_bits_at::<BLEN>(2), 3);
        d.set_bits_at::<BLEN>(3, 0);
        assert_eq!(d.get_bits_at::<BLEN>(0), 1);
        assert_eq!(d.get_bits_at::<BLEN>(1), 2);
        assert_eq!(d.get_bits_at::<BLEN>(2), 3);
        assert_eq!(d.get_bits_at::<BLEN>(3), 0);
        d.set_bits_at::<BLEN>(4, 2);
        assert_eq!(d.get_bits_at::<BLEN>(0), 1);
        assert_eq!(d.get_bits_at::<BLEN>(1), 2);
        assert_eq!(d.get_bits_at::<BLEN>(2), 3);
        assert_eq!(d.get_bits_at::<BLEN>(3), 0);
        assert_eq!(d.get_bits_at::<BLEN>(4), 2);
        d.set_bits_at::<BLEN>(5, 1);
        assert_eq!(d.get_bits_at::<BLEN>(0), 1);
        assert_eq!(d.get_bits_at::<BLEN>(1), 2);
        assert_eq!(d.get_bits_at::<BLEN>(2), 3);
        assert_eq!(d.get_bits_at::<BLEN>(3), 0);
        assert_eq!(d.get_bits_at::<BLEN>(4), 2);
        assert_eq!(d.get_bits_at::<BLEN>(5), 1);
        d.set_bits_at::<BLEN>(6, 3);
        assert_eq!(d.get_bits_at::<BLEN>(0), 1);
        assert_eq!(d.get_bits_at::<BLEN>(1), 2);
        assert_eq!(d.get_bits_at::<BLEN>(2), 3);
        assert_eq!(d.get_bits_at::<BLEN>(3), 0);
        assert_eq!(d.get_bits_at::<BLEN>(4), 2);
        assert_eq!(d.get_bits_at::<BLEN>(5), 1);
        assert_eq!(d.get_bits_at::<BLEN>(6), 3);
        d.set_bits_at::<BLEN>(7, 0);
        assert_eq!(d.get_bits_at::<BLEN>(0), 1);
        assert_eq!(d.get_bits_at::<BLEN>(1), 2);
        assert_eq!(d.get_bits_at::<BLEN>(2), 3);
        assert_eq!(d.get_bits_at::<BLEN>(3), 0);
        assert_eq!(d.get_bits_at::<BLEN>(4), 2);
        assert_eq!(d.get_bits_at::<BLEN>(5), 1);
        assert_eq!(d.get_bits_at::<BLEN>(6), 3);
        assert_eq!(d.get_bits_at::<BLEN>(7), 0);
    }
}

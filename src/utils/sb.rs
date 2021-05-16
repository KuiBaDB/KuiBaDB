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

use anyhow::bail;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::{RwLock, TryLockError};

pub trait SBK: Eq + Hash + Copy + std::fmt::Debug {}

impl<K: Eq + Hash + Copy + std::fmt::Debug> SBK for K {}

pub trait Value: std::marker::Sized {
    type Data;
    type K: SBK;
    fn load(k: &Self::K, ctx: &Self::Data) -> anyhow::Result<Self>;
    fn store(&self, k: &Self::K, ctx: &Self::Data, force: bool) -> anyhow::Result<()>;
}

type Map<V, E> = HashMap<<V as Value>::K, Box<Slot<V, E>>>;

pub trait EvictPolicy: std::marker::Sized {
    type Data; // slot data

    // on_create_slot() also means that the slot just be used;
    fn on_create_slot<K: SBK>(&mut self, k: &K) -> Self::Data;
    fn on_use_slot<K: SBK>(&self, k: &K, s: &Self::Data);
    fn on_drop_slot<K: SBK>(&mut self, k: &K, s: &Self::Data);
    // StrategyGetBuffer
    fn evict_cand<'a, V: Value>(
        &self,
        part: &'a Map<V, Self>,
        newk: &V::K,
    ) -> (Option<&'a Slot<V, Self>>, u32);
}

pub struct SharedBuffer<V: Value, E: EvictPolicy> {
    dat: RwLock<(Map<V, E>, E)>,
    valctx: V::Data,
    cap: usize,
}

enum TryGetRet<'a, V: Value, E: EvictPolicy> {
    Found((&'a Slot<V, E>, bool)),
    HasIdleSlot,
    Evict(Option<&'a Slot<V, E>>, u32),
}

pub struct SlotPinGuard<'a, V: Value, E: EvictPolicy>(&'a Slot<V, E>);

impl<'a, V: Value, E: EvictPolicy> Drop for SlotPinGuard<'a, V, E> {
    fn drop(&mut self) {
        self.0.unpin()
    }
}

impl<'a, V: Value, E: EvictPolicy> std::ops::Deref for SlotPinGuard<'a, V, E> {
    type Target = Slot<V, E>;
    fn deref(&self) -> &'a Self::Target {
        self.0
    }
}

impl<V: Value, E: EvictPolicy> Drop for SharedBuffer<V, E> {
    fn drop(&mut self) {
        let dirty_keys = self.get_dirty_keys();
        if !dirty_keys.is_empty() {
            panic!("SharedBuffer::drop(): dirty keys: {:?}", dirty_keys);
        }
    }
}

// TODO: Add prometheus metric and bgwriter thread. bgwriter thread will periodly flush dirty slot.
impl<V: Value, E: EvictPolicy> SharedBuffer<V, E> {
    pub fn new(cap: usize, evict: E, valctx: V::Data) -> Self {
        Self {
            dat: RwLock::new((Map::with_capacity(cap), evict)),
            cap,
            valctx,
        }
    }

    fn pin_slot(&self, v: &Slot<V, E>) -> (&Slot<V, E>, bool) {
        let valid = v.pin();
        return (self.p2r(v as *const _), valid);
    }

    fn use_slot(&self, evict: &E, v: &Slot<V, E>) -> (&Slot<V, E>, bool) {
        evict.on_use_slot(&v.k, &v.evict);
        self.pin_slot(v)
    }

    // Without invoking on_use_slot().
    fn find(&self, k: &V::K) -> Option<SlotPinGuard<V, E>> {
        let dat = self.dat.read().unwrap();
        let map = &dat.0;
        map.get(k).map(|v| {
            let (pinned_v, valid) = self.pin_slot(v);
            debug_assert!(valid);
            SlotPinGuard(pinned_v)
        })
    }

    fn get_dirty_keys(&self) -> Vec<V::K> {
        let mut v = Vec::new();

        let dat = self.dat.read().unwrap();
        let map = &dat.0;
        for (key, slot) in map {
            if dirty(slot.locked_state()) {
                v.push(*key);
            }
        }

        return v;
    }

    fn try_get(&self, k: &V::K) -> TryGetRet<V, E> {
        let dat = self.dat.read().unwrap();
        let partmap = &dat.0;
        let evict = &dat.1;
        if let Some(v) = partmap.get(k) {
            return TryGetRet::Found(self.use_slot(evict, &v));
        }
        if partmap.len() < self.cap {
            return TryGetRet::HasIdleSlot;
        }
        let (slot, state) = evict.evict_cand(&partmap, k);
        return TryGetRet::Evict(slot.map(|v| self.p2r(v as *const _)), state);
    }

    fn create_slot(&self, dat: &mut (Map<V, E>, E), k: &V::K) -> &Slot<V, E> {
        let evict = dat.1.on_create_slot(k);
        let slot = Box::new(Slot::new(k, evict));
        let slotref = self.p2r(slot.as_ref() as *const _);
        dat.0.insert(*k, slot);
        return slotref;
    }

    fn try_create(&self, k: &V::K, evict: Option<&Slot<V, E>>) -> (Option<&Slot<V, E>>, bool) {
        let mut dat = self.dat.write().unwrap();
        if let Some(v) = dat.0.get(k) {
            let ret = self.use_slot(&dat.1, &v);
            return (Some(ret.0), ret.1);
        }
        if dat.0.len() < self.cap {
            return (Some(self.create_slot(&mut dat, k)), false);
        }
        if let Some(evict) = evict {
            if evict.canremove() {
                let evict = dat.0.remove(&evict.k).unwrap();
                dat.1.on_drop_slot(&evict.k, &evict.evict);
                let retslot = self.create_slot(&mut dat, k);
                std::mem::drop(dat);
                // evict.drop() is invoked outside of the lock.
                return (Some(retslot), false);
            }
        }
        return (None, false);
    }

    fn p2r(&self, slot: *const Slot<V, E>) -> &Slot<V, E> {
        unsafe { &*slot }
    }

    // the slot returned should have be pinned.
    fn get(&self, k: &V::K) -> anyhow::Result<(&Slot<V, E>, bool)> {
        loop {
            let evict_slot = match self.try_get(k) {
                TryGetRet::Found(s) => {
                    return Ok(s);
                }
                TryGetRet::Evict(None, _) => {
                    bail!("no unpinned buffers available. key={:?}", k);
                }
                TryGetRet::Evict(Some(s), state) => (Some(s), state),
                TryGetRet::HasIdleSlot => (None, 0),
            };
            // evict_slot is pinned.
            match evict_slot {
                (Some(evict_slot), state) if dirty(state) => {
                    let _d = SlotPinGuard(evict_slot);
                    if !evict_slot.try_flush(&self.valctx)? {
                        continue;
                    }
                    std::mem::forget(_d);
                }
                _ => {}
            };
            if let (Some(s), valid) = self.try_create(k, evict_slot.0) {
                return Ok((s, valid));
            }
        }
    }

    pub fn read(&self, k: &V::K) -> anyhow::Result<SlotPinGuard<V, E>> {
        let (slot, valid) = self.get(k)?;
        if valid {
            return Ok(SlotPinGuard(slot));
        }
        if !slot.startio(true) {
            return Ok(SlotPinGuard(slot));
        }
        match V::load(k, &self.valctx) {
            Ok(v) => {
                slot.setv(v);
                slot.endio(false, SLOT_VALID);
                return Ok(SlotPinGuard(slot));
            }
            Err(e) => {
                slot.abortio();
                slot.unpin();
                return Err(e);
            }
        }
    }

    pub fn flushall(&self, force: bool) -> anyhow::Result<()> {
        let dirty_keys = self.get_dirty_keys();
        for dirty_key in &dirty_keys {
            if let Some(pinned_slot) = self.find(dirty_key) {
                if !dirty(pinned_slot.locked_state()) {
                    continue;
                }
                if force {
                    pinned_slot.flush(&self.valctx)?;
                } else {
                    pinned_slot.try_flush(&self.valctx)?;
                }
            }
        }
        return Ok(());
    }
}

const REFCOUNT_ONE: u32 = 1;
const REFCOUNT_MASK: u32 = (1 << 18) - 1;
const SLOT_LOCKED: u32 = 1 << 22;
const SLOT_DIRTY: u32 = 1 << 23;
// SLOT_VALID remains permanently after set.
const SLOT_VALID: u32 = 1 << 24;
const SLOT_IO_INPROGRESS: u32 = 1 << 26;
const SLOT_IO_ERR: u32 = 1 << 27;
const SLOT_JUST_DIRTIED: u32 = 1 << 28;

fn biton(state: u32, bit: u32) -> bool {
    (state & bit) != 0
}

fn dirty(state: u32) -> bool {
    biton(state, SLOT_DIRTY)
}

fn just_dirtied(state: u32) -> bool {
    biton(state, SLOT_JUST_DIRTIED)
}

fn rc(state: u32) -> u32 {
    state & REFCOUNT_MASK
}

fn ioerr(state: u32) -> bool {
    biton(state, SLOT_IO_ERR)
}

fn io_in_progress(state: u32) -> bool {
    biton(state, SLOT_IO_INPROGRESS)
}

fn locked(state: u32) -> bool {
    biton(state, SLOT_LOCKED)
}

fn valid(state: u32) -> bool {
    biton(state, SLOT_VALID)
}

pub struct Slot<V: Value, E: EvictPolicy> {
    k: V::K,
    v: RwLock<Option<V>>, // Use MaybeUninit when assume_init_ref is stable.
    state: AtomicU32,
    evict: E::Data,
}

struct SlotLockGuard<'a, V: Value, E: EvictPolicy> {
    slot: &'a Slot<V, E>,
    state: u32,
}

impl<'a, V: Value, E: EvictPolicy> Drop for SlotLockGuard<'a, V, E> {
    fn drop(&mut self) {
        self.slot.unlock(self.state);
    }
}

impl<V: Value, E: EvictPolicy> Slot<V, E> {
    fn new(k: &V::K, evict: E::Data) -> Self {
        Self {
            k: *k,
            v: RwLock::new(None),
            state: AtomicU32::new(REFCOUNT_ONE), // pinned
            evict,
        }
    }

    fn setv(&self, v: V) {
        *self.v.write().unwrap() = Some(v);
        return;
    }

    fn get_state(&self) -> u32 {
        self.state.load(Relaxed)
    }

    fn set_state(&self, oldstate: u32, state: u32) -> Result<u32, u32> {
        self.state
            .compare_exchange_weak(oldstate, state, Relaxed, Relaxed)
    }

    fn atomic_change(&self, change: impl Fn(u32) -> u32) -> u32 {
        let mut old_state = self.get_state();
        loop {
            if locked(old_state) {
                old_state = self.wait();
            }
            let state = change(old_state);
            match self.set_state(old_state, state) {
                Ok(s) => {
                    return s;
                }
                Err(s) => {
                    old_state = s;
                }
            }
        }
    }

    // PinBuffer
    fn pin(&self) -> bool {
        return valid(self.atomic_change(|v| v + REFCOUNT_ONE));
    }

    fn unpin(&self) {
        self.atomic_change(|v| v - REFCOUNT_ONE);
        return;
    }

    // False means the slot already is dirty.
    pub fn mark_dirty(&self) -> bool {
        return !dirty(self.atomic_change(|v| v | (SLOT_DIRTY | SLOT_JUST_DIRTIED)));
    }

    fn pin_locked(&self, mut g: SlotLockGuard<V, E>) -> u32 {
        g.state += REFCOUNT_ONE;
        return g.state;
    }

    // lock()/unlock() does not use acquire/release semantics,
    // so do not use it for synchronization
    fn lock(&self) -> SlotLockGuard<V, E> {
        loop {
            let state = self.state.fetch_or(SLOT_LOCKED, Relaxed);
            if locked(state) {
                std::hint::spin_loop(); // Use a more adaptive approach.
            } else {
                return SlotLockGuard {
                    slot: self,
                    state: state | SLOT_LOCKED,
                };
            }
        }
    }

    fn wait(&self) -> u32 {
        let mut state = self.get_state();
        while locked(state) {
            std::hint::spin_loop(); // Use a more adaptive approach.
            state = self.get_state();
        }
        return state;
    }

    fn unlock(&self, state: u32) {
        self.state.store(state & (!SLOT_LOCKED), Relaxed);
    }

    fn clear_just_dirtied(&self) {
        let mut guard = self.lock();
        guard.state &= !SLOT_JUST_DIRTIED;
        return;
    }

    // FlushBuffer, flush current slot, we have the read lock on self.v, and v is always self.v.
    // If do_flush() returns Err, everything is unchanged except SLOT_IO_ERR is set.
    // If do_flush() returns Ok, it means that we have successfully flushed current slot,
    // and the dirty flag should have been cleared.
    // The slot may be still dirty after do_flush() return, others may modify the slot in parallel
    // when they have the read lock, just like MarkBufferDirtyHint() in PostgreSQL.
    fn do_flush(&self, v: &V, valctx: &V::Data, force: bool) -> anyhow::Result<()> {
        if !self.startio(false) {
            return Ok(());
        }
        self.clear_just_dirtied();
        match v.store(&self.k, valctx, force) {
            Ok(_) => {
                self.endio(true, 0);
                return Ok(());
            }
            Err(e) => {
                self.abortio();
                return Err(e);
            }
        }
    }

    fn try_flush(&self, valctx: &V::Data) -> anyhow::Result<bool> {
        match self.v.try_read() {
            Ok(gurad) => {
                self.do_flush(gurad.as_ref().unwrap(), valctx, false)?;
                return Ok(true);
            }
            Err(TryLockError::Poisoned(_)) => {
                panic!("Slot::try_flush: TryLockError::Poisoned. key={:?}", &self.k);
            }
            Err(TryLockError::WouldBlock) => {
                return Ok(false);
            }
        }
    }

    fn flush(&self, valctx: &V::Data) -> anyhow::Result<()> {
        let v = self.v.read().unwrap();
        self.do_flush(v.as_ref().unwrap(), valctx, true)
    }

    fn canremove(&self) -> bool {
        let state = self.locked_state();
        return rc(state) == 1 && !dirty(state);
    }

    fn locked_state(&self) -> u32 {
        self.lock().state
    }

    fn waitio(&self) {
        loop {
            if !io_in_progress(self.locked_state()) {
                return;
            }
            std::thread::yield_now(); // Use Semaphore?
        }
    }

    fn startio(&self, forinput: bool) -> bool {
        let mut guard = loop {
            {
                let guard = self.lock();
                if !io_in_progress(guard.state) {
                    break guard;
                }
            }
            self.waitio();
        };

        let canret = if forinput {
            valid(guard.state)
        } else {
            !dirty(guard.state)
        };
        if canret {
            return false;
        }

        guard.state |= SLOT_IO_INPROGRESS;
        return true;
    }

    fn abortio(&self) {
        if ioerr(self.locked_state()) {
            log::warn!(
                "SharedBuffer::Value: multiple failures happened when doing load/store. key={:?}",
                &self.k,
            );
        }
        self.endio(false, SLOT_IO_ERR);
    }

    fn endio(&self, clear_dirty: bool, set_flag_bits: u32) {
        let mut guard = self.lock();
        guard.state &= !(SLOT_IO_INPROGRESS | SLOT_IO_ERR);
        if clear_dirty && !just_dirtied(guard.state) {
            guard.state &= !SLOT_DIRTY;
        }
        guard.state |= set_flag_bits;
        return;
    }
}

pub struct FIFOPolicy {
    no: u32, // next number.
}

impl FIFOPolicy {
    fn new() -> Self {
        Self { no: 0 }
    }
}

impl EvictPolicy for FIFOPolicy {
    type Data = u32;

    fn on_create_slot<K: SBK>(&mut self, _k: &K) -> Self::Data {
        let v = self.no;
        self.no += 1;
        v
    }
    fn on_use_slot<K: SBK>(&self, _k: &K, _s: &Self::Data) {}
    fn on_drop_slot<K: SBK>(&mut self, _k: &K, _s: &Self::Data) {}
    // StrategyGetBuffer
    fn evict_cand<'a, V: Value>(
        &self,
        part: &'a Map<V, Self>,
        _newk: &V::K,
    ) -> (Option<&'a Slot<V, Self>>, u32) {
        let mut minslot: Option<SlotPinGuard<'a, V, Self>> = None;
        let mut minslotstate = 0;
        for (_, slot) in part {
            if let Some(ref mins) = minslot {
                if mins.evict <= slot.evict {
                    continue;
                }
            }
            let lguard = slot.lock();
            if rc(lguard.state) > 0 {
                continue;
            }
            minslotstate = slot.pin_locked(lguard);
            minslot = Some(SlotPinGuard(slot));
        }
        if let Some(minslot) = minslot {
            let slot = minslot.0;
            std::mem::forget(minslot);
            return (Some(slot), minslotstate);
        }
        return (None, 0);
    }
}

pub fn new_fifo_sb<V: Value>(cap: usize, valctx: V::Data) -> SharedBuffer<V, FIFOPolicy> {
    SharedBuffer::new(cap, FIFOPolicy::new(), valctx)
}

// TODO: Implement the real LRUPolicy based on the method in slru.rs.
pub type LRUPolicy = FIFOPolicy;
pub fn new_lru_sb<V: Value>(cap: usize, valctx: V::Data) -> SharedBuffer<V, LRUPolicy> {
    SharedBuffer::new(cap, LRUPolicy::new(), valctx)
}

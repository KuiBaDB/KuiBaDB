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

// WHY lmgr is placed in src/backend/storage/? Is lmgr a storage?
use crate::catalog::is_shared_rel;
use crate::utils::SessionState;
use crate::{Oid, NSRELID};
use std::collections::HashMap;
use std::sync::{Condvar, Mutex, RwLock};

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum LockTag {
    Relation {
        dboid: Option<Oid>,
        reloid: Oid,
    },
    Object {
        dboid: Oid,
        clsoid: Oid,
        objoid: Oid,
    },
}

type LockMask = u32;
#[derive(Clone, Copy, Eq, Hash, PartialEq, Debug)]
#[repr(u32)]
pub enum LockMode {
    NoLock = 0,
    AccessShare = 1,
    RowShare = 2,
    RowExclusive = 3,
    ShareUpdateExclusive = 4,
    Share = 5,
    ShareRowExclusive = 6,
    Exclusive = 7,
    AccessExclusive = 8,
}
const LOCKMODESNUM: usize = LockMode::AccessExclusive as usize + 1;

const fn lockbit_on(l: LockMode) -> LockMask {
    1 << (l as u32)
}

const fn lockbit_off(l: LockMode) -> LockMask {
    !lockbit_on(l)
}

const LOCKCONFLICT: [LockMask; LOCKMODESNUM] = [
    0,
    /* LockMode::AccessShare */
    lockbit_on(LockMode::AccessExclusive),
    /* LockMode::RowShare */
    lockbit_on(LockMode::Exclusive) | lockbit_on(LockMode::AccessExclusive),
    /* LockMode::RowExclusive */
    lockbit_on(LockMode::Share)
        | lockbit_on(LockMode::ShareRowExclusive)
        | lockbit_on(LockMode::Exclusive)
        | lockbit_on(LockMode::AccessExclusive),
    /* LockMode::ShareUpdateExclusive */
    lockbit_on(LockMode::ShareUpdateExclusive)
        | lockbit_on(LockMode::Share)
        | lockbit_on(LockMode::ShareRowExclusive)
        | lockbit_on(LockMode::Exclusive)
        | lockbit_on(LockMode::AccessExclusive),
    /* LockMode::Share */
    lockbit_on(LockMode::RowExclusive)
        | lockbit_on(LockMode::ShareUpdateExclusive)
        | lockbit_on(LockMode::ShareRowExclusive)
        | lockbit_on(LockMode::Exclusive)
        | lockbit_on(LockMode::AccessExclusive),
    /* LockMode::ShareRowExclusive */
    lockbit_on(LockMode::RowExclusive)
        | lockbit_on(LockMode::ShareUpdateExclusive)
        | lockbit_on(LockMode::Share)
        | lockbit_on(LockMode::ShareRowExclusive)
        | lockbit_on(LockMode::Exclusive)
        | lockbit_on(LockMode::AccessExclusive),
    /* LockMode::Exclusive */
    lockbit_on(LockMode::RowShare)
        | lockbit_on(LockMode::RowExclusive)
        | lockbit_on(LockMode::ShareUpdateExclusive)
        | lockbit_on(LockMode::Share)
        | lockbit_on(LockMode::ShareRowExclusive)
        | lockbit_on(LockMode::Exclusive)
        | lockbit_on(LockMode::AccessExclusive),
    /* LockMode::AccessExclusive */
    lockbit_on(LockMode::AccessShare)
        | lockbit_on(LockMode::RowShare)
        | lockbit_on(LockMode::RowExclusive)
        | lockbit_on(LockMode::ShareUpdateExclusive)
        | lockbit_on(LockMode::Share)
        | lockbit_on(LockMode::ShareRowExclusive)
        | lockbit_on(LockMode::Exclusive)
        | lockbit_on(LockMode::AccessExclusive),
];

const fn conflict_modes(mode: LockMode) -> LockMask {
    LOCKCONFLICT[mode as usize]
}

// const LOCKMODENAMES: [&'static str; LOCKMODESNUM] = [
//     "INVALID",
//     "AccessShareLock",
//     "RowShareLock",
//     "RowExclusiveLock",
//     "ShareUpdateExclusiveLock",
//     "ShareLock",
//     "ShareRowExclusiveLock",
//     "ExclusiveLock",
//     "AccessExclusiveLock"
// ];

#[derive(Default)]
struct Lock {
    grant: LockMask,
    wait: LockMask,
    req: [u32; LOCKMODESNUM],
    nreq: u32,
    granted: [u32; LOCKMODESNUM],
    ngranted: u32,
}

impl Lock {
    fn req(&mut self, mode: LockMode) {
        self.req[mode as usize] += 1;
        self.nreq += 1;
    }

    fn grant(&mut self, mode: LockMode) {
        let modeidx = mode as usize;
        self.granted[modeidx] += 1;
        self.ngranted += 1;
        self.grant |= lockbit_on(mode);
        if self.granted[modeidx] >= self.req[modeidx] {
            self.wait &= lockbit_off(mode);
        }
        debug_assert!(self.ngranted > 0 && self.granted[modeidx] > 0);
        debug_assert!(self.ngranted <= self.nreq);
    }

    fn ungrant(&mut self, mode: LockMode) -> bool /* wakeup? */ {
        let modeidx = mode as usize;
        debug_assert!(self.ngranted > 0 && self.granted[modeidx] > 0);
        debug_assert!(self.ngranted <= self.nreq);
        self.ngranted -= 1;
        self.granted[modeidx] -= 1;
        self.req[modeidx] -= 1;
        self.nreq -= 1;
        if self.granted[modeidx] == 0 {
            self.grant &= lockbit_off(mode);
        }
        return (conflict_modes(mode) & self.wait) != 0;
    }

    fn new(mode: LockMode) -> Self {
        let mut s = Self::default();
        s.req(mode);
        s
    }
}

struct LockState {
    lock: Mutex<Lock>,
    cv: Condvar,
}

pub struct GlobalStateExt {
    lm: RwLock<HashMap<LockTag, Box<LockState>>>,
}

impl GlobalStateExt {
    pub fn new() -> Self {
        Self {
            lm: RwLock::new(HashMap::new()),
        }
    }

    fn p2r(&self, l: *const LockState) -> &LockState {
        unsafe { &*l }
    }

    fn pin(&self, l: &LockState, mode: LockMode) -> &LockState {
        {
            let mut lock = l.lock.lock().unwrap();
            lock.req(mode);
        }
        return self.p2r(l as *const _);
    }

    // SetupLockInTable
    fn setup_lock(&self, tag: &LockTag, mode: LockMode) -> &LockState {
        {
            let hm = self.lm.read().unwrap();
            if let Some(v) = hm.get(tag) {
                return self.pin(v, mode);
            }
        }

        let lock = Box::new(LockState {
            lock: Mutex::new(Lock::new(mode)),
            cv: Condvar::new(),
        });
        let lockp = lock.as_ref() as *const _;
        let mut hm = self.lm.write().unwrap();
        if let Some(v) = hm.get(tag) {
            return self.pin(v, mode);
        }

        hm.insert(*tag, lock);
        return self.p2r(lockp);
    }
}

#[derive(Copy, Clone)]
struct LocalLock<'a> {
    n: u64,
    // Only use lock if n > 0.
    lock: &'a LockState,
}

pub struct SessionStateExt<'a> {
    lm: HashMap<LockTag, [LocalLock<'a>; LOCKMODESNUM]>,
}

impl<'a> SessionStateExt<'a> {
    pub fn new() -> Self {
        Self { lm: HashMap::new() }
    }
}

pub trait SessionExt {
    fn lock_acquire(&mut self, tag: &LockTag, mode: LockMode);
    fn lock_release(&mut self, tag: &LockTag, mode: LockMode);
    fn lock_release_all(&mut self);
    // LockDatabaseObject
    fn lock_dbobj(&mut self, cls: Oid, obj: Oid, mode: LockMode);
    fn lock_ns(&mut self, ns: Oid, mode: LockMode);
    fn lock_rel(&mut self, rel: Oid, mode: LockMode);
    fn unlock_rel(&mut self, rel: Oid, mode: LockMode);
}

fn local_acquire(
    locallocks: &mut [LocalLock<'_>; LOCKMODESNUM],
    mode: LockMode,
) -> Option<[u32; LOCKMODESNUM]> {
    let locallock = &mut locallocks[mode as usize];
    if locallock.n > 0 {
        locallock.n += 1;
        return None;
    }
    let mut lockcnt = [0; LOCKMODESNUM];
    macro_rules! assign {
        ($lockmode: ident) => {
            lockcnt[LockMode::$lockmode as usize] =
                (locallocks[LockMode::$lockmode as usize].n > 0) as u32;
        };
    }
    assign!(AccessShare);
    assign!(RowShare);
    assign!(RowExclusive);
    assign!(ShareUpdateExclusive);
    assign!(Share);
    assign!(ShareRowExclusive);
    assign!(Exclusive);
    assign!(AccessExclusive);
    return Some(lockcnt);
}

fn check_conflict(lock: &Lock, localcnts: &[u32; LOCKMODESNUM], mode: LockMode) -> bool {
    let confmodes = conflict_modes(mode);
    if (confmodes & lock.wait) != 0 {
        return true;
    }
    if (confmodes & lock.grant) == 0 {
        return false;
    }
    let mut confnum = 0;
    macro_rules! assign {
        ($lockmode: ident) => {
            confnum += if (confmodes & LockMode::$lockmode as u32) == 0 {
                0
            } else {
                lock.granted[LockMode::$lockmode as usize] - localcnts[LockMode::$lockmode as usize]
            }
        };
    }
    assign!(AccessShare);
    assign!(RowShare);
    assign!(RowExclusive);
    assign!(ShareUpdateExclusive);
    assign!(Share);
    assign!(ShareRowExclusive);
    assign!(Exclusive);
    assign!(AccessExclusive);
    return confnum > 0;
}

fn global_release(lock: &LockState, mode: LockMode) -> bool /* cleanup global */ {
    let mut l = lock.lock.lock().unwrap();
    if l.ungrant(mode) {
        lock.cv.notify_all();
        return false;
    }
    return l.nreq <= 0;
}

fn global_cleanup(lmgrg: &GlobalStateExt, tag: &LockTag) {
    let mut lm = lmgrg.lm.write().unwrap();
    if let Some(lock) = lm.get(tag) {
        let nreq = { lock.lock.lock().unwrap().nreq };
        if nreq <= 0 {
            lm.remove(tag);
        }
    }
}

fn local_release(
    locallocks: &mut [LocalLock<'_>; LOCKMODESNUM],
    mode: LockMode,
) -> (
    bool, /* cleanup local */
    bool, /* cleanup global */
) {
    let locallock = &mut locallocks[mode as usize];
    debug_assert!(locallock.n > 0);
    locallock.n -= 1;
    if locallock.n > 0 {
        return (false, false);
    }
    return (true, global_release(locallock.lock, mode));
}

fn total_n(locallocks: &[LocalLock<'_>; LOCKMODESNUM]) -> u64 {
    let mut retn = 0;
    for locallock in locallocks {
        retn += locallock.n;
    }
    return retn;
}

// SetLocktagRelationOid
fn get_rel_locktag(sess: &SessionState, reloid: Oid) -> LockTag {
    return LockTag::Relation {
        dboid: if is_shared_rel(reloid) {
            None
        } else {
            Some(sess.reqdb)
        },
        reloid,
    };
}

impl SessionExt for SessionState {
    fn lock_acquire(&mut self, tag: &LockTag, mode: LockMode) {
        let (locallocks, localcnts) = if let Some(locallocks) = self.lmgrs.lm.get_mut(&tag) {
            if let Some(localcnts) = local_acquire(locallocks, mode) {
                (Some(locallocks), localcnts)
            } else {
                return;
            }
        } else {
            (None, [0; LOCKMODESNUM])
        };
        let lockstate = self.lmgrg.setup_lock(tag, mode);
        {
            let mut lock = lockstate.lock.lock().unwrap();
            loop {
                if !check_conflict(&lock, &localcnts, mode) {
                    lock.grant(mode);
                    break;
                } else {
                    lock = lockstate.cv.wait(lock).unwrap();
                }
            }
        }
        if let Some(locallocks) = locallocks {
            let locallock = &mut locallocks[mode as usize];
            locallock.n = 1;
            locallock.lock = lockstate;
        } else {
            let mut locallocks = [LocalLock {
                n: 0,
                lock: lockstate,
            }; LOCKMODESNUM];
            locallocks[mode as usize].n = 1;
            self.lmgrs.lm.insert(*tag, locallocks);
        }
        return;
    }

    fn lock_release(&mut self, tag: &LockTag, mode: LockMode) {
        let locallocks = self.lmgrs.lm.get_mut(tag).unwrap();
        let (cleanupl, cleanupg) = local_release(locallocks, mode);
        if cleanupl && total_n(locallocks) <= 0 {
            self.lmgrs.lm.remove(tag);
        }
        if cleanupg {
            global_cleanup(self.lmgrg, tag);
        }
        return;
    }
    fn lock_release_all(&mut self) {
        for (locktag, locallocks) in &self.lmgrs.lm {
            macro_rules! release {
                ($mode: ident) => {
                    if locallocks[LockMode::$mode as usize].n > 0 {
                        global_release(locallocks[LockMode::$mode as usize].lock, LockMode::$mode)
                    } else {
                        false
                    }
                };
            }
            let b1 = release!(AccessShare);
            let b2 = release!(RowShare);
            let b3 = release!(RowExclusive);
            let b4 = release!(ShareUpdateExclusive);
            let b5 = release!(Share);
            let b6 = release!(ShareRowExclusive);
            let b7 = release!(Exclusive);
            let b8 = release!(AccessExclusive);
            if b1 || b2 || b3 || b4 || b5 || b6 || b7 || b8 {
                global_cleanup(self.lmgrg, locktag);
            }
        }
        self.lmgrs.lm = HashMap::new();
        return;
    }

    fn lock_dbobj(&mut self, cls: Oid, obj: Oid, mode: LockMode) {
        let locktag = LockTag::Object {
            dboid: self.reqdb,
            clsoid: cls,
            objoid: obj,
        };
        self.lock_acquire(&locktag, mode);
    }

    fn lock_ns(&mut self, ns: Oid, mode: LockMode) {
        self.lock_dbobj(NSRELID, ns, mode);
    }

    fn lock_rel(&mut self, rel: Oid, mode: LockMode) {
        let locktag = get_rel_locktag(self, rel);
        self.lock_acquire(&locktag, mode);
    }

    fn unlock_rel(&mut self, rel: Oid, mode: LockMode) {
        let locktag = get_rel_locktag(self, rel);
        self.lock_release(&locktag, mode);
    }
}

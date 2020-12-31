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
use static_assertions::const_assert;
use std::cmp::Ordering;
use std::cmp::{max, min};
use std::debug_assert;
use std::iter::Iterator;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Condvar, Mutex};
use stderrlog::{ColorChoice, Timestamp};

pub const KB_MAJOR: i32 = 0;
pub const KB_MINOR: i32 = 0;
pub const KB_PATCH: i32 = 1;
pub const KB_VER: i32 = KB_MAJOR * 100 * 100 + KB_MINOR * 100 + KB_PATCH;
// change the server_version in gucdef.yaml and Cargo.toml TOO!
pub const KB_VERSTR: &str = "0.0.1";
pub const KB_BLCKSZ: usize = 8192;
const_assert!((KB_BLCKSZ & (KB_BLCKSZ - 1)) == 0); // KB_BLCKSZ should be 2^n!

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

pub struct SelectedSliceIter<'a, T, IdxIter> {
    d: &'a [T],
    idx_iter: IdxIter,
}

impl<'a, T, IdxIter> Iterator for SelectedSliceIter<'a, T, IdxIter>
where
    IdxIter: Iterator,
    IdxIter::Item: std::convert::Into<usize>,
{
    type Item = (&'a T, usize);

    fn next(&mut self) -> Option<Self::Item> {
        match self.idx_iter.next() {
            None => None,
            Some(idx) => {
                let idx = idx.into();
                Some((&self.d[idx], idx))
            }
        }
    }
}

impl<'a, T, IdxIter> SelectedSliceIter<'a, T, IdxIter>
where
    IdxIter: Iterator,
    IdxIter::Item: std::convert::Into<usize>,
{
    pub fn new(d: &'a [T], idx_iter: IdxIter) -> SelectedSliceIter<'a, T, IdxIter> {
        SelectedSliceIter { d, idx_iter }
    }
}

// It took me 45min to name it, I did my best...
// Progresstracker is used to track what we have done. I try to explain ProgressTracker with the following scenario:
// 1. create a file.
// 2. Start 4 concurrent tasks to write data to [0, 100), [100, 200), [200, 300), [300, 400) respectively.
// 3. Task 3 is done so we know that data in [300, 400) is written.
// 4. Task 0 is done so we know that data in [0, 100) is written, it means that all data before 100 has been written.
// 5. Task 1 is done, it means that all data before 200 has been written.
// 6. Task 2 is done so we know that data in [200, 300) is written, and all data before 400 has been written.
pub struct ProgressTracker {
    // activity on all offset less than inflight[0].1 has been done
    inflight: Vec<(u64, u64)>,
}

impl ProgressTracker {
    pub fn new(d: u64) -> ProgressTracker {
        ProgressTracker {
            inflight: vec![(0, d)],
        }
    }

    // activity on all offset less than has_done() has been done
    fn has_done(&self) -> u64 {
        self.inflight[0].1
    }

    // Return new value of self.d if self.d has changed, return None otherwise.
    pub fn done(&mut self, start: u64, end: u64) -> Option<u64> {
        // debug_assert!(self.inflight.is_sorted());
        if start >= end {
            return None;
        }
        let s_idx = match self.inflight.binary_search_by_key(&start, |&(_, e)| e) {
            Ok(i) | Err(i) => i,
        };
        if s_idx >= self.inflight.len() {
            self.inflight.push((start, end));
            return None;
        }
        // e_idx is the first element whose start is greater than end.
        let e_idx = match self.inflight.binary_search_by(|&(s, _)| {
            if s <= end {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }) {
            Ok(i) | Err(i) => i,
        };
        debug_assert!(e_idx > 0 && s_idx <= e_idx);
        // v[s_idx - 1].end < start <= v[s_idx].end
        // v[e_idx - 1].start <= end < v[e_idx].start
        if s_idx == e_idx {
            self.inflight.insert(s_idx, (start, end));
            return None;
        }
        let donebefore = self.has_done();
        self.inflight[s_idx].0 = min(start, self.inflight[s_idx].0);
        self.inflight[s_idx].1 = max(end, self.inflight[e_idx - 1].1);
        self.inflight.drain(s_idx + 1..e_idx);
        let doneafter = self.has_done();
        debug_assert!(donebefore <= doneafter);
        if donebefore < doneafter {
            Some(doneafter)
        } else {
            None
        }
    }
}

pub struct Progress {
    curbak: AtomicU64,
    cur: Mutex<u64>,
    cond: Condvar,
}

impl Progress {
    pub fn new(cur: u64) -> Progress {
        Progress {
            cur: Mutex::new(cur),
            curbak: AtomicU64::new(cur),
            cond: Condvar::new(),
        }
    }

    pub fn set(&self, new_progress: u64) {
        {
            let mut cur = self.cur.lock().unwrap();
            *cur = new_progress;
            self.curbak.store(new_progress, Relaxed);
        }
        self.cond.notify_all();
    }

    pub fn get(&self) -> u64 {
        self.curbak.load(Relaxed)
    }

    pub fn wait(&self, progress: u64) {
        if progress <= self.get() {
            return;
        }
        let mut cur = self.cur.lock().unwrap();
        loop {
            if progress <= *cur {
                return;
            }
            cur = self.cond.wait(cur).unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Progress, ProgressTracker};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use std::{assert, assert_eq, thread};

    #[test]
    fn progress_tracker_test() {
        let mut pt = ProgressTracker::new(33);
        assert_eq!(None, pt.done(33, 33));
        assert_eq!(None, pt.done(44, 77));
        assert_eq!(Some(40), pt.done(33, 40));
        assert_eq!(Some(77), pt.done(40, 44));
        assert_eq!(&[(0, 77)], pt.inflight.as_slice());

        assert_eq!(None, pt.done(100, 200));
        assert_eq!(None, pt.done(200, 300));
        assert_eq!(2, pt.inflight.len());
        assert_eq!(None, pt.done(400, 500));
        assert_eq!(3, pt.inflight.len());

        assert_eq!(None, pt.done(90, 100));
        assert_eq!(3, pt.inflight.len());

        assert_eq!(None, pt.done(80, 85));
        assert_eq!(4, pt.inflight.len());

        assert_eq!(None, pt.done(86, 88));
        assert_eq!(
            &[(0, 77), (80, 85), (86, 88), (90, 300), (400, 500)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(89, 90));
        assert_eq!(
            &[(0, 77), (80, 85), (86, 88), (89, 300), (400, 500)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(88, 89));
        assert_eq!(
            &[(0, 77), (80, 85), (86, 300), (400, 500)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(300, 333));
        assert_eq!(
            &[(0, 77), (80, 85), (86, 333), (400, 500)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(85, 86));
        assert_eq!(&[(0, 77), (80, 333), (400, 500)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(333, 400));
        assert_eq!(&[(0, 77), (80, 500)], pt.inflight.as_slice());
        assert_eq!(Some(500), pt.done(77, 80));
        assert_eq!(&[(0, 500)], pt.inflight.as_slice());
    }

    #[test]
    fn progress_tracker_test2() {
        let mut pt = ProgressTracker::new(33);
        assert_eq!(&[(0, 33)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(77, 88));
        assert_eq!(&[(0, 33), (77, 88)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(88, 99));
        assert_eq!(&[(0, 33), (77, 99)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(200, 203));
        assert_eq!(&[(0, 33), (77, 99), (200, 203)], pt.inflight.as_slice());
        assert_eq!(None, pt.done(102, 105));
        assert_eq!(
            &[(0, 33), (77, 99), (102, 105), (200, 203)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(119, 122));
        assert_eq!(
            &[(0, 33), (77, 99), (102, 105), (119, 122), (200, 203)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(108, 111));
        assert_eq!(
            &[
                (0, 33),
                (77, 99),
                (102, 105),
                (108, 111),
                (119, 122),
                (200, 203)
            ],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(113, 116));
        assert_eq!(
            &[
                (0, 33),
                (77, 99),
                (102, 105),
                (108, 111),
                (113, 116),
                (119, 122),
                (200, 203)
            ],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(107, 177));
        assert_eq!(
            &[(0, 33), (77, 99), (102, 105), (107, 177), (200, 203)],
            pt.inflight.as_slice()
        );
        assert_eq!(None, pt.done(77, 203));
        assert_eq!(&[(0, 33), (77, 203)], pt.inflight.as_slice());
        assert_eq!(Some(233), pt.done(23, 233));
        assert_eq!(&[(0, 233)], pt.inflight.as_slice());
    }

    #[test]
    fn progress_test() {
        let p = Progress::new(33);
        p.wait(11);
    }

    #[test]
    fn progress_test2() {
        let p = Arc::new(Progress::new(33));
        let p1 = p.clone();
        let t = thread::spawn(move || {
            thread::sleep(Duration::from_secs(7));
            p1.set(55);
            thread::sleep(Duration::from_secs(7));
            p1.set(100);
        });
        let wp = Instant::now();
        p.wait(77);
        let d = wp.elapsed();
        assert!(d >= Duration::from_secs(11));
        t.join().unwrap();
    }
}

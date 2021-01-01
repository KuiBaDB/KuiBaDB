use anyhow::anyhow;
use kuiba::KB_BLCKSZ;
use nix::sys::uio::{pread, pwrite};
use std::collections::HashMap;
use std::debug_assert;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const PAGES_PER_SEGMENT: u64 = 32;

pub type Pageno = u64;

type Buff = [u8; KB_BLCKSZ];

#[derive(PartialEq, Eq)]
enum Status {
    ReadInProgress,
    Valid {
        buff: Arc<Buff>,
        write_in_progress: bool,
    },
}

struct Slot {
    dirty: bool,
    status: Status,
    cond: Arc<Mutex<()>>,
    lru_cnt: AtomicU64,
}

impl Slot {
    fn new(status: Status) -> Slot {
        Slot {
            dirty: false,
            status,
            cond: Arc::default(),
            lru_cnt: AtomicU64::new(0),
        }
    }

    fn set_write_in_progress(&mut self, val: bool) {
        if let Status::Valid {
            buff: _,
            write_in_progress,
        } = &mut self.status
        {
            *write_in_progress = val;
        } else {
            panic!("unreachable");
        }
    }
}

struct SlruData {
    m: HashMap<Pageno, Slot>,
    max_size: usize,
}

fn get_u64(val: &mut AtomicU64) -> u64 {
    *val.get_mut()
}

impl SlruData {
    fn least_used(&mut self, lru_cnt: u64) -> (Pageno, &mut Slot) {
        let mut invalid_least: Option<(u64, Pageno, &mut Slot)> = None;
        let mut valid_least: Option<(u64, Pageno, &mut Slot)> = None;
        for (&pageno, slot) in &mut self.m {
            let slot_lru_cnt = get_u64(&mut slot.lru_cnt);
            debug_assert!(lru_cnt >= slot_lru_cnt);
            let slot_delta = lru_cnt - slot_lru_cnt;
            let least = if let Status::Valid { .. } = slot.status {
                &mut valid_least
            } else {
                &mut invalid_least
            };
            if let &mut Some((delta, ..)) = least {
                if delta >= slot_delta {
                    continue;
                }
            }
            *least = Some((slot_delta, pageno, slot));
        }
        let least = if let Some(least) = valid_least {
            least
        } else {
            invalid_least.unwrap()
        };
        (least.1, least.2)
    }
}

fn write(dir: &str, pageno: Pageno, buff: Arc<Buff>) -> anyhow::Result<()> {
    let segno = pageno / PAGES_PER_SEGMENT;
    let rpageno = pageno % PAGES_PER_SEGMENT;
    let off = (rpageno * KB_BLCKSZ as u64) as i64;
    let path = seg_path(dir, segno);
    let file = OpenOptions::new().create(true).write(true).open(path)?;
    if KB_BLCKSZ != pwrite(file.as_raw_fd(), &*buff, off)? {
        return Err(anyhow!("SLRU_WRITE_FAILED"));
    }
    // TODO: checkpointer sync
    Ok(())
}

fn seg_path(dir: &str, segno: u64) -> PathBuf {
    Path::new(dir).join(segno.to_string())
}

fn read(dir: &str, pageno: Pageno) -> anyhow::Result<Arc<Buff>> {
    let segno = pageno / PAGES_PER_SEGMENT;
    let rpageno = pageno % PAGES_PER_SEGMENT;
    let off = (rpageno * KB_BLCKSZ as u64) as i64;
    let path = seg_path(dir, segno);
    let mut buff = Arc::<Buff>::new([0; KB_BLCKSZ]);
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                return Ok(buff);
            }
            return Err(e)?;
        }
    };
    let readn = pread(file.as_raw_fd(), Arc::get_mut(&mut buff).unwrap(), off)?;
    if readn == KB_BLCKSZ || readn == 0 {
        Ok(buff) // Do we really need ExtendCLOG()?
    } else {
        Err(anyhow!("SLRU_READ_FAILED"))
    }
}

pub struct Slru {
    data: RwLock<SlruData>,
    lru_cnt: AtomicU64,
    dir: &'static str,
}

impl Slru {
    fn recently_used(&self, slot: &Slot) {
        let lru_cnt = self.lru_cnt.fetch_add(1, Ordering::Relaxed) + 1;
        slot.lru_cnt.fetch_max(lru_cnt, Ordering::Relaxed);
    }

    pub fn new(max_size: usize, dir: &'static str) -> Slru {
        Slru {
            data: RwLock::new(SlruData {
                max_size,
                m: HashMap::with_capacity(max_size),
            }),
            lru_cnt: AtomicU64::new(0),
            dir,
        }
    }

    fn load<T, F>(&self, pageno: Pageno, cb: F) -> anyhow::Result<T>
    where
        F: FnOnce(&mut Arc<Buff>) -> (T, bool),
    {
        let mut slru_guard = self.data.write().unwrap();
        let mut slru = &mut *slru_guard;
        macro_rules! unlock_slru {
            () => {
                std::mem::drop(slru_guard);
            };
        }
        macro_rules! lock_slru {
            () => {
                slru_guard = self.data.write().unwrap();
                slru = &mut *slru_guard;
            };
        }
        macro_rules! wait_io {
            ($slot: ident) => {{
                let cond = $slot.cond.clone();
                unlock_slru!();
                {
                    let _ = cond.lock().unwrap();
                }
                lock_slru!();
            }};
        }
        macro_rules! invoke_cb {
            ($slot: ident, $buff: expr) => {{
                let (cbret, dirty) = cb($buff);
                $slot.dirty = dirty;
                self.recently_used($slot);
                cbret
            }};
        }
        loop {
            if let Some(slot) = slru.m.get_mut(&pageno) {
                match slot.status {
                    Status::ReadInProgress => {
                        wait_io!(slot);
                        continue;
                    }
                    Status::Valid { ref mut buff, .. } => {
                        return Ok(invoke_cb!(slot, buff));
                    }
                }
            }

            if slru.m.len() >= slru.max_size {
                let (wpageno, wslot) = slru.least_used(self.lru_cnt.load(Ordering::Relaxed));
                match &mut wslot.status {
                    Status::Valid {
                        buff,
                        write_in_progress,
                    } if !*write_in_progress => {
                        if wslot.dirty {
                            *write_in_progress = true;
                            wslot.dirty = false;

                            let wres;
                            {
                                let buff = buff.clone();
                                let cond = wslot.cond.clone();
                                let _write_guard = cond.lock().unwrap();
                                unlock_slru!();
                                wres = write(self.dir, wpageno, buff);
                                lock_slru!();
                            }

                            if let Err(e) = wres {
                                let wslot = slru.m.get_mut(&wpageno).unwrap();
                                wslot.dirty = true;
                                wslot.set_write_in_progress(false);
                                return Err(e);
                            }
                        }
                        slru.m.remove(&wpageno).unwrap();
                    }
                    _ => {
                        wait_io!(wslot);
                        continue;
                    }
                }
            }
            debug_assert!(slru.m.len() < slru.max_size);

            let slot = Slot::new(Status::ReadInProgress);
            let cond = slot.cond.clone();
            slru.m.insert(pageno, slot);

            let rres;
            {
                let _read_guard = cond.lock().unwrap();
                unlock_slru!();
                rres = read(self.dir, pageno);
                lock_slru!();
            }

            return match rres {
                Err(e) => {
                    slru.m.remove(&pageno);
                    Err(e)
                }
                Ok(mut buff) => {
                    let slot = slru.m.get_mut(&pageno).unwrap();
                    debug_assert!(slot.status == Status::ReadInProgress && !slot.dirty);
                    let cbret = invoke_cb!(slot, &mut buff);
                    slot.status = Status::Valid {
                        write_in_progress: false,
                        buff,
                    };
                    Ok(cbret)
                }
            };
        }
    }

    pub fn writable_load<F>(&self, pageno: Pageno, cb: F) -> anyhow::Result<()>
    where
        F: FnOnce(&mut Buff),
    {
        return self.load(pageno, |buff| (cb(Arc::make_mut(buff)), true));
    }

    pub fn try_readonly_load<T, F>(&self, pageno: Pageno, cb: F) -> anyhow::Result<T>
    where
        F: FnOnce(&Buff) -> T,
    {
        {
            let slru = self.data.read().unwrap();
            if let Some(slot) = slru.m.get(&pageno) {
                if let Status::Valid { buff, .. } = &slot.status {
                    self.recently_used(slot);
                    return Ok(cb(&buff));
                }
            }
        }
        return self.load(pageno, |buff| (cb(buff), false));
    }
}

use crate::access::wal;
use crate::access::wal::{Ctl, LocalWalStorage, Rmgr, WalReader, XlogRmgr};
use crate::utils::Xid;
use crate::{GlobalState, Oid};
use anyhow::anyhow;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, AtomicU64};

pub struct RedoState {
    nextxid: Xid,
    nextoid: Oid,
}

impl RedoState {
    fn new(nextxid: Xid, nextoid: Oid) -> RedoState {
        RedoState { nextxid, nextoid }
    }

    pub fn set_nextxid(&mut self, nextxid: Xid) {
        if self.nextxid < nextxid {
            self.nextxid = nextxid;
        }
    }

    pub fn seen_xid(&mut self, xid: Xid) {
        if self.nextxid > xid {
            return;
        }
        self.nextxid = Xid::new(xid.get() + 1).unwrap();
    }
}

pub fn redo(datadir: &str) -> anyhow::Result<GlobalState> {
    let mut g = GlobalState::init(datadir);
    let ctl = Ctl::load()?;
    log::info!("start redo. ctl={:?}", ctl);

    let mut walreader = WalReader::new(Box::new(LocalWalStorage::new()), ctl.ckptcpy.redo);
    let redo_state = RefCell::new(RedoState::new(ctl.ckptcpy.nextxid, ctl.ckptcpy.nextoid));
    let mut xlogrmgr = XlogRmgr::new(&redo_state);
    let rmgrlist = [&mut xlogrmgr as &mut dyn Rmgr];
    loop {
        match walreader.read_record() {
            Err(e) => {
                log::info!(
                    "end redo because of failed read. endlsn={} endtli={} err={}",
                    walreader.endlsn,
                    walreader.endtli(),
                    e
                );
                break;
            }
            Ok((h, data)) => {
                if let Some(x) = h.xid {
                    redo_state.borrow_mut().seen_xid(x);
                }
                rmgrlist[h.id as u8 as usize].redo(&h, &data)?;
            }
        }
    }
    if walreader.endlsn <= ctl.ckpt {
        return Err(anyhow!("redo: quit early. endlsn={}", walreader.endlsn));
    }
    let redo_state = redo_state.into_inner();
    log::info!(
        "End of redo. nextxid: {}, nextoid: {}",
        redo_state.nextxid,
        redo_state.nextoid
    );

    walreader.storage.recycle(walreader.endlsn)?;
    g.oid_creator = Some(Box::leak(Box::new(AtomicU32::new(
        redo_state.nextoid.get(),
    ))));
    g.xid_creator = Some(Box::leak(Box::new(AtomicU64::new(
        redo_state.nextxid.get(),
    ))));
    let readlsn = match walreader.readlsn {
        None => return Err(anyhow!("walreader.readlsn is None")),
        Some(r) => r,
    };
    g.wal = Some(wal::init(
        walreader.endtli(),
        walreader.endlsn,
        Some(readlsn),
        ctl.ckptcpy.redo,
        &g.gucstate,
    )?);
    Ok(g)
}

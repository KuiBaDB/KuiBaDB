use crate::access::wal::{Ctl, LocalWalStorage, Rmgr, WalReader, XlogRmgr};
use crate::access::{wal, wal::RmgrId, xact, xact::XactRmgr};
use crate::utils::{inc_xid, WorkerState, Xid};
use crate::{guc, make_static, GlobalState, Oid, REDO_SESSID};
use anyhow::anyhow;
use std::sync::atomic::AtomicU32;

pub struct RedoState {
    nextxid: Xid,
    nextoid: Oid,
    pub worker: WorkerState,
}

impl RedoState {
    fn new(nextxid: Xid, nextoid: Oid, worker: WorkerState) -> RedoState {
        RedoState {
            nextxid,
            nextoid,
            worker,
        }
    }

    pub fn set_nextoid(&mut self, nextoid: Oid) {
        if self.nextoid < nextoid {
            self.nextoid = nextoid;
        }
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
        self.nextxid = inc_xid(xid);
    }
}

pub fn redo(datadir: &str) -> anyhow::Result<GlobalState> {
    let mut g = GlobalState::init(datadir);
    let ctl = Ctl::load()?;
    log::info!("start redo. ctl={:?}", ctl);

    let mut walreader = WalReader::new(Box::new(LocalWalStorage::new()), ctl.ckptcpy.redo);
    let session = g.clone().internal_session(REDO_SESSID).unwrap();
    session.init_thread_locals();
    let worker = session.new_worker();
    let mut redo_state = RedoState::new(ctl.ckptcpy.nextxid, ctl.ckptcpy.nextoid, worker);
    let mut xlogrmgr = XlogRmgr::new();
    let mut xactrmgr = XactRmgr::new();
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
                    redo_state.seen_xid(x);
                }
                match h.id {
                    RmgrId::Xlog => xlogrmgr.redo(&h, &data, &mut redo_state)?,
                    RmgrId::Xact => xactrmgr.redo(&h, &data, &mut redo_state)?,
                    _ => todo!(),
                }
            }
        }
    }
    if walreader.endlsn <= ctl.ckpt {
        return Err(anyhow!("redo: quit early. endlsn={}", walreader.endlsn));
    }
    log::info!(
        "End of redo. nextxid: {}, nextoid: {}",
        redo_state.nextxid,
        redo_state.nextoid
    );

    walreader.storage.recycle(walreader.endlsn)?;
    // Use CreateCheckPoint() instead.
    g.tabsv.flushall(true)?;
    g.tabmvcc.flushall(true)?;

    g.oid_creator = Some(make_static(AtomicU32::new(redo_state.nextoid.get())));
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
    g.xact = Some(make_static(xact::GlobalStateExt::new(
        redo_state.nextxid,
        guc::get_int(&g.gucstate, guc::XidStopLimit),
    )));
    g.renew();
    Ok(g)
}

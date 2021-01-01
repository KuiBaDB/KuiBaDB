use crate::catalog;
use crate::utils::{SessionState, Worker, WorkerState};
use crate::GlobalState;
use std::env;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

mod clog;

fn init_global_state() -> GlobalState {
    let datadir = env::var("KUIBADB_DATADIR").expect("KUIBADB_DATADIR env");
    GlobalState::init(&datadir)
}

lazy_static::lazy_static! {
    static ref GLOBAL_STATE: GlobalState = init_global_state();
}

fn new_session_state(global_state: &GlobalState) -> SessionState {
    let reqdb = catalog::get_database("kuiba").unwrap();
    SessionState::new(
        20181218,
        reqdb.oid,
        reqdb.datname,
        Arc::<AtomicBool>::default(),
        sqlite::open(format!("base/{}/meta.db", reqdb.oid)).unwrap(),
        global_state.clone(),
    )
}

fn new_worker() -> Worker {
    Worker::new(WorkerState::new(&new_session_state(&GLOBAL_STATE)))
}

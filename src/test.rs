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

use crate::utils::{SessionState, Worker, WorkerState};
use crate::{GlobalState, TEST_SESSID};
use std::env;

mod clog;

fn init_global_state() -> GlobalState {
    let datadir = env::var("KUIBADB_DATADIR").expect("KUIBADB_DATADIR env");
    GlobalState::init(&datadir)
}

lazy_static::lazy_static! {
    static ref GLOBAL_STATE: GlobalState = init_global_state();
}

fn new_session_state(global_state: &GlobalState) -> SessionState {
    global_state.clone().internal_session(TEST_SESSID).unwrap()
}

fn new_worker() -> Worker {
    Worker::new(WorkerState::new(&new_session_state(&GLOBAL_STATE)))
}

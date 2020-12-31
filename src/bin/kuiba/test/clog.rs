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

use crate::clog::{VecXidStatus, WorkerExt, XidStatus, XACTS_PER_BYTE};
use crate::guc;
use crate::test::new_worker;
use crate::utils::{Worker, Xid};
use kuiba::KB_BLCKSZ;
use std::assert_eq;

fn get_xid_status(w: &Worker, xids: &[Xid]) -> Vec<XidStatus> {
    let idxes: Vec<usize> = (0..xids.len()).into_iter().collect();
    let mut ret = VecXidStatus::new(xids.len());
    w.get_xid_status(xids, &idxes, &mut ret).unwrap();
    let mut v = Vec::<XidStatus>::with_capacity(xids.len());
    for &byteval in ret.data() {
        let xid_status = VecXidStatus::split(byteval);
        v.push(xid_status[0]);
        v.push(xid_status[1]);
        v.push(xid_status[2]);
        v.push(xid_status[3]);
    }
    v
}

fn get_single_xid_status(w: &Worker, xid: Xid) -> XidStatus {
    let xids = [xid];
    get_xid_status(w, &xids)[0]
}

#[test]
fn t() {
    let worker = new_worker();
    assert_eq!(
        1,
        guc::get_int(&worker.state.gucstate, guc::ClogL2cacheSize)
    );

    let xid = Xid::new(1).unwrap();
    assert_eq!(XidStatus::InProgress, get_single_xid_status(&worker, xid));
    worker.set_xid_status(xid, XidStatus::Committed).unwrap();
    assert_eq!(XidStatus::Committed, get_single_xid_status(&worker, xid));

    let xid = Xid::new(KB_BLCKSZ as u64 * XACTS_PER_BYTE * 4).unwrap();
    assert_eq!(XidStatus::InProgress, get_single_xid_status(&worker, xid));
    worker.set_xid_status(xid, XidStatus::Committed).unwrap();
    assert_eq!(XidStatus::Committed, get_single_xid_status(&worker, xid));

    let xid = Xid::new(1).unwrap();
    assert_eq!(XidStatus::Committed, get_single_xid_status(&worker, xid));
}

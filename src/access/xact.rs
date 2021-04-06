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
use crate::utils::{SessionState, Xid};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::sync::RwLock;

struct BTreeMultiSet<T: Ord> {
    d: BTreeMap<T, u32>,
}

impl<T: Ord> BTreeMultiSet<T> {
    fn new() -> Self {
        Self { d: BTreeMap::new() }
    }

    fn insert(&mut self, value: T) {
        if let Some(cnt) = self.d.get_mut(&value) {
            *cnt += 1;
        } else {
            self.d.insert(value, 1);
        }
    }

    fn first(&self) -> Option<&T> {
        self.d.iter().next().map(|kv| kv.0)
    }

    fn remove<Q: ?Sized>(&mut self, value: &Q)
    where
        T: Borrow<Q>,
        Q: Ord,
    {
        if let Some(cnt) = self.d.get_mut(&value) {
            *cnt -= 1;
            if *cnt <= 0 {
                self.d.remove(value);
            }
        }
    }
}

struct RunningXactState {
    xids: BTreeMultiSet<Xid>,
    last_completed: Xid,
    nextxid: Xid,
}

pub struct GlobalStateExt {
    running: RwLock<RunningXactState>,
    xmins: RwLock<BTreeMultiSet<Xid>>,
}

impl GlobalStateExt {
    pub fn new(nextxid: Xid) -> GlobalStateExt {
        GlobalStateExt {
            running: RwLock::new(RunningXactState {
                xids: BTreeMultiSet::new(),
                last_completed: Xid::new(nextxid.get() - 1).unwrap(),
                nextxid,
            }),
            xmins: RwLock::new(BTreeMultiSet::new()),
        }
    }
}

enum TransState {
    Default,
    Inprogress,
}

pub struct SessionStateExt {
    xid: Option<Xid>,
}

// StartTransaction
fn start_tran(sess: &mut SessionState) -> anyhow::Result<()> {
    todo!()
}
// CommitTransaction
fn commit_tran(sess: &mut SessionState) -> anyhow::Result<()> {
    todo!()
}
// AbortTransaction
fn abort_tran(sess: &mut SessionState) -> anyhow::Result<()> {
    todo!()
}
// CleanupTransaction
fn cleanup_tran(sess: &mut SessionState) -> anyhow::Result<()> {
    todo!()
}

pub trait SessionExt {
    // StartTransactionCommand
    fn start_tran_cmd(&mut self) -> anyhow::Result<()>;
    // CommitTransactionCommand
    fn commit_tran_cmd(&mut self) -> anyhow::Result<()>;
    // AbortCurrentTransaction
    fn abort_cur_tran(&mut self) -> anyhow::Result<()>;
    // BeginTransactionBlock
    fn begin_tran_block(&mut self) -> anyhow::Result<()>;
    // EndTransactionBlock
    fn end_tran_block(&mut self) -> anyhow::Result<()>;
    // UserAbortTransactionBlock
    fn user_abort_tran_block(&mut self) -> anyhow::Result<()>;
}

impl SessionExt for SessionState {
    fn start_tran_cmd(&mut self) -> anyhow::Result<()> {
        todo!()
    }
    fn commit_tran_cmd(&mut self) -> anyhow::Result<()> {
        todo!()
    }
    fn abort_cur_tran(&mut self) -> anyhow::Result<()> {
        todo!()
    }
    fn begin_tran_block(&mut self) -> anyhow::Result<()> {
        todo!()
    }
    fn end_tran_block(&mut self) -> anyhow::Result<()> {
        todo!()
    }
    fn user_abort_tran_block(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}

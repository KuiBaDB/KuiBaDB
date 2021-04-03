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
use chrono::offset::Local;
use chrono::DateTime;
use kuiba::access::wal;

fn main() {
    let ctl = wal::Ctl::load().unwrap();
    let t: DateTime<Local> = ctl.time.into();
    println!("kb_control version number: {}", wal::KB_CTL_VER);
    println!("Catalog version number: {}", wal::KB_CAT_VER);
    println!("kb_control last modified: {:?}", t);
    let ckpt = ctl.ckpt;
    println!("Latest checkpoint location: {}", ckpt);
    let v = ctl.ckptcpy.redo;
    println!("Latest checkpoint's REDO location: {}", v);
    let v = ctl.ckptcpy.curtli;
    println!("Latest checkpoint's TimeLineID: {}", v);
    let v = ctl.ckptcpy.prevtli;
    println!("Latest checkpoint's PrevTimeLineID: {}", v);
    let v = ctl.ckptcpy.nextxid;
    println!("Latest checkpoint's NextXID: {}", v);
    let v = ctl.ckptcpy.nextoid;
    println!("Latest checkpoint's NextOID: {}", v);
    let v: DateTime<Local> = ctl.ckptcpy.time.into();
    println!("Time of latest checkpoint: {:?}", v);
}

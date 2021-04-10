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
use clap::{App, Arg};
use kuiba::{access::redo::redo, guc, init_log, postgres_main, LAST_INTERNAL_SESSID};
use std::net::TcpListener;
use std::thread;

fn new_sessid(lastused: &mut u32) -> u32 {
    *lastused += 1;
    let v = *lastused;
    if v <= LAST_INTERNAL_SESSID {
        panic!("new_sessid: unexpected sessid")
    } else {
        v
    }
}

fn main() {
    init_log();
    let cmdline = App::new("KuiBaDB(魁拔)")
        .version(kuiba::KB_VERSTR)
        .author("盏一 <w@hidva.com>")
        .about("KuiBaDB is another Postgresql written in Rust")
        .arg(
            Arg::with_name("datadir")
                .short("D")
                .long("datadir")
                .required(true)
                .takes_value(true),
        )
        .get_matches();
    let datadir = cmdline
        .value_of("datadir")
        .expect("You must specify the -D invocation option!");
    let global_state = redo(&datadir).expect("redo failed");
    let port = guc::get_int(&global_state.gucstate, guc::Port) as u16;
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    log::info!("listen. port={}", port);
    let mut lastused_sessid = LAST_INTERNAL_SESSID;
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let global_state = global_state.clone();
        let sessid = new_sessid(&mut lastused_sessid);
        thread::spawn(move || {
            postgres_main(global_state, stream, sessid);
        });
    }
}

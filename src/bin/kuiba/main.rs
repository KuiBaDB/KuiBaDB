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

use clap::{App, Arg};
use kuiba::guc::{self, GucState};
use kuiba::{postgres_main, GlobalState};
use std::io;
use std::net::TcpListener;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::runtime::{Builder, Runtime};
use tracing::warn;

const OPT_DATADIR: &str = "datadir";
const OPT_BUFFLOG_LINE_MAX: &str = "bufflog_line_max";

fn new_runtime(gucstate: &GucState) -> io::Result<Runtime> {
    let max_blocking_threads = guc::get_int(&gucstate, guc::TokioMaxBlockingThreads) as usize;
    let keep_alive = guc::get_int(&gucstate, guc::TokioThreadKeepAlive);
    let keep_alive = Duration::from_secs(keep_alive as u64);
    let stack_size = guc::get_int(&gucstate, guc::TokioThreadStackSize) as usize;
    let threads = guc::get_int(&gucstate, guc::TokioWorkerThreads) as usize;
    let mut builder = Builder::new_multi_thread();
    builder
        .max_blocking_threads(max_blocking_threads)
        .thread_keep_alive(keep_alive)
        .thread_stack_size(stack_size);
    if threads > 0 {
        builder.worker_threads(threads);
    }
    return builder.build();
}

async fn do_main(gucstate: GucState) {
    let gstate = GlobalState::new(Arc::new(gucstate)).unwrap();
    let port = guc::get_int(&gstate.gucstate, guc::Port) as u16;
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    let listener = listener.as_raw_fd();
    let uring = gstate.urings.non_iopoll();
    loop {
        match uring.accept(listener).await {
            Ok((srvfd, cliaddr)) => {
                let gstate = gstate.clone();
                tokio::spawn(postgres_main(gstate, srvfd, cliaddr));
            }
            Err(e) => {
                warn!("accept failed. err={:#}", e);
            }
        }
    }
}

fn main() {
    let cmdline = App::new("KuiBaDB(魁拔)")
        .version(kuiba::KB_VERSTR)
        .author("盏一 <w@hidva.com>")
        .about("KuiBaDB is another Postgresql written in Rust")
        .arg(
            Arg::with_name(OPT_DATADIR)
                .short("D")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name(OPT_BUFFLOG_LINE_MAX)
                .short("L")
                .takes_value(true),
        )
        .get_matches();

    let datadir = cmdline
        .value_of(OPT_DATADIR)
        .expect("You must specify the -D invocation option!");
    let bufflog_line_max = cmdline
        .value_of(OPT_BUFFLOG_LINE_MAX)
        .map(|v| v.parse().unwrap())
        .unwrap_or(8192usize);
    let gucstate = kuiba::init(bufflog_line_max, datadir).unwrap();
    let rt = new_runtime(&gucstate).unwrap();
    rt.block_on(do_main(gucstate));
    return;
}

// Copyright 2021 <盏一 w@hidva.com>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use crate::access::cs;
use crate::access::csmvcc::MVCCBuf;
use crate::access::lmgr::LockMode;
use crate::access::rel;
use crate::access::sv;
use crate::access::xact::SessionExt as XACTSessionExt;
use crate::catalog::get_type_input_info;
use crate::catalog::namespace::SessionExt as NSSessionExt;
use crate::datums::Datums;
use crate::guc;
use crate::parser::syn;
use crate::parser::syn::RangeVar;
use crate::utility::Response;
use crate::utils::fmgr::{call_inproc, FmgrInfo};
use crate::utils::{SessionState, WorkerExitGuard, WorkerState};
use crate::{kbbail, kbensure};
use crossbeam_channel::{bounded, Receiver};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::mem::forget;
use std::rc::Rc;

// pub fn lock_stmt(sess: &mut SessionState, lock: &syn::LockStmt<'_>) -> anyhow::Result<Response> {

struct CopyOpts<'syn> {
    delim: &'syn str,
    parallel: usize,
    null: &'syn str,
}

struct CopyFromArgs {
    inrec: Receiver<(Vec<Datums>, u32)>,
    tabid: sv::TableId,
    rel: rel::Rel,
    l0file: sv::FileMeta,
    typins: Vec<FmgrInfo>,
    mvccbuf: &'static MVCCBuf,
}

fn new_indatums(attcnt: usize, batch_size: u32) -> Vec<Datums> {
    let mut indatums = Vec::with_capacity(attcnt);
    indatums.resize_with(attcnt, || {
        let mut datums = Datums::new();
        datums.resize_varlen(batch_size);
        datums.set_notnull_all();
        datums
    });
    return indatums;
}

fn indatums2data(
    indatums: Vec<Datums>,
    typmods: &[Rc<Datums>],
    typins: &[FmgrInfo],
    worker: &WorkerState,
) -> anyhow::Result<Vec<Rc<Datums>>> {
    debug_assert_eq!(indatums.len(), typins.len());
    debug_assert_eq!(typmods.len(), typins.len());
    let mut outs = Vec::with_capacity(typins.len());
    let mut idx = 0;
    for indatum in indatums {
        let typmod = typmods[idx].clone();
        let indatum = Rc::new(indatum);
        let typin = &typins[idx];
        let mut outdatum = Rc::new(Datums::new());
        call_inproc(typin, &mut outdatum, indatum, typmod, worker)?;
        outs.push(outdatum);
        idx += 1;
    }
    debug_assert_eq!(typins.len(), outs.len());
    return Ok(outs);
}

fn copyfrommain(args: CopyFromArgs, worker: &mut WorkerState) -> anyhow::Result<sv::FileMeta> {
    let attcnt = args.rel.attrs.len();
    let mut typmods = Vec::with_capacity(attcnt);
    for attr in &args.rel.attrs {
        typmods.push(Rc::new(Datums::new_single_fixedlen(attr.typ.mode)));
    }
    debug_assert_eq!(attcnt, args.typins.len());
    let mut l0writer = cs::L0Writer::new(args.tabid, args.rel, args.l0file);
    for (indatums, inrownum) in args.inrec.iter() {
        debug_assert_eq!(indatums.len(), attcnt);
        let outs = indatums2data(indatums, &typmods, &args.typins, worker)?;
        l0writer.write(outs, inrownum)?;
    }
    l0writer.sync(worker, args.mvccbuf)?;
    return Ok(l0writer.meta);
}

fn copyfrom(
    dest: &RangeVar<'_>,
    input: impl BufRead,
    opts: &CopyOpts,
    sess: &mut SessionState,
) -> anyhow::Result<u64> {
    let batch_size = guc::get_int(&sess.gucstate, guc::BatchSize) as u32;
    let tableoid = sess.rv_get_oid(dest, LockMode::AccessShare)?;
    let tableid = sv::TableId {
        db: sess.reqdb,
        table: tableoid,
    };
    let destrel = rel::getrel(sess, tableoid)?;
    let attcnt = destrel.attrs.len();
    let mut typins = Vec::with_capacity(destrel.attrs.len());
    for attr in &destrel.attrs {
        let typinoid = get_type_input_info(sess, attr.typ.id)?;
        let typin = FmgrInfo::new(typinoid, sess.fmgr_builtins)?;
        typins.push(typin);
    }

    // mvccslot pin guard
    let mvccslot = sess.tabmvcc.read(&tableid, &destrel.opt)?;
    let mvcc = mvccslot.v.read().unwrap();
    let mvcc = mvcc.as_ref().unwrap();
    let mvcc: &'static MVCCBuf = unsafe { &*(mvcc as *const _) };
    mvccslot.mark_dirty();

    // svslot guard
    let svslot = sess.tabsv.read(&tableid, &destrel.opt.enable_cs_wal)?;
    let l0files = sv::start_write(sess, &svslot, opts.parallel)?;
    // AbortWriteGuard
    let abort_guard = sv::AbortWriteGuard::new(&svslot, &l0files);

    sess.get_xid()?;
    let (datas, datar) = bounded::<(Vec<Datums>, u32)>(opts.parallel);
    let arggen = |idx| CopyFromArgs {
        inrec: datar.clone(),
        tabid: tableid,
        rel: destrel.clone(),
        l0file: l0files[idx],
        typins: typins.clone(),
        mvccbuf: mvcc,
    };
    let workerrec = sess.exec(opts.parallel, arggen, copyfrommain);
    // worker_exit_guard
    let worker_exit_guard = WorkerExitGuard::new(&workerrec);

    let mut totalrows = 0u64;
    let mut inrownum = 0isize;
    let mut indatums = new_indatums(attcnt, batch_size);
    for line in input.lines() {
        let line = line?;
        let mut colidx = 0usize;
        for colstr in line.split(opts.delim) {
            kbensure!(
                colidx < attcnt,
                ERRCODE_BAD_COPY_FILE_FORMAT,
                "extra data after last expected column",
            );
            if colstr == opts.null {
                indatums[colidx].set_null_at(inrownum);
                indatums[colidx].set_empty_at(inrownum);
            } else {
                indatums[colidx].set_varchar_at(inrownum, colstr.as_bytes());
            }
            colidx += 1;
        }
        kbensure!(
            colidx == attcnt,
            ERRCODE_BAD_COPY_FILE_FORMAT,
            "missing data for column",
        );
        inrownum += 1;
        if inrownum >= batch_size as isize {
            datas.send((indatums, inrownum as u32))?;
            indatums = new_indatums(attcnt, batch_size);
            totalrows += inrownum as u64;
            inrownum = 0;
        }
    }
    if inrownum > 0 {
        for indatum in &mut indatums {
            indatum.set_len(inrownum as u32);
        }
        datas.send((indatums, inrownum as u32))?;
        totalrows += inrownum as u64;
    }
    drop(datas);

    let mut l0newmeta = Vec::with_capacity(opts.parallel);
    for (workexit, filemeta) in workerrec.iter() {
        let filemeta = filemeta?;
        l0newmeta.push(filemeta);
        sess.exit_worker(workexit);
    }
    forget(worker_exit_guard);
    sv::commit_write(sess, &svslot, &l0newmeta);
    forget(abort_guard);
    return Ok(totalrows);
}

fn parse_copyopts<'syn>(copy: &'syn syn::CopyStmt<'_>) -> anyhow::Result<CopyOpts<'syn>> {
    let mut delim = "";
    let mut parallel = 1usize;
    let mut null = "";
    for defelem in &copy.opts {
        let (name, val) = match defelem {
            syn::DefElem::Unspec(v) | syn::DefElem::Add(v) => (&v.defname, &v.arg),
            _ => continue,
        };
        let name: &str = name;
        match name {
            "format" => match val {
                syn::Value::Str(s) if s.as_str() == "csv" => continue,
                _ => {
                    kbbail!(
                        ERRCODE_INVALID_PARAMETER_VALUE,
                        "COPY format {} not recognized",
                        val
                    );
                }
            },
            "delimiter" => match val {
                syn::Value::Str(val) => {
                    delim = val;
                }
                _ => {
                    kbbail!(
                        ERRCODE_INVALID_PARAMETER_VALUE,
                        "COPY delimiters {} not recognized",
                        val
                    );
                }
            },
            "parallel" => match val {
                syn::Value::Num(syn::NumVal::Int(n)) => {
                    parallel = *n as usize;
                }
                _ => {
                    kbbail!(
                        ERRCODE_INVALID_PARAMETER_VALUE,
                        "COPY parallel {} not recognized",
                        val
                    );
                }
            },
            "null" => match val {
                syn::Value::Str(val) => {
                    null = val;
                }
                _ => {
                    kbbail!(
                        ERRCODE_INVALID_PARAMETER_VALUE,
                        "COPY null {} not recognized",
                        val
                    );
                }
            },
            &_ => {
                kbbail!(ERRCODE_SYNTAX_ERROR, "option {} not recognized", name);
            }
        }
    }
    return Ok(CopyOpts {
        delim,
        parallel,
        null,
    });
}

pub fn copy_stmt(sess: &mut SessionState, copy: &syn::CopyStmt<'_>) -> anyhow::Result<Response> {
    kbensure!(
        copy.from,
        ERRCODE_FEATURE_NOT_SUPPORTED,
        "COPY TO is not supported"
    );
    let copyopts = parse_copyopts(copy)?;
    let input = File::open(copy.filename.as_str())?;
    let processed = copyfrom(&copy.rel, BufReader::new(input), &copyopts, sess)?;
    return Ok(Response::new_str(format!("COPY {}", processed)));
}

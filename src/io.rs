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

use kbio::{cqeres2rust, ready, CQEFuture, Uring};
use std::future::Future;
use std::io::{self, IoSlice};
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct OPFuture<'uring> {
    fut: CQEFuture<'uring>,
    #[cfg(debug_assertions)]
    bufaddr: u64,
    #[cfg(debug_assertions)]
    buflen: u32,
}

pub struct Stream<'uring> {
    uring: &'uring Uring,
    fd: i32,
    read_fut: Option<OPFuture<'uring>>,
    write_fut: Option<OPFuture<'uring>>,
    shutdown_fut: Option<OPFuture<'uring>>,
}

impl<'uring, 'arg> Stream<'uring> {
    pub fn new(uring: &'uring Uring, fd: i32) -> Stream<'uring> {
        Self {
            uring,
            fd,
            read_fut: None,
            write_fut: None,
            shutdown_fut: None,
        }
    }
}

/*
fn readbuf_as_bytes<'a>(buf: &'a mut ReadBuf<'_>) -> &'a mut [u8] {
    unsafe { &mut *(buf.unfilled_mut() as *mut _ as *mut [u8]) }
}
*/

macro_rules! readbuf_as_bytes {
    ($buf: ident) => {
        unsafe { &mut *($buf.unfilled_mut() as *mut _ as *mut [u8]) }
    };
}

impl<'u> AsyncRead for Stream<'u> {
    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    fn poll_read<'l1, 'l2, 'l3, 'l4, 'l5>(
        mut self: Pin<&'l1 mut Stream<'u>>,
        cx: &'l2 mut Context<'l3>,
        buf: &'l4 mut ReadBuf<'l5>,
    ) -> Poll<io::Result<()>> {
        fn on_ready(res: i32, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let res = cqeres2rust(res).map(|n| {
                let n = n as usize;
                unsafe { buf.assume_init(n) };
                buf.advance(n);
                return ();
            });
            return Poll::Ready(res);
        }

        #[cfg(debug_assertions)]
        let bufaddr = readbuf_as_bytes!(buf).as_ptr() as usize as u64;
        #[cfg(debug_assertions)]
        let buflen = readbuf_as_bytes!(buf).len() as u32;
        match &mut self.read_fut {
            None => {
                // SAFETY#1
                // Using readbuf_as_bytes(buf) will cause the error:
                // cannot infer an appropriate lifetime for lifetime parameter..
                //
                // if we use readbuf_as_bytes(), the type of buff is `&'l4`,
                // the type of fut and opfut are `CQEFuture<min('u, 'l4)>`,
                // and code at '#1' requires that opfut outlives self.read_fut whose type is `CQEFuture<'u>`,
                // which means that `min('u, 'l4)` should outlive `'a`, and 'l4 should outlive `'a`.
                //
                // If we specify `'l4: 'a`, we will get an error:
                // lifetimes do not match method in trait!
                let buff = readbuf_as_bytes!(buf);
                let mut fut = self.uring.recv_fut(self.fd, buff);
                match Pin::new(&mut fut).poll(cx) {
                    Poll::Pending => {
                        let opfut = OPFuture {
                            fut,
                            #[cfg(debug_assertions)]
                            bufaddr,
                            #[cfg(debug_assertions)]
                            buflen,
                        };
                        self.read_fut = Some(opfut); // #1
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        return on_ready(res, buf);
                    }
                }
            }
            Some(fut) => {
                #[cfg(debug_assertions)]
                {
                    debug_assert_eq!(bufaddr, fut.bufaddr);
                    debug_assert_eq!(buflen, fut.buflen);
                }
                let res = ready!(Pin::new(&mut fut.fut).poll(cx));
                self.read_fut = None;
                return on_ready(res, buf);
            }
        }
    }
}

impl<'u> AsyncWrite for Stream<'u> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        #[cfg(debug_assertions)]
        let bufaddr = buf.as_ptr() as usize as u64;
        #[cfg(debug_assertions)]
        let buflen = buf.len() as u32;
        fn on_ready(res: i32) -> Poll<io::Result<usize>> {
            Poll::Ready(cqeres2rust(res).map(|v| v as usize))
        }
        match &mut self.write_fut {
            None => {
                // SAFETY: See SAFETY#1
                let buf = unsafe { &*(buf as *const _) };
                let mut fut = self.uring.write_fut(self.fd, buf);
                match Pin::new(&mut fut).poll(cx) {
                    Poll::Pending => {
                        let opfut = OPFuture {
                            fut,
                            #[cfg(debug_assertions)]
                            bufaddr,
                            #[cfg(debug_assertions)]
                            buflen,
                        };
                        self.write_fut = Some(opfut);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        return on_ready(res);
                    }
                }
            }
            Some(fut) => {
                #[cfg(debug_assertions)]
                {
                    debug_assert_eq!(bufaddr, fut.bufaddr);
                    debug_assert_eq!(buflen, fut.buflen);
                }
                let res = ready!(Pin::new(&mut fut.fut).poll(cx));
                self.write_fut = None;
                return on_ready(res);
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Stream has no buffer.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        fn on_ready(res: i32) -> Poll<io::Result<()>> {
            Poll::Ready(cqeres2rust(res).map(|v| ()))
        }
        match &mut self.shutdown_fut {
            None => {
                let mut fut = self.uring.shutdown_fut(self.fd, Shutdown::Write);
                match Pin::new(&mut fut).poll(cx) {
                    Poll::Pending => {
                        let opfut = OPFuture {
                            fut,
                            #[cfg(debug_assertions)]
                            bufaddr: 0,
                            #[cfg(debug_assertions)]
                            buflen: 0,
                        };
                        self.shutdown_fut = Some(opfut);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        return on_ready(res);
                    }
                }
            }
            Some(fut) => {
                let res = ready!(Pin::new(&mut fut.fut).poll(cx));
                self.shutdown_fut = None;
                return on_ready(res);
            }
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        fn on_ready(res: i32) -> Poll<io::Result<usize>> {
            Poll::Ready(cqeres2rust(res).map(|v| v as usize))
        }
        let bufaddr = bufs.as_ptr() as usize;
        let buflen = bufs.len();
        match &mut self.write_fut {
            None => {
                // SAFETY: See SAFETY#1
                let buf = unsafe { std::slice::from_raw_parts(bufaddr as *const _, buflen) };
                let mut fut = self.uring.writev_fut(self.fd, buf, 0);
                match Pin::new(&mut fut).poll(cx) {
                    Poll::Pending => {
                        let opfut = OPFuture {
                            fut,
                            #[cfg(debug_assertions)]
                            bufaddr: bufaddr as u64,
                            #[cfg(debug_assertions)]
                            buflen: buflen as u32,
                        };
                        self.write_fut = Some(opfut);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        return on_ready(res);
                    }
                }
            }
            Some(fut) => {
                #[cfg(debug_assertions)]
                {
                    debug_assert_eq!(bufaddr as u64, fut.bufaddr);
                    debug_assert_eq!(buflen as u32, fut.buflen);
                }
                let res = ready!(Pin::new(&mut fut.fut).poll(cx));
                self.write_fut = None;
                return on_ready(res);
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}
/*

!!!!!!!PREMATURE OPTIMIZATION IS THE ROOT OF ALL EVIL!!!!!!!

use kbio::{cqeres2rust, io_uring_sqe, ready, PromiseSlot, Uring, IORING_OP_RECV, IORING_OP_WRITE};
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;
use tokio::io::{AsyncWrite, ReadBuf};

enum RWState {
    None,
    Init(io_uring_sqe),
    #[cfg(not(debug_assertions))]
    Wait(NonNull<PromiseSlot>),
    #[cfg(debug_assertions)]
    Wait(
        NonNull<PromiseSlot>,
        /* buf ptr */ u64,
        /* buf len */ u32,
    ),
}

enum WState {
    None,
    Init(io_uring_sqe),

}

struct TcpStream<'a> {
    uring: &'a Uring,
    fd: i32,
    rstate: RWState,
    wstate: RWState,
}

impl<'a> TcpStream<'a> {
    pub fn new(uring: &'a Uring, fd: i32) -> Self {
        Self {
            uring,
            fd,
            rstate: RWState::None,
            wstate: RWState::None,
        }
    }

    fn on_wwait(
        &mut self,
        slot: NonNull<PromiseSlot>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        todo!()
    }

    fn on_rwait(
        &mut self,
        slot: NonNull<PromiseSlot>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        #[cfg(debug_assertions)]
        {
            if let RWState::Wait(xslot, bufaddr, buflen) = self.rstate {
                let buffer = readbuf_as_bytes(buf);
                debug_assert_eq!(bufaddr, buffer.as_ptr() as usize as u64);
                debug_assert_eq!(buflen as usize, buffer.len());
                debug_assert_eq!(xslot, slot);
            } else {
                debug_assert!(false);
            }
        }
        let res = ready!(unsafe { self.uring.poll(slot, cx) });
        let res = cqeres2rust(res).map(|n| {
            let n = n as usize;
            unsafe { buf.assume_init(n) };
            buf.advance(n);
            return ();
        });
        self.rstate = RWState::None;
        return Poll::Ready(res);
    }
}

fn new_sqe(opcode: u8, fd: i32, addr: u64, buflen: u32) -> io_uring_sqe {
    let mut sqe = io_uring_sqe::default();
    sqe.opcode = opcode;
    sqe.fd = fd;
    sqe.__bindgen_anon_2.addr = addr;
    sqe.len = buflen;
    return sqe;
}

#[cfg(debug_assertions)]
fn sqe_addr(sqe: &io_uring_sqe) -> u64 {
    unsafe { sqe.__bindgen_anon_2.addr }
}

fn readbuf_as_bytes<'a, 'b>(buf: &'a mut ReadBuf<'b>) -> &'a mut [u8] {
    unsafe { &mut *(buf.unfilled_mut() as *mut _ as *mut [u8]) }
}

impl AsyncRead for TcpStream<'_> {
    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &self.rstate {
            RWState::None => {
                let buffer = readbuf_as_bytes(buf);
                let bufaddr = buffer.as_ptr() as usize as u64;
                let fd = self.fd;
                let sqe = new_sqe(IORING_OP_RECV as u8, fd, bufaddr, buffer.len() as u32);
                if let Some(slot) = self.uring.submit(&sqe) {
                    #[cfg(not(debug_assertions))]
                    {
                        self.rstate = RWState::Wait(slot);
                    }
                    #[cfg(debug_assertions)]
                    {
                        self.rstate = RWState::Wait(slot, sqe_addr(&sqe), sqe.len);
                    }
                    return self.on_rwait(slot, cx, buf);
                }
                self.rstate = RWState::Init(sqe);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            RWState::Init(sqe) => {
                #[cfg(debug_assertions)]
                {
                    let buffer = readbuf_as_bytes(buf);
                    let bufaddr = buffer.as_ptr() as usize as u64;
                    debug_assert_eq!(bufaddr, sqe_addr(sqe));
                    debug_assert_eq!(buffer.len(), sqe.len as usize);
                }
                if let Some(slot) = self.uring.submit(sqe) {
                    #[cfg(not(debug_assertions))]
                    {
                        self.rstate = RWState::Wait(slot);
                    }
                    #[cfg(debug_assertions)]
                    {
                        self.rstate = RWState::Wait(slot, sqe_addr(sqe), sqe.len);
                    }
                    return self.on_rwait(slot, cx, buf);
                }
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            #[cfg(not(debug_assertions))]
            &RWState::Wait(slot) => {
                return self.on_rwait(slot, cx, buf);
            }
            #[cfg(debug_assertions)]
            &RWState::Wait(slot, _, _) => {
                return self.on_rwait(slot, cx, buf);
            }
        }
    }
}

*/

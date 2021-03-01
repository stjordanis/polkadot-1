// Copyright 2021 Parity Technologies (UK) Ltd.
// This file is part of Polkadot.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Polkadot.  If not, see <http://www.gnu.org/licenses/>.

use crate::Priority;
use async_std::{
	io,
	os::unix::net::{UnixListener, UnixStream},
	path::{PathBuf, Path},
};
use futures::{
	AsyncRead, AsyncWrite, AsyncReadExt as _, AsyncWriteExt as _, FutureExt as _, StreamExt as _,
	channel::mpsc,
};
use futures_timer::Delay;
use std::{
	mem,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};
use pin_project::pin_project;

#[derive(Debug)]
pub struct IdleWorker {
	/// The stream to which the child process is connected.
	stream: UnixStream,
}

pub enum SpawnErr {
	Bind,
	Accept,
	ProcessSpawn,
}

/// This is a representation of a potentially running worker. Drop it and the process will be killed.
///
/// A worker's handle is also a future that resolves when it's detected that the worker's process
/// has been terminated.
///
/// This future relies on the fact that a child process's stdout fd is closed upon it's termination.
#[pin_project]
pub struct WorkerHandle {
	child: async_process::Child,
	#[pin]
	stdout: async_process::ChildStdout,
	drop_box: Box<[u8]>,
}

impl WorkerHandle {
	fn spawn(program: &str, socket_path: impl AsRef<Path>) -> io::Result<Self> {
		let mut child = async_process::Command::new(program)
			// TODO: args
			.stdout(async_process::Stdio::piped())
			.kill_on_drop(true)
			.spawn()?;

		let stdout = child
			.stdout
			.take()
			.expect("the process spawned with piped stdout should have the stdout handle");

		Ok(WorkerHandle {
			child,
			stdout,
			// We don't expect the bytes to be ever read. But in case we do, we should not use a buffer
			// of a small size, because otherwise if the child process does return any data we will end up
			// issuing a syscall for each byte. We also prefer not to do allocate that on the stack, since
			// each poll the buffer will be allocated and initialized (and that's due poll_read takes &mut [u8]
			// and there are no guarantees that a `poll_read` won't ever read from there even though that's
			// unlikely).
			//
			// OTOH, we also don't want to be super smart here and we could just afford to allocate a buffer
			// for that here.
			drop_box: vec![0; 8192].into_boxed_slice(),
		})
	}

	/// Returns `pid` of the worker process.
	pub fn id(&self) -> u32 {
		self.child.id()
	}
}

impl futures::Future for WorkerHandle {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		use futures_lite::io::AsyncRead;

		let me = self.project();
		match futures::ready!(AsyncRead::poll_read(me.stdout, cx, &mut *me.drop_box)) {
			Ok(0) => {
				// 0 means EOF means the child was terminated. Resolve.
				Poll::Ready(())
			}
			Ok(_bytes_read) => {
				// weird, we've read something. Pretend that never happened and reschedule ourselves.
				cx.waker().wake_by_ref();
				Poll::Pending
			}
			Err(_) => {
				// The implementation is guaranteed to not to return WouldBlock and Interrupted. This
				// leaves us with a legit errors which we suppose were due to termination.
				Poll::Ready(())
			}
		}
	}
}

pub async fn spawn() -> Result<(IdleWorker, WorkerHandle), SpawnErr> {
	let socket_path = transient_socket_path();
	let listener = UnixListener::bind(&socket_path)
		.await
		.map_err(|_| SpawnErr::Bind)?;
	let mut handle = WorkerHandle::spawn("", socket_path).map_err(|_| SpawnErr::ProcessSpawn)?;
	let (mut stream, _) = listener.accept().await.map_err(|_| SpawnErr::Accept)?;
	Ok((IdleWorker { stream }, handle))
}

fn transient_socket_path() -> PathBuf {
	let mut temp_dir = std::env::temp_dir();
	temp_dir.push(format!("pvf-prepare-{}", "")); // TODO: see tempfile impl
	todo!()
}

pub enum Outcome {
	/// The worker has finished the work assigned to it.
	Concluded(IdleWorker),
	/// The execution was interrupted abruptly and the worker is not available anymore. For example,
	/// this could've happen because the worker hadn't finished the work until the given deadline.
	DidntMakeIt,
}

pub async fn start_work(worker: IdleWorker, code: Arc<Vec<u8>>, artifact_path: PathBuf) -> Outcome {
	let IdleWorker { mut stream } = worker;

	if let Err(err) = write_request(&mut stream, code, artifact_path).await {
		// TODO: Log
		return Outcome::DidntMakeIt;
	}

	// Wait for the result from the worker, keeping in mind that there may be a timeout, the
	// worker may get killed, or something along these lines.
	//
	// In that case we should handle these gracefully by writing the artifact file by ourselves.
	// We may potentially overwrite the artifact in rare cases where the worker didn't make
	// it to report back the result.

	enum Selected {
		Read(io::Result<()>),
		Deadline,
	}

	let selected = futures::select! {
		ack_read = framed_read(&mut stream).fuse() => Selected::Read(ack_read.map(|_| ())),
		_ = Delay::new(Duration::from_secs(3)).fuse() => Selected::Deadline,
	};

	// TODO: if deadline was reached or the error has happened then treat it as didn't make it.
	// In any case of the error we must write the artifact path ourselves.

	todo!()
}

async fn write_request(
	stream: &mut UnixStream,
	code: Arc<Vec<u8>>,
	artifact_path: PathBuf,
) -> io::Result<()> {
	framed_write(stream, &*code).await?;
	framed_write(stream, path_bytes(&artifact_path)).await?;
	Ok(())
}

/// Convert the given path into a byte buffer.
fn path_bytes(path: &Path) -> &[u8] {
	// Ideally, we take the OsStr of the path, send that and reconstruct this on the other side.
	// However, libstd doesn't provide us with such an option. There are crates out there that
	// allow for extraction of a path, but TBH it doesn't seem to be a real issue.
	//
	// However, should be there reports we can incorporate such a crate here.
	path.to_str().expect("non-UTF-8 path").as_bytes()
}

async fn framed_write(w: &mut (impl AsyncWrite + Unpin), buf: &[u8]) -> io::Result<()> {
	let len_buf = buf.len().to_le_bytes();
	w.write_all(&len_buf).await?;
	w.write_all(buf).await;
	Ok(())
}

async fn framed_read(r: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
	let mut len_buf = [0u8; mem::size_of::<usize>()];
	r.read_exact(&mut len_buf).await?;
	let len = usize::from_le_bytes(len_buf);
	let mut buf = vec![0; len];
	r.read_exact(&mut buf).await?;
	Ok(buf)
}

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
use rand::Rng;
use std::{
	borrow::Cow,
	mem,
	pin::Pin,
	str::{FromStr, from_utf8},
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};
use pin_project::pin_project;

const NICENESS_BACKGROUND: i32 = 5;
const NICENESS_FOREGROUND: i32 = 0;

#[derive(Debug)]
pub struct IdleWorker {
	/// The stream to which the child process is connected.
	stream: UnixStream,
	pid: u32,
}

#[derive(Debug)]
pub enum SpawnErr {
	Bind,
	Accept,
	ProcessSpawn,
	CurrentExe,
	AcceptTimeout,
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
	fn spawn(
		program: &str,
		extra_args: &[&str],
		socket_path: impl AsRef<Path>,
	) -> io::Result<Self> {
		let mut child = async_process::Command::new(program)
			.args(extra_args)
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

	/// Returns the process id of this worker.
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
	// NOTE: `current_exe` is known to be prone to priviledge escalation exploits if used
	//        with a hard link as suggested in its rustdoc.
	//
	//        However, I believe this is not very relevant to us since the exploitation requires
	//        the binary to be under suid and which is a bad idea anyway. Furthermore, our
	//        security model assumes that an attacker doesn't have access to the local machine.
	let current_exe = std::env::current_exe().map_err(|_| SpawnErr::CurrentExe)?;
	let program_path = current_exe.to_string_lossy();
	spawn_with_program_path(&*program_path, &[]).await
}

/// Exposed only for integration tests. Use [`spawn`] instead.
#[doc(hidden)]
pub async fn spawn_with_program_path(
	program_path: &str,
	extra_args: &[&str],
) -> Result<(IdleWorker, WorkerHandle), SpawnErr> {
	let socket_path = transient_socket_path();
	let listener = UnixListener::bind(&socket_path)
		.await
		.map_err(|_| SpawnErr::Bind)?;

	let mut handle = WorkerHandle::spawn(&*program_path, extra_args, socket_path)
		.map_err(|_| SpawnErr::ProcessSpawn)?;

	futures::select! {
		accept_result = listener.accept().fuse() => {
			let (stream, _) = accept_result.map_err(|_| SpawnErr::Accept)?;
			Ok((IdleWorker { stream, pid: handle.id() }, handle))
		}
		_ = Delay::new(Duration::from_secs(3)).fuse() => {
			Err(SpawnErr::AcceptTimeout)
		}
	}
}

fn transient_socket_path() -> PathBuf {
	use std::ffi::OsString;
	use rand::distributions::Alphanumeric;

	const PREFIX: &[u8] = b"pvf-prepare-";
	const DESCRIMINATOR_LEN: usize = 10;

	let mut buf = Vec::with_capacity(PREFIX.len() + DESCRIMINATOR_LEN);
	buf.extend(PREFIX);
	buf.extend(
		rand::thread_rng()
			.sample_iter(&Alphanumeric)
			.take(DESCRIMINATOR_LEN),
	);

	let s = std::str::from_utf8(&buf).expect("the string is collected from a valid utf-8 sequence");

	let mut temp_dir = PathBuf::from(std::env::temp_dir());
	temp_dir.push(s);
	temp_dir
}

pub enum Outcome {
	/// The worker has finished the work assigned to it.
	Concluded(IdleWorker),
	/// The execution was interrupted abruptly and the worker is not available anymore. For example,
	/// this could've happen because the worker hadn't finished the work until the given deadline.
	DidntMakeIt,
}

pub async fn start_work(
	worker: IdleWorker,
	code: Arc<Vec<u8>>,
	artifact_path: PathBuf,
	background_priority: bool,
) -> Outcome {
	let IdleWorker { mut stream, pid } = worker;

	if background_priority {
		renice(pid, NICENESS_BACKGROUND);
	}

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

	// TODO: if it is concluded we should restore the previous niceness value.

	todo!()
}

fn renice(pid: u32, niceness: i32) {
	// TODO: upstream to nix
	unsafe {
		if -1 == libc::setpriority(libc::PRIO_PROCESS, pid, niceness) {
			let err = std::io::Error::last_os_error();
			drop(err); // TODO: warn
		}
	}
}

pub fn worker_entrypoint(socket_path: &str) {
	let err = async_std::task::block_on(async {
		let mut stream = UnixStream::connect(socket_path).await?;

		loop {
			let code = framed_read(&mut stream).await?;
			let artifact_path = framed_read(&mut stream).await?;
			let artifact_path = bytes_to_path(&artifact_path);

			// TODO: actual workload

			framed_write(&mut stream, b"ack").await?;
		}

		io::Result::<()>::Ok(())
	})
	.unwrap_err();

	// TODO: proper handling
	drop(err);
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

fn bytes_to_path(bytes: &[u8]) -> PathBuf {
	let str_buf = std::str::from_utf8(bytes).unwrap(); // TODO:
	PathBuf::from_str(&str_buf).unwrap()
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

#[cfg(test)]
mod tests {
	// The logic is actually exercised using an integration test under `tests/it.rs`
}

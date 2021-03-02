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

use crate::{
	Priority,
	worker_common::{
		IdleWorker, SpawnErr, WorkerHandle, bytes_to_path, framed_read, framed_write,
		path_to_bytes, spawn_with_program_path, tmpfile,
	},
};
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

pub enum Outcome {
	/// The worker has finished the work assigned to it.
	Concluded(IdleWorker),
	/// The execution was interrupted abruptly and the worker is not available anymore. For example,
	/// this could've happen because the worker hadn't finished the work until the given deadline.
	///
	/// This doesn't return an idle worker instance, thus this worker is no longer usable.
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

	if let Err(err) = write_request(&mut stream, code).await {
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
		Done,
		IoErr,
		Deadline,
	}

	let selected = futures::select! {
		artifact_path_bytes = framed_read(&mut stream).fuse() => {
			match artifact_path_bytes {
				Ok(bytes) => {
					let tmp_path = bytes_to_path(&bytes);
					async_std::fs::rename(tmp_path, artifact_path)
						.await
						.map(|_| Selected::Done)
						.unwrap_or(Selected::IoErr)
				},
				Err(_) => Selected::IoErr,
			}
		},
		_ = Delay::new(Duration::from_secs(3)).fuse() => Selected::Deadline,
	};

	match selected {
		Selected::Done => {
			renice(pid, NICENESS_FOREGROUND);
			Outcome::Concluded(IdleWorker { stream, pid })
		}
		Selected::IoErr | Selected::Deadline => Outcome::DidntMakeIt,
	}
}

// TODO: rename to `send` and `recv`.

async fn write_request(stream: &mut UnixStream, code: Arc<Vec<u8>>) -> io::Result<()> {
	framed_write(stream, &*code).await?;
	Ok(())
}

async fn read_request(stream: &mut UnixStream) -> io::Result<Vec<u8>> {
	let code = framed_read(stream).await?;
	Ok(code)
}

pub fn bump_priority(handle: &WorkerHandle) {
	let pid = handle.id();
	renice(pid, NICENESS_FOREGROUND);
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
			let code = read_request(&mut stream).await?;
			let artifact_contents = crate::executor_intf::prepare(&code);

			let dest = tmpfile("prepare-artifact-");
			async_std::fs::write(&dest, &artifact_contents).await?;

			framed_write(&mut stream, &path_to_bytes(&dest)).await?;
		}

		io::Result::<()>::Ok(())
	})
	.unwrap_err();

	// TODO: proper handling
	drop(err);
}

#[cfg(test)]
mod tests {
	// The logic is actually exercised using an integration test under `tests/it.rs`
}

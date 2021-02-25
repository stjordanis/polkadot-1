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
use async_std::prelude::*;
use async_std::{
	io,
	os::unix::net::{UnixListener, UnixStream},
	path::PathBuf,
};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, StreamExt, channel::mpsc};
use futures_timer::Delay;
use std::{
	mem,
	path::Path,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};

pub struct AvailableWorker {
	stream: UnixStream,
}

pub async fn spawn(socket_path: String) -> io::Result<AvailableWorker> {
	let listener = UnixListener::bind(socket_path).await?;
	let (mut stream, _) = listener.accept().await?;

	Ok(AvailableWorker { stream })
}

pub enum Outcome {
	/// The worker has finished the work assigned to it.
	Concluded(AvailableWorker),
	/// The execution was interrupted abruptly and the worker is not available anymore. For example,
	/// this could've happen because the worker hadn't finished the work until the given deadline.
	DidntMakeIt,
}

pub async fn start_work(
	worker: AvailableWorker,
	code: Arc<Vec<u8>>,
	artifact_path: PathBuf,
	background_priority: bool,
) -> io::Result<()> {
	let AvailableWorker { stream } = worker;

	// TODO: handle the priority stuff.

	framed_write(&mut stream, &*code).await?;
	framed_write(&mut stream, path_bytes(&artifact_path)).await?;

	// Wait for the result from the worker, keeping in mind that there may be a timeout, the
	// worker may get killed, or something along these lines.
	//
	// In that case we should handle these gracefully by writing the artifact file by ourselves.
	// We may potentially overwrite the artifact in rare cases where the worker didn't make
	// it to report back the result.
	let ack_read = framed_read(&mut stream);
	let deadline = Delay::new(Duration::from_secs(3)); // TODO: const or configurable

	enum Selected {
		Read(io::Result<()>),
		Deadline,
	}

	let selected = futures::select! {
		ack_read = ack_read => Selected::Read(ack_read.map(|_| ())),
		_ = deadline => Selected::Deadline,
	};

	// TODO: if deadline was reached or the error has happened then treat it as didn't make it.
	// In any case of the error we must write the artifact path ourselves.

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
	w.write_all(buf);
	Ok(())
}

async fn framed_read(r: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
	let len_buf = [0usize; mem::size_of::<usize>()];
	r.read_exact(&mut len_buf).await?;
	let len = usize::from_le_bytes(&len_buf);
	let mut buf = vec![0; len];
	r.read_exact(&mut buf).await?;
	Ok(buf)
}

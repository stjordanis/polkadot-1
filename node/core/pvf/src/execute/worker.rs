use std::time::Instant;

use crate::worker_common::{
	IdleWorker, SpawnErr, WorkerHandle, bytes_to_path, framed_read, framed_write, path_to_bytes,
	spawn_with_program_path,
};
use async_std::{
	io,
	os::unix::net::UnixStream,
	path::{Path, PathBuf},
};

pub async fn spawn() -> Result<(IdleWorker, WorkerHandle), SpawnErr> {
	let current_exe = std::env::current_exe().map_err(|_| SpawnErr::CurrentExe)?;
	let program_path = current_exe.to_string_lossy();
	spawn_with_program_path(&*program_path, &[]).await
}

pub enum Outcome {
	ValidationResult {
		// TODO: the result itself, Ok or InvalidCandidate
		soft_timeout: bool,
		idle_worker: IdleWorker,
	},
	HardTimeout,
	// TODO: In this case we should try to recompile the binary?
	Deserialization,
	Error,
}

pub async fn start_work(
	worker: IdleWorker,
	artifact_path: PathBuf,
	validation_params: Vec<u8>,
) -> Outcome {
	let IdleWorker { mut stream, pid } = worker;

	if let Err(err) = write_request(&mut stream, &artifact_path, &validation_params).await {
		return Outcome::Error;
	}

	// futures::select! {

	// }

	todo!()
}

async fn write_request(
	stream: &mut UnixStream,
	artifact_path: &Path,
	validation_params: &[u8],
) -> io::Result<()> {
	framed_write(stream, path_to_bytes(artifact_path)).await?;
	framed_write(stream, validation_params).await?;
	Ok(())
}

async fn read_request(stream: &mut UnixStream) -> io::Result<(PathBuf, Vec<u8>)> {
	let artifact_path = framed_read(stream).await?;
	let artifact_path = bytes_to_path(&artifact_path);
	let params = framed_read(stream).await?;
	Ok((artifact_path, params))
}

pub fn worker_entrypoint(socket_path: &str) {
	let err = async_std::task::block_on(async {
		let mut stream = UnixStream::connect(socket_path).await?;

		loop {
			let (artifact_path, params) = read_request(&mut stream).await?;

			let validation_started_at = Instant::now();

			// TODO:
			// - Load the artifact
			// - Create an sc-executor-wasmtime instance from it
			// - Execute it with the given params.

			// TODO: if `validation_started_at` exceeded the soft deadline threshold, we should
			// set soft_timeout = true.

			// TODO: write the response
		}

		io::Result::<()>::Ok(())
	})
	.unwrap_err();

	drop(err)
}

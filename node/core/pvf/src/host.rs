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

use std::{
	collections::{HashMap, hash_map::Entry},
	sync::Arc,
	time::SystemTime,
};
use async_std::path::{Path, PathBuf};
use polkadot_parachain::{
	primitives::ValidationResult,
	wasm_executor::{InternalError, ValidationError},
};
use crate::{Priority, Pvf, artifacts::ArtifactId, execute, prepare};
use futures::{
	Future, FutureExt, SinkExt, StreamExt,
	channel::{mpsc, oneshot},
	future::BoxFuture,
	stream::FuturesUnordered,
};
use polkadot_core_primitives::Hash;

pub struct ValidationHost {
	from_handle_tx: mpsc::Sender<FromHandle>,
}

impl ValidationHost {
	pub async fn execute_pvf(
		&mut self,
		pvf: Pvf,
		params: &[u8],
		priority: Priority,
	) -> Result<ValidationResult, ValidationError> {
		let (result_tx, result_rx) =
			oneshot::channel::<Result<ValidationResult, ValidationError>>();

		self.from_handle_tx
			.send(FromHandle::ExecutePvf {
				pvf,
				params: params.to_owned(),
				priority,
				result_tx,
			})
			.await
			.map_err(|_| {
				ValidationError::Internal(InternalError::System("the inner loop hung up".into()))
			})?;
		result_rx.await.map_err(|_| {
			ValidationError::Internal(InternalError::System("the result was dropped".into()))
		})?
	}
}

enum FromHandle {
	ExecutePvf {
		pvf: Pvf,
		params: Vec<u8>,
		priority: Priority,
		result_tx: oneshot::Sender<Result<ValidationResult, ValidationError>>,
	},
	HeadsUp {
		active_pvfs: Vec<Pvf>,
	},
}

pub fn start(cache_path: &Path) -> (ValidationHost, impl Future<Output = ()>) {
	let cache_path = cache_path.to_owned();

	let (from_handle_tx, from_handle_rx) = mpsc::channel(10);

	let validation_host = ValidationHost { from_handle_tx };

	let (to_prepare_pool, from_prepare_pool, run_prepare_pool) = prepare::start_pool();

	let soft_capacity = 5;
	let hard_capacity = 8;
	let (to_prepare_queue_tx, from_prepare_queue_rx, run_prepare_queue) = prepare::start_queue(
		soft_capacity,
		hard_capacity,
		cache_path.clone(),
		to_prepare_pool,
		from_prepare_pool,
	);

	let (to_execute_queue_tx, run_execute_queue) = execute::start();

	let run = async move {
		let artifacts = Artifacts::new(cache_path.to_owned()).await;

		run(
			Inner {
				from_handle_rx,
				to_prepare_queue_tx,
				from_prepare_queue_rx,
				to_execute_queue_tx,
				artifacts,
			},
			run_prepare_pool,
			run_prepare_queue,
			run_execute_queue,
		)
		.await
	};

	(validation_host, run)
}

struct Inner {
	from_handle_rx: mpsc::Receiver<FromHandle>,

	to_prepare_queue_tx: mpsc::Sender<prepare::ToQueue>,
	from_prepare_queue_rx: mpsc::UnboundedReceiver<prepare::FromQueue>,

	to_execute_queue_tx: mpsc::Sender<execute::ToQueue>,

	artifacts: Artifacts,
}

async fn run(
	Inner {
		from_handle_rx,
		from_prepare_queue_rx,
		mut to_prepare_queue_tx,
		mut to_execute_queue_tx,
		mut artifacts,
		..
	}: Inner,
	prepare_pool: impl Future<Output = ()>,
	prepare_queue: impl Future<Output = ()>,
	execute_pool: impl Future<Output = ()>,
) {
	futures::pin_mut!(prepare_queue, prepare_pool, execute_pool);

	let mut from_handle_rx = from_handle_rx.fuse();
	let mut from_prepare_queue_rx = from_prepare_queue_rx.fuse();

	loop {
		if futures::poll!(&mut prepare_queue).is_ready()
			|| futures::poll!(&mut prepare_pool).is_ready()
			|| futures::poll!(&mut execute_pool).is_ready()
		{
			// TODO: Shouldn't happen
		}

		futures::select! {
			from_handle = from_handle_rx.select_next_some() => {
				handle_from_handle(
					&mut artifacts,
					&mut to_prepare_queue_tx,
					&mut to_execute_queue_tx,
					from_handle,
				)
				.await;
			},
			prepare::FromQueue::Prepared(artifact_id) = from_prepare_queue_rx.select_next_some() => {
				// Note that preparation always succeeds.
				//
				// That's because the error conditions are written into the artifact and will be
				// reported at the time of the  execution. It potentially, but not necessarily,
				// can be scheduled as a result of this function call, in case there are pending
				// executions.
				//
				// We could be eager in terms of reporting and plumb the result from the prepartion
				// worker but we don't for the sake of simplicity.
				handle_prepare_done(
					&mut artifacts,
					&mut to_execute_queue_tx,
					artifact_id,
				).await;
			},
		}
	}
}

async fn handle_from_handle(
	artifacts: &mut Artifacts,
	prepare_queue: &mut mpsc::Sender<prepare::ToQueue>,
	execute_queue: &mut mpsc::Sender<execute::ToQueue>,
	from_handle: FromHandle,
) {
	match from_handle {
		FromHandle::ExecutePvf {
			pvf,
			params,
			priority,
			result_tx,
		} => {
			handle_execute_pvf(
				artifacts,
				prepare_queue,
				execute_queue,
				pvf,
				params,
				priority,
				result_tx,
			)
			.await;
		}
		FromHandle::HeadsUp { active_pvfs } => {
			handle_heads_up(artifacts, prepare_queue, active_pvfs).await;
		}
	}
}

async fn handle_execute_pvf(
	artifacts: &mut Artifacts,
	prepare_queue: &mut mpsc::Sender<prepare::ToQueue>,
	execute_queue: &mut mpsc::Sender<execute::ToQueue>,
	pvf: Pvf,
	params: Vec<u8>,
	priority: Priority,
	result_tx: oneshot::Sender<Result<ValidationResult, ValidationError>>,
) {
	let artifact_id = pvf.to_artifact_id();

	match artifacts.artifacts.entry(artifact_id.clone()) {
		Entry::Occupied(mut o) => match *o.get_mut() {
			ArtifactState::Prepared {
				ref artifact_path, ..
			} => {
				execute_queue.send(execute::ToQueue::Enqueue {
					artifact_path: artifact_path.clone(),
					params,
					result_tx,
				});
			}
			ArtifactState::Preparing {
				ref mut pending_requests,
			} => {
				pending_requests.push(PendingExecutionRequest { params, result_tx });
			}
		},
		Entry::Vacant(v) => {
			prepare_queue
				.send(prepare::ToQueue::Enqueue { priority, pvf })
				.await;

			v.insert(ArtifactState::Preparing {
				pending_requests: vec![PendingExecutionRequest { params, result_tx }],
			});
		}
	}
}

async fn handle_heads_up(
	artifacts: &mut Artifacts,
	prepare_queue: &mut mpsc::Sender<prepare::ToQueue>,
	active_pvfs: Vec<Pvf>,
) {
	for active_pvf in active_pvfs {
		let artifact_id = active_pvf.to_artifact_id();
		match artifacts.artifacts.entry(artifact_id.clone()) {
			Entry::Occupied(_) => {}
			Entry::Vacant(v) => {
				v.insert(ArtifactState::Preparing {
					pending_requests: vec![],
				});

				prepare_queue
					.send(prepare::ToQueue::Enqueue {
						priority: Priority::Background,
						pvf: active_pvf,
					})
					.await;
			}
		}
	}
}

async fn handle_prepare_done(
	artifacts: &mut Artifacts,
	execute_queue: &mut mpsc::Sender<execute::ToQueue>,
	artifact_id: ArtifactId,
) {
	let artifact_path = artifact_id.path(&artifacts.cache_path);
	let artifact_state = artifacts.artifacts.remove(&artifact_id).unwrap(); // TODO:
	match artifact_state {
		ArtifactState::Preparing { pending_requests } => {
			for PendingExecutionRequest { params, result_tx } in pending_requests {
				execute_queue
					.send(execute::ToQueue::Enqueue {
						artifact_path: artifact_path.clone(),
						params,
						result_tx,
					})
					.await;
			}
		}
		_ => panic!(), // TODO:
	}

	artifacts.artifacts.insert(
		artifact_id,
		ArtifactState::Prepared {
			last_time_needed: SystemTime::now(),
			artifact_path,
		},
	);
}

struct PendingExecutionRequest {
	params: Vec<u8>,
	result_tx: oneshot::Sender<Result<ValidationResult, ValidationError>>,
}

enum ArtifactState {
	/// The artifact is ready to be used by the executor.
	Prepared {
		/// The time when the artifact was the last time needed.
		///
		/// This is updated when we get the heads up for this artifact or when we just discover
		/// this file.
		last_time_needed: SystemTime,

		/// The path under which the artifact is saved on the FS.
		artifact_path: PathBuf,
	},
	/// A task to prepare this artifact is scheduled.
	Preparing {
		// TODO: Consider extracting this into a side table.
		/// Requests that are going to be submitted for execution as soon as this artifact is ready.
		pending_requests: Vec<PendingExecutionRequest>,
	},
}

pub struct Artifacts {
	cache_path: PathBuf,
	artifacts: HashMap<ArtifactId, ArtifactState>,
}

impl Artifacts {
	pub async fn new(cache_path: PathBuf) -> Self {
		let artifacts = match scan_for_known_artifacts(&cache_path).await {
			Ok(a) => a,
			Err(_) => {
				// TODO: warn
				HashMap::new()
			}
		};

		Self {
			cache_path,
			artifacts,
		}
	}
}

async fn scan_for_known_artifacts(
	cache_path: &Path,
) -> async_std::io::Result<HashMap<ArtifactId, ArtifactState>> {
	let mut result = HashMap::new();

	let mut dir = async_std::fs::read_dir(cache_path).await?;
	while let Some(res) = dir.next().await {
		let entry = res?;

		if entry.file_type().await?.is_dir() {
			// dirs do not belong to us, remove.
			let _ = async_std::fs::remove_dir_all(entry.path()).await;
		}

		let path = entry.path();
		let file_name = match path.file_name() {
			None => {
				// A file without a file name? Weird, just skip it.
				continue;
			}
			Some(file_name) => file_name,
		};

		let file_name = match file_name.to_str() {
			None => {
				// Non unicode file name? Definitely not us.
				let _ = async_std::fs::remove_file(&path).await;
				continue;
			}
			Some(file_name) => file_name,
		};

		let artifact_id = match ArtifactId::from_file_name(file_name) {
			None => {
				let _ = async_std::fs::remove_file(&path).await;
				continue;
			}
			Some(artifact_id) => artifact_id,
		};

		result.insert(
			artifact_id,
			ArtifactState::Prepared {
				last_time_needed: SystemTime::now(),
				artifact_path: path,
			},
		);
	}

	Ok(result)
}

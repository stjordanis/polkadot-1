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
use crate::{Priority, Pvf, artifacts::ArtifactId, execute, prepare};
use futures::{
	FutureExt, StreamExt,
	channel::{mpsc, oneshot},
	future::BoxFuture,
	stream::FuturesUnordered,
};
use polkadot_core_primitives::Hash;

struct ValidationHost;

impl ValidationHost {
	pub async fn execute_pvf(&self, pvf: Pvf, params: &[u8], priority: Priority) {
		let (result_tx, result_rx) = oneshot::channel::<()>();
		todo!()
	}
}

enum FromHandle {
	ExecutePvf {
		pvf: Pvf,
		params: Vec<u8>,
		priority: Priority,
		result_tx: oneshot::Sender<()>, // TODO: type
	},
	HeadsUp {
		active_pvfs: Vec<Pvf>,
	},
}

struct Inner {
	from_handle_rx: mpsc::Receiver<FromHandle>,
	prepare_queue: prepare::Queue,
	execute_pool: execute::Pool<()>,
	artifacts: Artifacts,
}

impl Inner {
	async fn run(mut self) {
		let mut from_handle_rx = self.from_handle_rx.fuse();
		let mut prepare_queue = self.prepare_queue;
		let mut execute_pool = self.execute_pool;
		let mut artifacts = self.artifacts;

		loop {
			futures::select! {
				from_handle = from_handle_rx.select_next_some() => {
					handle_from_handle(
						&mut artifacts,
						&mut prepare_queue,
						&mut execute_pool,
						from_handle,
					)
					.await;
				},
				artifact_id = prepare_queue.select_next_some() => {
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
						&mut execute_pool,
						artifact_id,
					);
				},
				() = execute_pool.select_next_some() => {}
			}
		}
	}
}

async fn handle_from_handle(
	artifacts: &mut Artifacts,
	prepare_queue: &mut prepare::Queue,
	execute_pool: &mut execute::Pool<()>,
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
				execute_pool,
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
	prepare_queue: &mut prepare::Queue,
	execute_pool: &mut execute::Pool<()>,
	pvf: Pvf,
	params: Vec<u8>,
	priority: Priority,
	result_tx: oneshot::Sender<()>,
) {
	let artifact_id = pvf.to_artifact_id();

	match artifacts.artifacts.entry(artifact_id.clone()) {
		Entry::Occupied(mut o) => match *o.get_mut() {
			ArtifactState::Prepared {
				ref artifact_path, ..
			} => execute_pool.enqueue(artifact_path.clone(), params, result_tx),
			ArtifactState::Preparing {
				ref mut pending_requests,
			} => {
				pending_requests.push(PendingExecutionRequest { params, result_tx });
			}
		},
		Entry::Vacant(v) => {
			prepare_queue.enqueue(priority, pvf);
			v.insert(ArtifactState::Preparing {
				pending_requests: vec![PendingExecutionRequest { params, result_tx }],
			});
		}
	}
}

async fn handle_heads_up(
	artifacts: &mut Artifacts,
	prepare_queue: &mut prepare::Queue,
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
				prepare_queue.enqueue(Priority::Background, active_pvf);
			}
		}
	}
}

fn handle_prepare_done(
	artifacts: &mut Artifacts,
	execute_pool: &mut execute::Pool<()>,
	artifact_id: ArtifactId,
) {
	let artifact_path = artifact_id.path(&artifacts.cache_path);
	let artifact_state = artifacts.artifacts.remove(&artifact_id).unwrap(); // TODO:
	match artifact_state {
		ArtifactState::Preparing { pending_requests } => {
			for pending_request in pending_requests {
				execute_pool.enqueue(
					artifact_path.clone(),
					pending_request.params,
					pending_request.result_tx,
				)
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
	result_tx: oneshot::Sender<()>, // TODO: Proper result
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

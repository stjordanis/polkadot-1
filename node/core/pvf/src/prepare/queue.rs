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

use super::{
	pool::{Pool, PoolEvent, WorkerId},
	worker,
};
use crate::{
	Priority, Pvf,
	artifacts::{ArtifactId},
	priority,
};
use std::{
	collections::{HashMap, VecDeque},
	iter,
	path::PathBuf,
	task::Poll,
};
use futures::{
	FutureExt,
	channel::{mpsc, oneshot},
	future::BoxFuture,
	stream::{FuturesOrdered, StreamExt as _},
};

const SOFT_MAX_WORKERS: usize = 10;
const HARD_MAX_WORKERS: usize = 15;

#[derive(Default)]
struct Workers {
	// TODO: This should not be a constant.
	workers: [Worker; HARD_MAX_WORKERS],
}

impl std::ops::Index<WorkerId> for Workers {
	type Output = Worker;

	fn index(&self, index: WorkerId) -> &Worker {
		&self.workers[index.0]
	}
}

impl std::ops::IndexMut<WorkerId> for Workers {
	fn index_mut(&mut self, index: WorkerId) -> &mut Worker {
		&mut self.workers[index.0]
	}
}

impl Workers {
	fn find_available(&mut self) -> Option<WorkerId> {
		self.workers
			.iter()
			.position(|w| w.job.is_none())
			.map(WorkerId)
	}
}

struct Job {
	/// The artifact ID which is being prepared in the context of this job. Fixed throughout the
	/// execution of the job.
	artifact_id: ArtifactId,

	/// The priority of this job. Can be bumped.
	priority: Priority,
}

#[derive(Default)]
struct Worker {
	/// `None` means it wasn't created.
	handle: Option<WorkerId>,

	/// Whether or not the worker has job or not.
	job: Option<Job>,
}

pub struct Queue {
	cache_path: PathBuf,
	workers: Workers,
	assignments: HashMap<ArtifactId, WorkerId>,

	/// The jobs that are not yet scheduled. These are waiting until the next `poll` where they are
	/// processed all at once.
	unscheduled: Vec<(Priority, Pvf)>,

	/// The pool that is responsible for providing the queue with workers.
	pool: Box<dyn Pool>,
}

impl Queue {
	fn new(cache_path: PathBuf, pool: Box<dyn Pool>) -> Self {
		Self {
			workers: Workers::default(),
			assignments: HashMap::new(),
			unscheduled: Vec::new(),
			cache_path,
			pool,
		}
	}

	pub fn enqueue(&mut self, prio: Priority, pvf: Pvf) {
		if let Some(worker_id) = self.assignments.get(&pvf.to_artifact_id()) {
			// Preparation is already under way. Bump the priority if needed.

			// TODO: get the current priority and ensure that it matches the required.
			return;
		}

		if let Some(available) = self.workers.find_available() {
			self.assign(available, prio, pvf);
		} else {
			let _ = self.pool.spawn(prio.is_critical());
			self.unscheduled.push((prio, pvf));
		}
	}

	fn handle_worker_concluded(&mut self, worker_id: WorkerId) -> Option<ArtifactId> {
		let worker = &mut self.workers[worker_id];
		let job = worker
			.job
			.take()
			.expect("the worker was assigned so it should have had job; qed");

		let _ = self.assignments.remove(&job.artifact_id);
		let artifact_id = job.artifact_id;

		// TODO: put back. If it was not culled proceed to scheduling.

		// see if there are more work available and schedule it.
		if let Some((prio, pvf)) = next_unscheduled(&mut self.unscheduled) {
			self.assign(worker_id, prio, pvf);
		}

		Some(artifact_id)
	}

	fn assign(&mut self, worker_id: WorkerId, prio: Priority, pvf: Pvf) {
		let artifact_id = pvf.to_artifact_id();
		let artifact_path = artifact_id.path(&self.cache_path);

		self.assignments.insert(artifact_id.clone(), worker_id);

		let worker = &mut self.workers[worker_id];
		worker.job = Some(Job {
			artifact_id: artifact_id.clone(),
			priority: prio,
		});
		self.pool.start_work(worker_id, pvf.code, artifact_path);
	}
}

fn next_unscheduled(unscheduled: &mut Vec<(Priority, Pvf)>) -> Option<(Priority, Pvf)> {
	// TODO: Respect priority
	unscheduled.pop()
}

impl futures::Stream for Queue {
	type Item = ArtifactId;

	fn poll_next(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Self::Item>> {
		// TODO: Think about who triggers polling.

		let event = match self.pool.poll_next(cx) {
			Poll::Ready(ev) => ev,
			Poll::Pending => return Poll::Pending,
		};

		match event {
			PoolEvent::Spawned(worker_id) => {
				// TODO: Register the worker in the workers.
				// TODO: Schedule the job
			}
			PoolEvent::Died(worker_id) => {
				// TODO:
			}
			PoolEvent::Concluded(worker_id) => {
				self.handle_worker_concluded(worker_id);
				cx.waker().wake_by_ref();
				Poll::Pending
			}
		}
	}
}

impl futures::stream::FusedStream for Queue {
	fn is_terminated(&self) -> bool {
		// Queue never concludes.
		false
	}
}

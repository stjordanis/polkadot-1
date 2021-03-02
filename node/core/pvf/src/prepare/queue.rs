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
	pool::{self, Worker},
	worker,
};
use crate::{artifacts::ArtifactId, priority, Priority, Pvf};
use futures::{
	channel::{mpsc, oneshot},
	future::BoxFuture,
	stream::{FuturesOrdered, StreamExt as _},
	Future, FutureExt, SinkExt,
};
use std::{
	collections::{HashMap, VecDeque},
	iter, mem,
	task::Poll,
};
use async_std::path::PathBuf;

pub enum ToQueue {
	Enqueue { priority: Priority, pvf: Pvf },
}

pub enum FromQueue {
	Prepared(ArtifactId),
}

enum Child {
	// TODO: Rename this enum to a slot? Port? The name sounds like the worker is free whereas this is actually about a
	// child.
	Free,
	Reserved,
	Running { child: Worker },
}

impl Child {
	fn is_free(&self) -> bool {
		matches!(self, Child::Free)
	}

	fn reserve(&mut self) {
		assert!(self.is_free());
		*self = Child::Reserved;
	}

	fn reset(&mut self) -> Option<Worker> {
		match mem::replace(self, Child::Free) {
			Child::Free | Child::Reserved => None,
			Child::Running { child } => Some(child),
		}
	}

	fn as_running(&self) -> Option<Worker> {
		match *self {
			Child::Free | Child::Reserved => None,
			Child::Running { ref child } => Some(*child),
		}
	}
}

#[derive(Default)]
struct Workers {
	workers: slotmap::SecondaryMap<Worker, Child>,

	/// The number of workers either live or just spawned.
	spawned_num: usize,

	/// The maximum number of workers this pool can ever host. This is expected to be a small
	/// number, e.g. within a dozen.
	hard_capacity: usize,

	/// The number of workers we want aim to have. If there is a critical job and we are already
	/// at `soft_capacity`, we are allowed to grow up to `hard_capacity`. Thus this should be equal
	/// or smaller than `hard_capacity`.
	soft_capacity: usize,
}

impl Workers {
	/// Returns `true` if the queue is allowed to request one more worker.
	fn can_afford_one_more(&self, critical: bool) -> bool {
		let cap = if critical {
			self.hard_capacity
		} else {
			self.soft_capacity
		};
		self.spawned_num < cap
	}

	fn find_available(&mut self) -> Option<Worker> {
		// self.workers
		// 	.iter()
		// 	.position(|w| w.job.is_none())
		// 	.map(WorkerId)
		todo!()
	}

	/// Offer the worker back to the pool. The passed worker ID must be considered unusable unless
	/// it wasn't taken by the pool, in which case it will be returned as `Some`.
	fn offer_back(&mut self, worker: Worker) {
		// TODO: if we are over limit -> kill it
		// we should probably also return a result so that the queue knows if it can reuse the
		// worker.

		todo!()
	}
}

struct Job {
	/// The artifact ID which is being prepared in the context of this job. Fixed throughout the
	/// execution of the job.
	artifact_id: ArtifactId,

	/// The priority of this job. Can be bumped.
	priority: Priority,
}

struct Queue {
	to_queue_rx: mpsc::Receiver<ToQueue>,
	from_queue_tx: mpsc::UnboundedSender<FromQueue>,

	to_pool_tx: mpsc::Sender<pool::ToPool>,
	from_pool_tx: mpsc::Receiver<pool::FromPool>,

	cache_path: PathBuf,
	workers: Workers,
	assignments: HashMap<ArtifactId, Worker>,
	jobs: slotmap::SecondaryMap<Worker, Job>,

	/// The jobs that are not yet scheduled. These are waiting until the next `poll` where they are
	/// processed all at once.
	unscheduled: Vec<(Priority, Pvf)>,
}

impl Queue {
	fn new(
		cache_path: PathBuf,
		to_queue_rx: mpsc::Receiver<ToQueue>,
		from_queue_tx: mpsc::UnboundedSender<FromQueue>,
		to_pool_tx: mpsc::Sender<pool::ToPool>,
		from_pool_tx: mpsc::Receiver<pool::FromPool>,
	) -> Self {
		Self {
			workers: Workers::default(),
			assignments: HashMap::new(),
			unscheduled: Vec::new(),
			cache_path,
			to_queue_rx,
			from_queue_tx,
			to_pool_tx,
			from_pool_tx,
			jobs: slotmap::SecondaryMap::new(),
		}
	}

	async fn run(mut self) {
		loop {
			// TODO: implement
			// TODO: Handle rip from the pool by decreasing the `spawned_num`.
		}
	}
}

async fn enqueue(queue: &mut Queue, prio: Priority, pvf: Pvf) {
	if let Some(&worker) = queue.assignments.get(&pvf.to_artifact_id()) {
		// Preparation is already under way. Bump the priority if needed.
		let job = &mut queue.jobs[worker];
		if job.priority.is_background() && !prio.is_background() {
			queue
				.to_pool_tx
				.send(pool::ToPool::BumpPriority(worker))
				.await;
		}
		job.priority = prio;
		return;
	}

	if let Some(available) = queue.workers.find_available() {
		// TODO: Explain, why this should be fair, i.e. that the work won't be handled out of order.
		assign(queue, available, prio, pvf).await;
	} else {
		if queue.workers.can_afford_one_more(prio.is_critical()) {
			queue.workers.spawned_num += 1;
			queue.to_pool_tx.send(pool::ToPool::Spawn).await;
		}
		queue.unscheduled.push((prio, pvf));
	}
}

async fn handle_worker_concluded(queue: &mut Queue, worker: Worker) -> Option<ArtifactId> {
	let job = queue
		.jobs
		.remove(worker)
		.take()
		.expect("the worker was assigned so it should have had job; qed");

	let _ = queue.assignments.remove(&job.artifact_id);
	let artifact_id = job.artifact_id;

	// TODO: put back. If it was not culled proceed to scheduling.

	// see if there are more work available and schedule it.
	if let Some((prio, pvf)) = next_unscheduled(&mut queue.unscheduled) {
		assign(queue, worker, prio, pvf).await;
	}

	Some(artifact_id)
}

async fn assign(queue: &mut Queue, worker: Worker, prio: Priority, pvf: Pvf) {
	let artifact_id = pvf.to_artifact_id();
	let artifact_path = artifact_id.path(&queue.cache_path);

	queue.assignments.insert(artifact_id.clone(), worker);
	queue.jobs.insert(
		worker,
		Job {
			artifact_id,
			priority: prio,
		},
	);

	queue
		.to_pool_tx
		.send(pool::ToPool::StartWork {
			worker,
			code: pvf.code,
			artifact_path,
			background_priority: prio.is_background(),
		})
		.await;
}

fn next_unscheduled(unscheduled: &mut Vec<(Priority, Pvf)>) -> Option<(Priority, Pvf)> {
	// TODO: Respect priority
	unscheduled.pop()
}

pub fn start(
	cache_path: PathBuf,
	to_pool_tx: mpsc::Sender<pool::ToPool>,
	from_pool_rx: mpsc::Receiver<pool::FromPool>,
) -> (
	mpsc::Sender<ToQueue>,
	mpsc::UnboundedReceiver<FromQueue>,
	impl Future<Output = ()>,
) {
	let (to_queue_tx, to_queue_rx) = mpsc::channel(150);
	let (from_queue_tx, from_queue_rx) = mpsc::unbounded();

	let run = Queue::new(
		cache_path,
		to_queue_rx,
		from_queue_tx,
		to_pool_tx,
		from_pool_rx,
	)
	.run();

	(to_queue_tx, from_queue_rx, run)
}

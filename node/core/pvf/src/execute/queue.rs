//! A pool that handles requests for PVF execution.

use crate::worker_common::{IdleWorker, SpawnErr, WorkerHandle};

use super::worker::{self as execute_worker, Outcome};
use std::{collections::VecDeque, fmt, task::Poll};
use futures::{
	Future, FutureExt, SinkExt as _,
	channel::{mpsc, oneshot},
	future::BoxFuture,
	stream::{FuturesOrdered, FuturesUnordered, StreamExt as _},
};
use async_std::path::PathBuf;
use polkadot_parachain::{primitives::ValidationResult, wasm_executor::ValidationError};
use slotmap::HopSlotMap;

slotmap::new_key_type! { struct Worker; }

pub enum ToQueue {
	Enqueue {
		artifact_path: PathBuf,
		params: Vec<u8>,
		result_tx: oneshot::Sender<Result<ValidationResult, ValidationError>>,
	},
}

struct ExecuteJob {
	artifact_path: PathBuf,
	params: Vec<u8>,
	result_tx: oneshot::Sender<Result<ValidationResult, ValidationError>>,
}

struct WorkerData {
	idle: Option<IdleWorker>,
	handle: WorkerHandle,
}

impl fmt::Debug for WorkerData {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "WorkerData(pid={})", self.handle.id())
	}
}

struct Workers {
	running: HopSlotMap<Worker, WorkerData>,

	/// The number of workers either live or just spawned.
	spawned_num: usize,

	capacity: usize,
}

impl Workers {
	fn can_afford_one_more(&self) -> bool {
		self.spawned_num < self.capacity
	}

	fn find_available(&self) -> Option<Worker> {
		todo!()
	}

	fn claim_idle(&mut self, worker: Worker) -> IdleWorker {
		let data = self
			.running
			.get_mut(worker)
			.expect("the worker doesn't exist");
		data.idle.take().expect("the worker has no idle token")
	}
}

enum QueueEvent {
	Spawn(Result<(IdleWorker, WorkerHandle), SpawnErr>),
	StartWork(Worker, Outcome),
}

type Mux = FuturesUnordered<BoxFuture<'static, QueueEvent>>;

struct Queue {
	/// The receiver that receives messages to the pool.
	to_queue_tx: mpsc::Receiver<ToQueue>,

	/// The queue of jobs that are waiting for a worker to pick up.
	queue: VecDeque<ExecuteJob>,

	workers: Workers,

	mux: Mux,
}

impl Queue {
	fn new(to_queue_tx: mpsc::Receiver<ToQueue>) -> Self {
		Self {
			to_queue_tx,
			queue: VecDeque::new(),
			mux: Mux::new(),
			workers: Workers {
				running: HopSlotMap::with_capacity_and_key(10),
				spawned_num: 0,
				capacity: 10,
			},
		}
	}

	async fn run(mut self) {
		loop {
			// TODO: loop

			purge_dead(&mut self.workers).await;
		}
	}
}

async fn purge_dead(workers: &mut Workers) {
	let mut to_remove = vec![];
	for (worker, data) in workers.running.iter_mut() {
		if let Poll::Ready(()) = futures::poll!(&mut data.handle) {
			// a resolved future means that the worker has terminated. Weed it out.
			to_remove.push(worker);
		}
	}
	for w in to_remove {
		let _ = workers.running.remove(w);
		workers.spawned_num -= 1;
	}
}

fn handle_to_queue(queue: &mut Queue, to_queue: ToQueue) {
	let ToQueue::Enqueue {
		artifact_path,
		params,
		result_tx,
	} = to_queue;

	let job = ExecuteJob {
		artifact_path,
		params,
		result_tx,
	};

	if let Some(available) = queue.workers.find_available() {
		assign(queue, available, job);
	} else {
		if queue.workers.can_afford_one_more() {
			spawn_one(queue);
		}
		queue.queue.push_back(job);
	}
}

/// If there are pending jobs in the queue, schedules the next of them onto the just freed up
/// worker. Otherwise, puts back into the available workers list.
fn handle_job_finish(queue: &mut Queue, just_freed: Worker) {
	if let Some(job) = queue.queue.pop_front() {
		assign(queue, just_freed, job);
	} else {
		// TODO: if there are no jobs available put the worker back?
	}
}

fn spawn_one(queue: &mut Queue) {
	queue.workers.spawned_num += 1;
	queue
		.mux
		.push(async move { QueueEvent::Spawn(super::worker::spawn().await) }.boxed());
}

fn assign(queue: &mut Queue, worker: Worker, job: ExecuteJob) {
	let idle = queue.workers.claim_idle(worker);
	queue.mux.push(
		async move {
			let outcome = super::worker::start_work(idle, job.artifact_path, job.params).await;
			QueueEvent::StartWork(worker, outcome)
		}
		.boxed(),
	);
}

pub fn start() -> (mpsc::Sender<ToQueue>, impl Future<Output = ()>) {
	let (to_queue_tx, to_queue_rx) = mpsc::channel(20);
	let run = Queue::new(to_queue_rx).run();
	(to_queue_tx, run)
}

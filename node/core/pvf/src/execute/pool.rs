//! A pool that handles requests for PVF execution.

use super::worker as execute_worker;
use std::{collections::VecDeque};
use futures::{
	FutureExt,
	channel::{mpsc, oneshot},
	future::BoxFuture,
	stream::{FuturesOrdered, StreamExt as _},
	SinkExt as _,
};
use async_std::path::PathBuf;

pub struct Handle {
	to_pool: mpsc::Sender<ToPool>,
}

impl Handle {
	pub async fn enqueue(
		&mut self,
		artifact_path: PathBuf,
		params: Vec<u8>,
		result_tx: oneshot::Sender<()>,
	) {
		self.to_pool
			.send(ToPool::Enqueue {
				artifact_path,
				params,
				result_tx,
			})
			.await;
	}
}

enum ToPool {
	Enqueue {
		artifact_path: PathBuf,
		params: Vec<u8>,
		result_tx: oneshot::Sender<()>, // TODO: validation result
	},
}

pub struct ExecuteJob {
	artifact_path: PathBuf,
	params: Vec<u8>,
	result_tx: oneshot::Sender<()>,
}

pub struct Pool<W: WorkerSpawn> {
	/// The receiver that receives messages from the handle.
	from_handle: mpsc::Receiver<ToPool>,

	/// The list of workers available to use by this pool.
	workers: Vec<W::Worker>,

	/// The queue of jobs that are waiting for a worker to pick up.
	queue: VecDeque<ExecuteJob>,

	/// The currently spawned execution work. When finished, it will return the worker back.
	in_flight: FuturesOrdered<BoxFuture<'static, W::Worker>>,
}

impl<W: WorkerSpawn> Pool<W> {
	pub async fn run(mut self) {
		loop {
			futures::select! {
				message = self.from_handle.select_next_some().fuse() => {
					self.handle_message(message);
				}
				freed_up_worker = self.in_flight.select_next_some() => {
					self.handle_job_finish(freed_up_worker);
				}
			}
		}
	}

	pub fn enqueue(
		&mut self,
		artifact_path: PathBuf,
		params: Vec<u8>,
		result_tx: oneshot::Sender<()>,
	) {
		todo!()
	}

	fn handle_message(&mut self, message: ToPool) {
		match message {
			ToPool::Enqueue {
				artifact_path,
				params,
				result_tx,
			} => self.handle_enqueue(artifact_path, params, result_tx),
		}
	}

	fn handle_enqueue(
		&mut self,
		artifact_path: PathBuf,
		params: Vec<u8>,
		result_tx: oneshot::Sender<()>,
	) {
		let job = ExecuteJob {
			artifact_path,
			params,
			result_tx,
		};

		if let Some(free_worker) = self.workers.pop() {
			self.in_flight.push(W::execute(free_worker, job));
		} else {
			self.queue.push_back(job);
		}
	}

	/// If there are pending jobs in the queue, schedules the next of them onto the just freed up
	/// worker. Otherwise, puts back into the available workers list.
	fn handle_job_finish(&mut self, worker: W::Worker) {
		if let Some(job) = self.queue.pop_front() {
			self.in_flight.push(W::execute(worker, job));
		}
	}
}

impl<W: WorkerSpawn> futures::Stream for Pool<W> {
	type Item = ();

	fn poll_next(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Self::Item>> {
		todo!()
	}
}

impl<W: WorkerSpawn> futures::stream::FusedStream for Pool<W> {
	fn is_terminated(&self) -> bool {
		false
	}
}

pub trait WorkerSpawn {
	type Worker;
	fn new() -> Self::Worker;
	fn execute(worker: Self::Worker, execute_job: ExecuteJob) -> BoxFuture<'static, Self::Worker>;
}

impl WorkerSpawn for () {
	type Worker = ();

	fn new() -> Self::Worker {
		todo!()
	}

	fn execute(worker: Self::Worker, execute_job: ExecuteJob) -> BoxFuture<'static, Self::Worker> {
		todo!()
	}
}

async fn perform_execute(
	worker: execute_worker::Handle,
	execute_job: ExecuteJob,
) -> execute_worker::Handle {
	{
		let result = worker
			.execute(&execute_job.artifact_path, &execute_job.params)
			.await;

		let _ = execute_job.result_tx.send(()); // TODO:
	}

	worker
}

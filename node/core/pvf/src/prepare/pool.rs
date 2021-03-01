use crate::Priority;
use super::worker::{self, IdleWorker, Outcome, SpawnErr, WorkerHandle};
use std::{
	fmt, mem,
	path::Path,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
};
use async_std::path::PathBuf;
use futures::{
	Future, FutureExt, SinkExt, StreamExt, channel::mpsc, future::BoxFuture,
	stream::FuturesUnordered,
};
use slotmap::HopSlotMap;
use assert_matches::assert_matches;

slotmap::new_key_type! { pub struct Worker; }

pub enum ToPool {
	/// Request a new worker to spawn.
	Spawn,

	/// Kill the given worker. No-op if it's not running.
	Kill(Worker),

	/// If the given worker was started with the background priority, then it will be raised up to
	/// normal priority.
	BumpPriority(Worker),

	/// Request the given worker to start working on the given code.
	StartWork {
		worker: Worker,
		code: Arc<Vec<u8>>,
		artifact_path: PathBuf,
		background_priority: bool,
	},
}

pub enum FromPool {
	/// The given worker was just spawned and is ready to be used.
	Spawned(Worker),

	/// A request to spawn a worker is failed. This should be used to free up any reserved resources
	/// if any.
	FailedToSpawn,

	/// The given worker either succeeded or failed the given job. Under any circumstances the
	/// artifact file has been written.
	Concluded(Worker),

	/// The given worker ceased to exist.
	Rip(Worker),
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

enum PoolEvent {
	Spawn(Result<(IdleWorker, WorkerHandle), SpawnErr>),
	StartWork(Worker, Outcome),
}

type Mux = FuturesUnordered<BoxFuture<'static, PoolEvent>>;

struct Pool {
	to_pool: mpsc::Receiver<ToPool>,
	from_pool: mpsc::UnboundedSender<FromPool>,
	spawned: HopSlotMap<Worker, WorkerData>,
	mux: Mux,
}

/// A fatal error that warrants stopping the pool.
struct Fatal;

impl Pool {
	async fn run(mut self) {
		loop {
			// TODO: implement the event loop

			purge_dead(&mut self.from_pool, &mut self.spawned);
		}
	}
}

async fn purge_dead(
	from_pool: &mut mpsc::UnboundedSender<FromPool>,
	spawned: &mut HopSlotMap<Worker, WorkerData>,
) -> Result<(), Fatal> {
	let mut to_remove = vec![];
	for (worker, data) in spawned.iter_mut() {
		if let Poll::Ready(()) = futures::poll!(&mut data.handle) {
			// a resolved future means that the worker has terminated. Weed it out.
			to_remove.push(worker);
		}
	}
	for w in to_remove {
		let _ = spawned.remove(w);
		from_pool
			.unbounded_send(FromPool::Rip(w))
			.map_err(|_| Fatal)?;
	}
	Ok(())
}

fn handle_to_pool(spawned: &mut HopSlotMap<Worker, WorkerData>, mux: &mut Mux, to_pool: ToPool) {
	match to_pool {
		ToPool::Spawn => {
			mux.push(async { PoolEvent::Spawn(worker::spawn().await) }.boxed());
		}
		ToPool::StartWork {
			worker,
			code,
			artifact_path,
			background_priority,
		} => {
			if let Some(data) = spawned.get_mut(worker) {
				let idle = data.idle.take().unwrap(); // TODO: this shouldn't be none

				mux.push(
					async move {
						// TODO: background prio
						PoolEvent::StartWork(
							worker,
							worker::start_work(idle, code, artifact_path).await,
						)
					}
					.boxed(),
				);
			} else {
				// TODO: Log
			}
		}
		ToPool::Kill(worker) => {
			if let Some(mut data) = spawned.remove(worker) {
				drop(data);
			} else {
				// TODO: Log
			}
		}
		ToPool::BumpPriority(worker) => {
			if let Some(idle) = spawned.get(worker) {
				// TODO: set to the foreground priority
			} else {
				// TODO: Log
			}
		}
	}
}

fn handle_mux(
	from_pool: &mut mpsc::UnboundedSender<FromPool>,
	spawned: &mut HopSlotMap<Worker, WorkerData>,
	event: PoolEvent,
) -> Result<(), Fatal> {
	match event {
		PoolEvent::Spawn(result) => {
			if let Ok((idle_worker, handle)) = result {
				let worker = spawned.insert(WorkerData {
					idle: Some(idle_worker),
					handle,
				});
				from_pool
					.unbounded_send(FromPool::Spawned(worker))
					.map_err(|_| Fatal)?;
			} else {
				// TODO: log
			}

			Ok(())
		}
		PoolEvent::StartWork(worker, outcome) => {
			match outcome {
				Outcome::Concluded(idle) => {
					let data = match spawned.get_mut(worker) {
						None => {
							// Perhaps the worker was killed meanwhile.
							return Ok(());
						}
						Some(data) => data,
					};

					// We just replace the idle worker that was loaned from this option during
					// the work starting.
					let old = data.idle.replace(idle);
					assert_matches!(old, None, "attempt to overwrite an idle worker");

					// TODO: restore the priority?

					from_pool.unbounded_send(FromPool::Concluded(worker));

					Ok(())
				}
				Outcome::DidntMakeIt => {
					from_pool
						.unbounded_send(FromPool::Concluded(worker))
						.map_err(|_| Fatal)?;

					if let Some(_data) = spawned.remove(worker) {
						from_pool
							.unbounded_send(FromPool::Rip(worker))
							.map_err(|_| Fatal)?;
					}

					Ok(())
				}
			}
		}
	}
}

pub fn start() -> (
	mpsc::Sender<ToPool>,
	mpsc::UnboundedReceiver<FromPool>,
	impl Future<Output = ()>,
) {
	let (to_pool_tx, to_pool_rx) = mpsc::channel(10);
	let (from_pool_tx, from_pool_rx) = mpsc::unbounded();

	let run = Pool {
		to_pool: to_pool_rx,
		from_pool: from_pool_tx,
		spawned: HopSlotMap::with_capacity_and_key(20),
		mux: Mux::new(),
	}
	.run();

	(to_pool_tx, from_pool_rx, run)
}

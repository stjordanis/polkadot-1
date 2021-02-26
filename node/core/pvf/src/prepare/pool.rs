use crate::Priority;
use super::worker;
use std::{
	mem,
	path::Path,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
};
use async_std::path::PathBuf;
use futures::{future::BoxFuture, stream::FuturesUnordered};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WorkerId(pub usize);

#[async_trait::async_trait]
pub trait Pool {
	fn spawn(&mut self, force: bool) -> Option<WorkerId>;

	fn start_work(&mut self, worker_id: WorkerId, code: Arc<Vec<u8>>, artifact_path: PathBuf);

	/// TODO: Rename to something like: "set background priority"
	fn set_priority(&self, worker_id: WorkerId, priority: bool);

	/// Offer the worker back to the pool. The passed worker ID must be considered unusable unless
	/// it wasn't taken by the pool, in which case it will be returned as `Some`.
	fn offer_back(&mut self, worker_id: WorkerId) -> Option<WorkerId>;

	fn poll_next(&mut self, cx: &mut Context) -> Poll<PoolEvent>;
}

pub enum PoolEvent {
	/// The given worker has been spawned and now is ready for work.
	Spawned(WorkerId),

	/// The given worker has concluded assigned work.
	Concluded(WorkerId),

	/// The given worker has died.
	Died(WorkerId),
}

enum Action {
	Spawn(WorkerId),
	StartWork(WorkerId, Arc<Vec<u8>>, PathBuf),
}

enum Child {
	// TODO: Rename this enum to a slot? The name sounds like the worker is free whereas this is actually about a
	// child.
	Free,
	Reserved,
	Running { child: async_process::Child },
}

impl Child {
	fn is_free(&self) -> bool {
		matches!(self, Child::Free)
	}

	fn reserve(&mut self) {
		assert!(self.is_free());
		*self = Child::Reserved;
	}

	fn reset(&mut self) -> Option<async_process::Child> {
		match mem::replace(self, Child::Free) {
			Child::Free | Child::Reserved => None,
			Child::Running { child } => Some(child),
		}
	}

	fn as_running(&self) -> Option<&async_process::Child> {
		match *self {
			Child::Free | Child::Reserved => None,
			Child::Running { ref child } => Some(child),
		}
	}
}

#[derive(Default)]
struct RealPool {
	tasks: FuturesUnordered<BoxFuture<'static, PoolEvent>>,

	children: Vec<Child>,

	/// The maximum number of workers this pool can ever host. This is expected to be a small
	/// number, e.g. within a dozen.
	hard_capacity: usize,

	/// The number of workers we want aim to have. If there is a critical job and we are already
	/// at `soft_capacity`, we are allowed to grow up to `hard_capacity`. Thus this should be equal
	/// or smaller than `hard_capacity`.
	soft_capacity: usize,
}

#[async_trait::async_trait]
impl Pool for RealPool {
	fn spawn(&mut self, force: bool) -> Option<WorkerId> {
		let spawned = self
			.children
			.iter()
			.filter(|child| !child.is_free())
			.count();

		let cap = if force {
			self.hard_capacity
		} else {
			self.soft_capacity
		};
		if spawned >= cap {
			return None;
		}

		if let Some((idx, child)) = self
			.children
			.iter_mut()
			.enumerate()
			.find(|(_, child)| child.is_free())
		{
			child.reserve();
			let worker_id = WorkerId(idx);
			self.tasks.push(async move {
				// TODO: What to do with an error?
				let (idle_worker, child) = worker::spawn().await;

			});
			return Some(worker_id);
		}

		None
	}

	fn start_work(&mut self, worker_id: WorkerId, code: Arc<Vec<u8>>, artifact_path: PathBuf) {

		todo!()
	}

	fn set_priority(&self, worker_id: WorkerId, priority: bool) {
		if let Some(ref child) = self.children[worker_id.0].as_running() {
			// TODO: renice
		}
	}

	fn offer_back(&mut self, worker_id: WorkerId) -> Option<WorkerId> {
		// TODO: if we are over limit -> kill it
		// we should probably also return a result so that the queue knows if it can reuse the
		// worker.

		// Should probably reset the priority to the default?

		todo!()
	}

	// fn kill(&mut self, worker_id: WorkerId) {
	// 	if let Some(mut child) = self.children[worker_id.0].reset() {
	// 		let _ = child.kill();
	// 		// TODO: report killing error, keep going because it may have succeeded.
	// 	}
	// }

	fn poll_next(&mut self, cx: &mut Context) -> Poll<PoolEvent> {
		todo!()
	}
}

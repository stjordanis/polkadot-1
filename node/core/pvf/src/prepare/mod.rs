
mod queue;
mod worker;
mod pool;

pub use queue::{ToQueue, FromQueue};
use pool::Worker;

#[doc(hidden)] // only for integration tests
pub use worker::{spawn_with_program_path, start_work, SpawnErr};

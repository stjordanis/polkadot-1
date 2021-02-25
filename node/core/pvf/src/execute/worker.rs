use async_std::path::Path;

pub enum Outcome {
	ValidationResult {
		soft_timeout: bool,
	},
	// TODO: In this case we should try to recompile the binary?
	Deserialization,
	HardTimeout,
	Error,
}

pub struct Handle {
	// ipc channel
}

impl Handle {
	pub async fn execute(&self, artifact_path: &Path, params: &[u8]) -> Outcome {
		// sends the message to the worker.
		todo!()
	}
}

// This function should be called from the cli
pub fn start_worker() {
	// get the ipc channel
}

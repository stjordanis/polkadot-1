use async_std::path::PathBuf;
use futures::future::join;
use polkadot_node_core_pvf::{Pvf, ValidationHost, start};
use polkadot_parachain::{
	primitives::{BlockData, ValidationParams, ValidationResult},
	wasm_executor::{InvalidCandidate, ValidationError},
};
use parity_scale_codec::Encode as _;

mod worker_common;

const PUPPET_EXE: &str = env!("CARGO_BIN_EXE_puppet_worker");

struct TestHost {
	_cache_dir: tempfile::TempDir,
	host: ValidationHost,
}

impl TestHost {
	fn new() -> Self {
		let cache_dir = tempfile::tempdir().unwrap();
		let program_path = PathBuf::from(PUPPET_EXE);
		let (host, task) = start(&program_path, &PathBuf::from(cache_dir.path().to_owned()));
		let _ = async_std::task::spawn(task);
		Self {
			_cache_dir: cache_dir,
			host,
		}
	}

	async fn validate_candidate(
		&self,
		code: &[u8],
		params: ValidationParams,
	) -> Result<ValidationResult, ValidationError> {
		self.host
			.execute_pvf(
				Pvf::from_code(code),
				params.encode(),
				polkadot_node_core_pvf::Priority::Normal,
			)
			.await
	}
}

#[async_std::test]
async fn terminates_on_timeout() {
	let host = TestHost::new();

	let result = host
		.validate_candidate(
			halt::wasm_binary_unwrap(),
			ValidationParams {
				block_data: BlockData(Vec::new()),
				parent_head: Default::default(),
				relay_parent_number: 1,
				relay_parent_storage_root: Default::default(),
			},
		)
		.await;

	match result {
		Err(ValidationError::InvalidCandidate(InvalidCandidate::ExternalWasmExecutor(msg)))
			if msg == "hard timeout" => {}
		r => panic!("{:?}", r),
	}

	// TODO: uncomment

	// check that another parachain can validate normaly
	// adder::execute_good_on_parent_with_external_process_validation();
}

#[async_std::test]
async fn parallel_execution() {
	let host = TestHost::new();
	let execute_pvf_future_1 = host.validate_candidate(
		halt::wasm_binary_unwrap(),
		ValidationParams {
			block_data: BlockData(Vec::new()),
			parent_head: Default::default(),
			relay_parent_number: 1,
			relay_parent_storage_root: Default::default(),
		},
	);
	let execute_pvf_future_2 = host.validate_candidate(
		halt::wasm_binary_unwrap(),
		ValidationParams {
			block_data: BlockData(Vec::new()),
			parent_head: Default::default(),
			relay_parent_number: 1,
			relay_parent_storage_root: Default::default(),
		},
	);

	let start = std::time::Instant::now();
	futures::join!(execute_pvf_future_1, execute_pvf_future_2);

	// TODO: Check the validity of these

	// total time should be < 2 x EXECUTION_TIMEOUT_SEC
	const EXECUTION_TIMEOUT_SEC: u64 = 3;
	assert!(
		std::time::Instant::now().duration_since(start)
			< std::time::Duration::from_secs(EXECUTION_TIMEOUT_SEC * 2)
	);
}

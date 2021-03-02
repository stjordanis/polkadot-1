use polkadot_node_core_pvf::integration_test::worker_common::{spawn_with_program_path, SpawnErr};

const PUPPET_EXE: &str = env!("CARGO_BIN_EXE_puppet_worker");

#[async_std::test]
async fn spawn_timeout() {
	let result = spawn_with_program_path(PUPPET_EXE, &["sleep"]).await;
	assert!(matches!(result, Err(SpawnErr::AcceptTimeout)));
}

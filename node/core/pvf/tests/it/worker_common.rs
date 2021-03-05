use crate::PUPPET_EXE;
use polkadot_node_core_pvf::integration_test::worker_common::{spawn_with_program_path, SpawnErr};

#[async_std::test]
async fn spawn_timeout() {
	let result = spawn_with_program_path(PUPPET_EXE, &["sleep"]).await;
	assert!(matches!(result, Err(SpawnErr::AcceptTimeout)));
}

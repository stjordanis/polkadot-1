//! A simple binary that allows mocking the behavior of the real worker.

use std::time::Duration;

fn main() {
	let args = std::env::args().collect::<Vec<_>>();
	if args.len() < 2 {
		panic!("wrong number of arguments");
	}

	let subcommand = &args[1];
	match subcommand.as_ref() {
		"sleep" => {
			std::thread::sleep(Duration::from_secs(5));
		}
		"prepare-worker" => {
			let socket_path = &args[2];
			polkadot_node_core_pvf::prepare_worker_entrypoint(socket_path);
		}
		"execute-worker" => {
			let socket_path = &args[2];
			polkadot_node_core_pvf::execute_worker_entrypoint(socket_path);
		}
		other => panic!("unknown subcommand: {}", other),
	}
}

#![no_main]
use libfuzzer_sys::fuzz_target;

// Copyright 2021 Parity Technologies (UK) Ltd.
// This file is part of Polkadot.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Polkadot.  If not, see <http://www.gnu.org/licenses/>.

use arbitrary::{self, Arbitrary, Unstructured};
use futures::channel::oneshot;
use polkadot_node_core_pvf::Pvf;
use polkadot_parachain::{
	primitives::{
		RelayChainBlockNumber, BlockData as GenericBlockData, HeadData as GenericHeadData,
		ValidationParams,
	},
};
use parity_scale_codec::{Decode, Encode};
use adder::{HeadData, BlockData, hash_state};

// we have a list of PVFs.
// Some of them we are going to execute and others we are going to just prepare.
//
// the parameters like PVF + validation params should define the expected result.
// Parameters are PVF specific.
//
// We generate PVFs by just slapping random sequence at the back.

#[derive(Debug, Clone, Arbitrary)]
enum Priority {
	Background,
	Normal,
	Critical,
}

impl From<Priority> for polkadot_node_core_pvf::Priority {
	fn from(priority: Priority) -> Self {
		match priority {
			Priority::Background => Self::Background,
			Priority::Normal => Self::Normal,
			Priority::Critical => Self::Critical,
		}
	}
}

#[derive(Arbitrary, Clone, Debug)]
enum PvfKind {
	Adder,
	Halt,
}

#[derive(Arbitrary, Clone, Debug)]
enum Action {
	Delay,
	ConjurePvf(PvfKind, u32),
	ExecutePvf(u32, Vec<u8>, Priority),
	HeadsUp(Vec<u32>),
}

struct Scenario {
	actions: Vec<Action>,
}

impl Arbitrary for Scenario {
	fn arbitrary(input: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
		let mut cx = ScenarioContext::new();

		let n = input.arbitrary_len::<Action>()?;
		let mut actions = Vec::with_capacity(n);

		for _ in 0..n {
			let mut alternatives: Vec<
				fn(&mut Unstructured, &mut ScenarioContext) -> arbitrary::Result<Action>,
			> = vec![];

			alternatives.push(|input, cx| {
				let kind = PvfKind::arbitrary(input)?;
				let cookie = u32::arbitrary(input)?;
				cx.pvfs.push(kind.clone());
				Ok(Action::ConjurePvf(kind, cookie))
			});

			if !cx.pvfs.is_empty() {
				alternatives.push(|input, cx| {
					cx.involves_prepare = true;
					let n = input.int_in_range(0..=cx.pvfs.len() - 1)?;
					let mut xs = Vec::with_capacity(n);
					for _ in 0..n {
						xs.push(input.int_in_range(0..=cx.pvfs.len() - 1)? as u32);
					}
					Ok(Action::HeadsUp(xs))
				});

				alternatives.push(|input, cx| {
					cx.involves_prepare = true;
					let pvf_idx = input.int_in_range(0..=cx.pvfs.len() - 1)? as u32;
					let params = match cx.pvfs[pvf_idx as usize] {
						PvfKind::Adder => {
							let parent_head = HeadData {
								number: 0,
								parent_hash: [0; 32],
								post_state: hash_state(0),
							};

							let block_data = BlockData { state: 0, add: 512 };
							ValidationParams {
								parent_head: GenericHeadData(parent_head.encode()),
								block_data: GenericBlockData(block_data.encode()),
								relay_parent_number: 1,
								relay_parent_storage_root: Default::default(),
							}
							.encode()
						}
						PvfKind::Halt => ValidationParams {
							block_data: GenericBlockData(Vec::new()),
							parent_head: Default::default(),
							relay_parent_number: 1,
							relay_parent_storage_root: Default::default(),
						}
						.encode(),
					};
					let priority = Priority::arbitrary(input)?;
					Ok(Action::ExecutePvf(pvf_idx, params, priority))
				});
			}

			if cx.involves_prepare {
				alternatives.push(|_, _| Ok(Action::Delay));
			}

			let alt = input.choose(&alternatives)?;
			actions.push(alt(input, &mut cx)?);
		}

		Ok(Scenario { actions })
	}
}

struct ScenarioContext {
	pvfs: Vec<PvfKind>,
	involves_prepare: bool,
}

impl ScenarioContext {
	fn new() -> Self {
		Self {
			pvfs: Vec::new(),
			involves_prepare: false,
		}
	}
}

struct TestHost {
	_cache_dir: tempfile::TempDir,
	host: polkadot_node_core_pvf::ValidationHost,
}

impl TestHost {
	fn new() -> Self {
		use async_std::path::PathBuf;
		let cache_dir = tempfile::tempdir().unwrap();
		let program_path = PathBuf::from("/home/lilpep/dev/polkadot-2/target/debug/puppet_worker");
		let (host, task) = polkadot_node_core_pvf::start(&program_path, &PathBuf::from(cache_dir.path().to_owned()));
		let _ = async_std::task::spawn(task);
		Self {
			_cache_dir: cache_dir,
			host,
		}
	}
}

async fn play(scenario: Scenario) {
	let host = TestHost::new();
	let mut pvfs = Vec::new();

	for action in scenario.actions {
		match action {
			Action::Delay => {
				futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
			}
			Action::ConjurePvf(kind, cookie) => {
				pvfs.push(Pvf::from_code(&conjure_pvf(kind, cookie)));
			}
			Action::ExecutePvf(x, params, priority) => {
				let pvf = pvfs[x as usize].clone();
				let (result_tx, result_rx) = oneshot::channel();
				host.host.execute_pvf(pvf, params, priority.into(), result_tx).await;
				let _ = result_rx.await;
			}
			Action::HeadsUp(xs) => {
				host.host.heads_up(xs.into_iter().map(|x| pvfs[x as usize].clone()).collect()).await;
			},
		}
	}
}

fn conjure_pvf(kind: PvfKind, cookie: u32) -> Vec<u8> {
	let mut base_image = match kind {
		PvfKind::Adder => adder::wasm_binary_unwrap().to_vec(),
		PvfKind::Halt => halt::wasm_binary_unwrap().to_vec(),
	};

	// a custom section prologue, consist of a byte with the section id, 0 for a custom section,
	// followed by the section length. 8 bytes for the section name (len and string), and the cookie.
	base_image.push(0);
	base_image.extend_from_slice(&12u32.to_le_bytes());

	// len of the name section string and the name string itself.
	base_image.extend_from_slice(&4u32.to_le_bytes());
	base_image.extend_from_slice(b"yolo");

	// finally add the cookie bytes.
	base_image.extend_from_slice(&cookie.to_le_bytes());

	base_image
}

fuzz_target!(|data: &[u8]| {
    if let Ok(scenario) = Scenario::arbitrary(&mut Unstructured::new(data)) {
        async_std::task::block_on(play(scenario));
    }
});

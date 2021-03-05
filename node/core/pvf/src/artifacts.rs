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

use async_std::path::{Path, PathBuf};
use polkadot_core_primitives::Hash;
use std::{collections::HashMap};
use parity_scale_codec::{Encode, Decode};

#[derive(Encode, Decode)]
pub enum Artifact {
	PrevalidationErr(String),
	PreparationErr(String),
	/// This state indicates that the process assigned to prepare the artifact wasn't responsible
	/// or were killed.
	DidntMakeIt,
	Compiled {
		compiled_artifact: Vec<u8>,
	},
}

impl Artifact {
	/// Serializes this struct into a byte buffer.
	pub fn serialize(&self) -> Vec<u8> {
		self.encode()
	}

	pub fn deserialize(mut bytes: &[u8]) -> Result<Self, String> {
		Artifact::decode(&mut bytes).map_err(|e| format!("{:?}", e))
	}
}

/// NOTE if we get to multiple engine implementations the artifact ID should include the engine
/// type as well.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArtifactId {
	code_hash: Hash,
}

impl ArtifactId {
	const PREFIX: &'static str = "wasmtime_1_";

	pub fn new(code_hash: Hash) -> Self {
		Self { code_hash }
	}

	/// Tries to recover the artifact id from the given file name.
	pub fn from_file_name(file_name: &str) -> Option<Self> {
		use std::str::FromStr as _;

		let file_name = file_name.strip_prefix(Self::PREFIX)?;
		let code_hash = Hash::from_str(file_name).ok()?;

		Some(Self { code_hash })
	}

	pub fn path(&self, cache_path: &Path) -> PathBuf {
		let file_name = format!("{}{}", Self::PREFIX, self.code_hash.to_string());
		cache_path.join(file_name)
	}
}

#[cfg(test)]
mod tests {
	use super::ArtifactId;

	#[test]
	fn from_file_name() {
		assert!(ArtifactId::from_file_name("").is_none());
		assert!(ArtifactId::from_file_name("junk").is_none());

		// TODO: Verify the resulting hash
		assert!(ArtifactId::from_file_name(
			"wasmtime_1_0x0000000000000000000000000000000000000000000000000000000000000000"
		)
		.is_some());
	}
}

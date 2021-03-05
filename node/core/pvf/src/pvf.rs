use crate::artifacts::ArtifactId;
use polkadot_core_primitives::Hash;
use sp_core::keccak_256;
use std::sync::Arc;

/// A struct that carries code of a parachain validation function and it's hash.
pub struct Pvf {
	pub(crate) code: Arc<Vec<u8>>,
	pub(crate) code_hash: Hash,
}

impl Pvf {
	pub fn from_code(code: &[u8]) -> Self {
		let code_hash = keccak_256(code).into();
		let code = Arc::new(code.to_owned());
		Self { code, code_hash }
	}

	/// Returns the artifact ID that corresponds to this PVF.
	pub fn to_artifact_id(&self) -> ArtifactId {
		ArtifactId::new(self.code_hash.clone())
	}
}

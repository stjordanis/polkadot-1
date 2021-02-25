use crate::artifacts::ArtifactId;
use polkadot_core_primitives::Hash;
use std::sync::Arc;

/// A struct that carries code of a parachain validation function and it's hash.
pub struct Pvf {
	pub(crate) code: Arc<Vec<u8>>,
	pub(crate) code_hash: Hash,
}

impl Pvf {
	/// Returns the artifact ID that corresponds to this PVF.
	pub fn to_artifact_id(&self) -> ArtifactId {
		ArtifactId::new(self.code_hash.clone())
	}
}

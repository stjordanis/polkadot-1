#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
	/// Jobs in this priority will be executed in the background, meaning that they will be only
	/// given spare CPU time.
	///
	/// This is mainly for cache warmings.
	Background,
	/// Normal priority for things that do not require immediate response, but still need to be
	/// done pretty quick.
	///
	/// Approvals and disputes fall into this category.
	Normal,
	/// This priority is used for requests that are required to be processed as soon as possible.
	///
	/// For example, backing is on critical path and require execution as soon as possible.
	Critical,
}

impl Priority {
	pub fn is_critical(&self) -> bool {
		*self == Priority::Critical
	}
}

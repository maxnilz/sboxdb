pub type FrameId = usize;

///  Replacer tracks page usage for replacement in case of buffer pool is full.
pub trait Replacer {
    /// Record the event that the given frame id is accessed at current timestamp.
    /// Create a new entry for access history if frame id has not been seen before.
    fn record_access(&self, frame_id: FrameId);

    /// Find the frame to evict with replace policy(e.g. backward k-distance). Only frames that
    /// are marked as evictable are candidates for eviction.
    ///
    /// Successful eviction of a frame should decrement the size of replacer and remove the frame's
    /// access history.
    ///
    /// Return the frame id if a frame is evicted successfully, None if no frames can be evicted.
    fn evict(&self) -> Option<FrameId>;

    /// Toggle whether a frame is evictable or non-evictable. this function also control replacer
    /// size. Note that size is equal to number of evictable entries.
    ///
    /// If a frame was previously evictable and is to be set to non-evictable, then size should
    /// decrement. If a frame was previously non-evictable and is to be set evictable, then size
    /// should increment.
    fn set_evictable(&self, frame_id: FrameId, evictable: bool);

    /// Check if a frame is evictable. if the frame is not found, return true.
    fn is_evictable(&self, frame_id: FrameId) -> bool;

    /// Remove an evictable frame from replacer, along with its access history. This function
    /// should also decrement the replacer size if removal is successful.
    ///
    /// Note that this is different from evicting a frame without check replacer policy.
    ///
    /// If remove is called on a non-evictable frame, return an error. If the specified frame is
    /// not found, do nothing without return any error.
    fn remove(&self, frame_id: FrameId);

    /// replace size.
    fn size(&self) -> usize;
}

pub mod bufferpool;
mod replacer;

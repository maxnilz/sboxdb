use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::storage::kv::KvStorage;

use super::page::{FrameId, Key, Page, PageId};
use super::replacer::Replacer;
use super::replacer::SyncLRUKReplacer;

/// The buffer pool is responsible for moving physical pages back and forth
/// from main memory to disk. It allows a DBMS to support databases that are
/// larger than the amount of memory available to the system.
///
/// The buffer pool's operations are transparent to other parts in the system.
/// For example, the system asks the buffer pool for a page using its unique
/// identifier (page_id) and it does not know whether that page is already in
/// memory or whether the system has to retrieve it from disk.
struct BufferPool {
    pool_size: usize,
    /// kv storage.
    storage: Box<dyn KvStorage>,
    /// array of buffer pool pages. use the array index as
    /// FrameId, i.e., the FrameId is in range: [0, pool_size).
    pages: Vec<Arc<Page>>,
    /// page table for keeping track of buffer pool pages.
    page_table: HashMap<PageId, FrameId>,
    /// list of free frames that don't have any pages on them.
    free_list: Vec<FrameId>,
    /// Replacer to find unpinned pages for replacement.
    replacer: Arc<dyn Replacer>,
    /// The next page id to be allocated.
    /// TODO: persist this info later.
    next_page_id: PageId,
}

impl BufferPool {
    fn new(storage: Box<dyn KvStorage>, pool_size: usize, replacer_k: usize) -> Self {
        let mut pages = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);
        let page_table = HashMap::new();
        let replacer: Arc<dyn Replacer> = Arc::new(SyncLRUKReplacer::new(replacer_k, pool_size));
        for i in 0..pool_size {
            pages.push(Arc::new(Page::new()));
            // initially, every page is in the free list
            free_list.push(i);
        }
        BufferPool { pool_size, storage, pages, page_table, free_list, replacer, next_page_id: 0 }
    }

    /// Create a new page in the buffer pool, return the newly created page or None if all
    /// frames are currently in use and not evictable(in another word, pinned).
    ///
    /// Pick the replacement frame from either the free list or the replacer(always find from
    /// the free list first), and then call allocate_page to get a new page id. If the replacement
    /// frame has a dirty page, write it back to the storage first. reset the memory and metadata
    /// for the new page.
    ///
    /// Remember to "pin" the frame by calling replacer.set_evictable(frame_id, false) so that
    /// the replacer wouldn't evict the frame before the buffer pool manager "unpin" it.
    fn new_page(&mut self) -> Result<Arc<Page>> {
        let mut frame_id: Option<FrameId> = None;
        // check if we have free frame available
        if self.free_list.len() > 0 {
            frame_id = Some(self.free_list.pop().unwrap());
        }
        // we have no free frame available, try to evict one
        if frame_id.is_none() {
            frame_id = self.replacer.evict();
        }
        if frame_id.is_none() {
            // no evictable frame found
            return Err(Error::BufferPoolNoAvailableFrame);
        }

        // found an evictable frame
        let frame_id = frame_id.unwrap();
        let page = Arc::clone(&self.pages[frame_id]);
        let mut guard = page.write()?;
        let prev_page_id = guard.id;

        // flush the in-memory page/frame as the storage page if it is dirty.
        if guard.is_dirty {
            let key = guard.as_key();
            self.storage.set(&key, guard.data.to_vec())?;
        }
        // clean page frame first
        guard.clear();
        // allocating new page id
        let new_page_id = self.allocate_page();
        // set the page with new page id
        guard.id = new_page_id;
        // pin the new page with initial value 1
        guard.pin_count = 1;
        // unlink the old page from page table
        self.page_table.remove(&prev_page_id);
        // link the new page with frame into page table
        self.page_table.insert(new_page_id, frame_id);
        // record frame access
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        drop(guard);
        Ok(page)
    }

    /// Fetch the requested page with the given page id from the buffer pool. return
    /// no available frame error if the page need to be fetched from disk but all frames
    /// are currently in use and not evictable(in other words, pinned).
    ///
    /// First search for page_id in the buffer pool. if not found, pick a replacement from
    /// either the free list or the replacer(always find from the free list first), read the
    /// page from the disk with storage and replace the old page in the frame. similar to
    /// the new_page, if the page is dirty, write it back to the storage and update the metadata
    /// of the new page.
    fn fetch_page(&mut self, page_id: PageId) -> Result<Arc<Page>> {
        // check if page table has the page id
        let frame_id = self.page_table.get(&page_id);
        if let Some(&frame_id) = frame_id {
            // we have the page frame in buffer pool already,
            // increase pin count, record frame access then
            // return it.
            let page = Arc::clone(&self.pages[frame_id]);
            let mut guard = page.write()?;
            guard.pin_count += 1;
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);

            drop(guard);
            return Ok(page);
        }

        // page not found, try to pick a replacement the free list.
        let mut frame_id: Option<FrameId> = None;
        if self.free_list.len() > 0 {
            frame_id = Some(self.free_list.pop().unwrap());
        }
        // if no free frame in free list, try to pick one from replacer.
        if frame_id.is_none() {
            frame_id = self.replacer.evict();
        }
        // if both free list and replacer have no available frame can be
        // replaced, return with Error::BufferPoolNoAvailableFrame error.
        if frame_id.is_none() {
            return Err(Error::BufferPoolNoAvailableFrame);
        }
        // evict the page pointed by the replaceable frame_id
        let frame_id = frame_id.unwrap();
        let page = Arc::clone(&self.pages[frame_id]);
        let mut guard = page.write()?;
        let prev_page_id = guard.id;

        // flush the in-memory page/frame as the storage page if it is dirty.
        if guard.is_dirty {
            let key = guard.as_key();
            self.storage.set(&key, guard.data.to_vec())?;
        }

        // fetch page from disk
        let key = Key::PageId(page_id).encode()?;
        let data = self.storage.get(&key)?;
        // clean page frame first
        guard.clear();
        guard.id = page_id;
        guard.pin_count += 1;
        // set the disk data onto the page frame.
        if let Some(data) = data {
            guard.data = data;
        }
        // unlink the old page from page table
        self.page_table.remove(&prev_page_id);
        // link the new page with frame into page table
        self.page_table.insert(guard.id, frame_id);
        // record frame access
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        drop(guard);
        Ok(page)
    }

    /// Flush the target to storage regardless of the dirty flag.
    /// unset the dirty flag of the page after flushing.
    ///
    /// Return false if the page cannot be found in the page table,
    /// ture otherwise.
    fn flush_page(&mut self, page_id: PageId) -> Result<bool> {
        let frame_id = self.page_table.get(&page_id);
        if frame_id.is_none() {
            return Ok(false);
        }
        let &frame_id = frame_id.unwrap();
        let page = &self.pages[frame_id];
        let mut guard = page.write()?;

        let key = Key::PageId(guard.id).encode()?;
        self.storage.set(&key, guard.data.to_vec())?;
        guard.is_dirty = false;

        return Ok(true);
    }

    /// Flush all the pages in the buffer pool to storage.
    fn flush_all_pages(&mut self) -> Result<()> {
        for (_, &frame_id) in self.page_table.iter() {
            let page = &self.pages[frame_id];
            let mut guard = page.write()?;

            let key = Key::PageId(guard.id).encode()?;
            self.storage.set(&key, guard.data.to_vec())?;
            guard.is_dirty = false;
        }
        Ok(())
    }

    /// Delete a page from buffer pool. if a page is not in the buffer pool, do nothing
    /// and return true. if the page is pinned and cannot be deleted, return false immediately.
    ///
    /// After deleting the page from the page table, stop tracking the frame in replacer and add
    /// back the frame to the free list. Also reset the page frame's memory and metadata.
    ///
    /// Deleting a page from buffer would also free the page from the storage as well.
    fn delete_page(&mut self, page_id: PageId) -> Result<bool> {
        let frame_id = self.page_table.get(&page_id);
        if frame_id.is_none() {
            return Ok(false);
        }
        let &frame_id = frame_id.unwrap();
        let page = &self.pages[frame_id];
        // use raw rwlock instead of page guard to check if the page is pinned or not.
        // since the RwLock have no way to upgrade a read lock to write lock, we acquire
        // write lock to check the pin count.
        let mut guard = page.write()?;
        if guard.pin_count > 0 {
            assert_eq!(true, self.replacer.is_evictable(frame_id));
            return Ok(false);
        }
        // delete the page from page storage
        let key = guard.as_key();
        self.storage.remove(&key)?;
        // clean page frame first
        guard.clear();
        // remove from replacer
        self.replacer.remove(frame_id);
        // unlink the page from page table
        self.page_table.remove(&guard.id);
        // add back to free list
        self.free_list.push(frame_id);
        // remove frame access from replacer.
        self.replacer.remove(frame_id);

        Ok(true)
    }

    /// Unpin the target page from the buffer pool. If page_id is not in the buffer pool
    /// or its pin count is already 0, return false.
    ///
    /// Decrement the pin count of a page. If the pin count reaches 0, the frame should
    /// be evictable by the replacer. Also, set the dirty flag on the page to indicate if
    /// the page was modified.
    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool {
        let frame_id = self.page_table.get(&page_id);
        if frame_id.is_none() {
            return false;
        }
        let &frame_id = frame_id.unwrap();
        let page = &self.pages[frame_id];
        let mut guard = page.write().unwrap();
        guard.is_dirty = is_dirty;
        if guard.pin_count == 0 {
            return false;
        }
        guard.pin_count -= 1;
        if guard.pin_count == 0 {
            self.replacer.set_evictable(frame_id, true);
        }
        true
    }

    fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        page_id
    }
}

/// Buffer pool manager wrap buffer pool with a mutex for concurrent access,
/// basically all the heavy lifting are happens in the buffer pool.
pub struct BufferPoolManager {
    /// hold the actual buffer pool protected by a mutex latch.
    /// TODO: we need more granularity concurrency control instead
    ///  of having this mutex latch all the operations.
    inner: Arc<Mutex<BufferPool>>,
}

impl BufferPoolManager {
    pub fn new(storage: Box<dyn KvStorage>, pool_size: usize, replacer_k: usize) -> Self {
        let inner = BufferPool::new(storage, pool_size, replacer_k);
        BufferPoolManager { inner: Arc::new(Mutex::new(inner)) }
    }

    /// Create a new page in buffer pool.
    pub fn new_page(&self) -> Result<Arc<Page>> {
        let mut inner = self.inner.lock()?;
        let guard = inner.new_page()?;
        Ok(guard)
    }

    /// Fetch the request page with the give page id from the buffer pool.
    pub fn fetch_page(&self, page_id: PageId) -> Result<Arc<Page>> {
        let mut inner = self.inner.lock()?;
        let guard = inner.fetch_page(page_id)?;
        Ok(guard)
    }

    /// Flush the target to storage regardless of the dirty flag.
    /// unset the dirty flag of the page after flushing.
    pub fn flush_page(&self, page_id: PageId) -> Result<bool> {
        let mut inner = self.inner.lock()?;
        inner.flush_page(page_id)
    }

    /// Flush all the pages in the buffer pool to storage.
    pub fn flush_all_pages(&self) -> Result<()> {
        let mut inner = self.inner.lock()?;
        inner.flush_all_pages()
    }

    /// Delete a page from buffer pool. if a page is not in the buffer pool, do nothing
    /// and return true. if the page is pinned and cannot be deleted, return false immediately.
    pub fn delete_page(&self, page_id: PageId) -> Result<bool> {
        let mut inner = self.inner.lock()?;
        inner.delete_page(page_id)
    }

    /// Unpin the target page from the buffer pool. If page_id is not in the buffer pool
    /// or its pin count is already 0, return false.
    ///
    /// Decrement the pin count of a page. If the pin count reaches 0, the frame should
    /// be evictable by the replacer. Also, set the dirty flag on the page to indicate if
    /// the page was modified.
    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.unpin_page(page_id, is_dirty)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use crate::storage::kv::{new_storage, StorageType};

    use super::*;

    #[test]
    fn test_buffer_pool_basic() -> Result<()> {
        let kvs = new_storage(StorageType::Memory)?;
        let buffer_pool_size = 10;
        let k = 5;
        let bpm = BufferPoolManager::new(kvs, buffer_pool_size, k);
        // Scenario: The buffer bool is empty, we should be able to create a new page.
        let page0 = bpm.new_page()?;

        // Scenario: Once we have a page, we should be able to read and write the content.
        let mut guard = page0.write()?;
        let data: &mut Vec<u8> = guard.as_mut();
        data.write(b"hello")?;
        drop(guard);
        let guard = page0.read()?;
        let data: &Vec<u8> = guard.as_ref();
        assert_eq!(Vec::from("hello"), *data);
        drop(guard);

        // Scenario: we should be able to create page until we fill up the buffer pool.
        for i in 1..buffer_pool_size {
            let page = bpm.new_page();
            assert!(page.is_ok())
        }
        // Scenario: Once the buffer pool is full, we should not be able to create any
        // new page.
        for i in buffer_pool_size..buffer_pool_size * 2 {
            let page = bpm.new_page();
            assert!(page.is_err())
        }
        // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pining another 4 new pages,
        // there would still be one buffer page left for reading page0.
        for i in 0..5 {
            let res = bpm.unpin_page(i, true);
            assert_eq!(true, res);
        }
        for i in 0..4 {
            let page = bpm.new_page();
            assert!(page.is_ok())
        }
        // Scenario: we should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page(0)?;
        let guard = page0.read()?;
        let data: &Vec<u8> = guard.as_ref();
        assert_eq!(Vec::from("hello"), *data);
        drop(guard);
        // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
        // now be pinned. Fetching page 0 should fail.
        assert_eq!(true, bpm.unpin_page(0, true));
        assert_eq!(true, bpm.new_page().is_ok());
        assert_eq!(true, bpm.fetch_page(0).is_err());

        Ok(())
    }
}

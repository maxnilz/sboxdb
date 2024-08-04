use std::ops::Deref;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::codec::keycodec;
use crate::error::Result;

pub type PageId = i64;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum Key {
    PageId(PageId),
}

impl Key {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = keycodec::serialize(self)?;
        Ok(bytes)
    }
}

/// Size of a data page in byte.
pub const PAGE_SIZE: usize = 4906;

/// Invalid page id
const INVALID_PAGE_ID: i64 = -1;

/// The actual page data that include in-memory metadata
/// like dirty bit and pin count etc. and the data on storage.
pub struct PageData {
    pub id: PageId,
    pub data: Vec<u8>,
    pub is_dirty: bool,
    pub pin_count: i32,
}

impl PageData {
    fn new() -> Self {
        Self {
            id: INVALID_PAGE_ID,
            data: Vec::with_capacity(PAGE_SIZE),
            is_dirty: false,
            pin_count: 0,
        }
    }

    pub fn as_key(&self) -> Vec<u8> {
        Key::PageId(self.id).encode().unwrap()
    }

    pub fn clear(&mut self) {
        self.id = INVALID_PAGE_ID;
        self.is_dirty = false;
        self.pin_count = 0;
        self.data.clear();
    }
}

impl<T> AsRef<T> for PageData {
    fn as_ref(&self) -> &T {
        unsafe { std::mem::transmute(&self.data) }
    }
}

impl<T> AsMut<T> for PageData {
    fn as_mut(&mut self) -> &mut T {
        unsafe { std::mem::transmute(&mut self.data) }
    }
}

/// Page act as the container of the actual page data for
/// providing concurrent access protection.
pub struct Page {
    inner: RwLock<PageData>,
}

impl Page {
    /// Allocate a page in memory with the given PAGE_SIZE and init
    /// the metadata accordingly.
    pub fn new() -> Self {
        Page { inner: RwLock::new(PageData::new()) }
    }
}

impl Deref for Page {
    type Target = RwLock<PageData>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

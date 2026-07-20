use crate::page::PageTrait;
use crate::page::PageType;
use crate::page_no::PageNo;
use crate::tree_dir_entry;
use crate::{Page, db_config::DbConfig};
use core::panic;
use std::cmp::Ordering;

pub struct DirPage {
    page: Page,
}

impl PageTrait for DirPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> PageNo {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: PageNo) {
        self.page.set_page_number(page_no)
    }

    fn get_page(&mut self) -> &mut Page {
        &mut self.page
    }

    fn get_version(&self) -> u64 {
        self.page.get_version()
    }

    fn set_version(&mut self, version: u64) {
        self.page.set_version(version);
    }
}

// The value in a DirPage is the page number of the child page so size is known.
pub struct DirSlot {
    offset: u16,
    key_len: u8,
}

// Header
//
// | Page No (8 bytes) | VersionHolder(8 bytes) | Entries(u16) | Free_Space(u16) |
// | prefix_length (u8) | left_fence_key_offset (u16) | left_fence_key_size (u8)
// | right_fence_key_offset (u16) | right_fence_key_size (u8) |
// | page_to_the_left (u64) |
// | slot | slot | slot | ...
// | free space
// | key | value | key | value | fence_key | fence_key | ...
//
//
// A dir_page is an internal node in the b-tree. It holds pointers to other pages,
// either other dir_pages or leaf_pages.
//
// The page layout is similar to leaf_page, there is a header, then an index into
// for the keys stored in the dir_page which grows down, then free space, then
// the actual keys and their associated page numbers that grow up and finally
// the fence keys for the dir_page if it has any.
//
// If the page has n keys then it holds n+1 references to pages. The left (or smallest) most
// page it references is stored in "page_to_the_left". If the client has a key that is smaller
// than the lowest key in the page then goes to this page, the "page_to_the_left".
//
// The keys stored in the page may be using head and tail compression. Head compression
// is where all the keys have a common prefix and the prefix is only stored once. To support
// head compression fence keys are stored.
// Any key in the page is equal or greater than the left fence key and equal or smaller than
// the right fence key. The fences are reset if a key is added to the page that is smaller
// than the left fence or greater than the right fence, the fences are not changed when
// keys are removed. Not changing the fences when removing keys means that the page
// and compression does not need to be rebuilt on key removal and avoids the possiblity of
// needing to split dir_pages on key removal.
//
// A dir_page may not have fence - the first dir_page in the tree will not have fences and
// thus no compression. In the tree the left most dir page will not have a left fence, and
// the right most dir page will not have a right fence. After the root dir page splits the
// child pages will get fences and thus compression.
//
// Tail compression is where the tail of the child page key is not stored actually stored
// in the directory node.
// Consider if a child node splits. If the largest key in the left page
// is "aeaf" and the smallest key in the page to the right is "aecd" then the directory entry
// for the for the new page to the right can be "aec". Anything less than "aec" will go to
// the page to the left and we do not need to store the "d" in the diectory node.
//
// For the page cache we want to be able to return immutable shared references to pages to
// clients who are read only, ie just using get. While for clients who want to add or delete
// tuples we need to provide a mutable version of the page, we provide a copy.
//
// This has caused complexity in the interface as we attempt to cater for both use cases.
// For the clients who want to mutate the page they key a copy of the page and use self,
// for clients who are read only they provide a reference to the underlying page and
// call the "static" methods here. This could be bad Rust and not idomatic.
//
// For an example of this see get_entries_size which takes a &self and
// get_no_entries_in_page which takes &Page. get_no_entries_in_page should be used by
// read only clients providing a reference is a base Page.
impl DirPage {
    const HEADER_SIZE: usize = 35; // 8 + 8 + 2 + 2 + 1 + 2 + 1 + 2 + 1 + 8
    const VALUE_SIZE: usize = 8; // u64 page number of child page
    const SLOT_SIZE: usize = 3; // 2 (offset) + 1 (key_len)

    pub fn create_new(page_config: &DbConfig, page_number: PageNo, version: u64) -> Self {
        DirPage::new(
            page_config.block_size,
            page_config.page_size,
            page_number,
            version,
        )
    }

    fn new(block_size: usize, page_size: usize, page_number: PageNo, version: u64) -> Self {
        let mut page = Page::new(block_size, page_size);
        page.set_type(PageType::DirPage);
        page.set_page_number(page_number);
        let mut dir_page = DirPage { page };
        dir_page.set_free_space(page_size as u16 - DirPage::HEADER_SIZE as u16);
        dir_page.set_version(version);
        dir_page.set_page_to_left(PageNo::from_u64(0));
        dir_page.set_prefix_length(0);
        dir_page.set_entries_size(0);
        dir_page.set_left_fence_key(&[]);
        dir_page.set_right_fence_key(&[]);
        dir_page
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::DirPage {
            panic!("Page type is not DirPage");
        }
        DirPage { page }
    }

    // Used when rebuilding page when resetting fences.
    pub fn reset(&mut self, page_size: usize) {
        self.set_free_space(page_size as u16 - DirPage::HEADER_SIZE as u16);
        self.set_entries_size(0);
        self.clear_left_fence_key();
        self.clear_right_fence_key();
        self.set_prefix_length(0);
    }

    pub fn get_entries_size(&self) -> u16 {
        Self::get_no_entries_in_page(&self.page)
    }

    pub fn get_no_entries_in_page(page: &Page) -> u16 {
        // Could assert the page type here.
        let bytes = &page.get_page_bytes()[16..18];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    fn set_entries_size(&mut self, entries: u16) {
        let bytes = entries.to_le_bytes();
        self.page.get_page_bytes_mut()[16..18].copy_from_slice(&bytes);
    }

    fn get_free_space(&self) -> u16 {
        let bytes = &self.page.get_page_bytes()[18..20];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    fn set_free_space(&mut self, free_space: u16) {
        let bytes = free_space.to_le_bytes();
        self.page.get_page_bytes_mut()[18..20].copy_from_slice(&bytes);
    }

    fn get_prefix_length_page(page: &Page) -> u8 {
        page.get_page_bytes()[20]
    }

    fn get_prefix_length(&self) -> u8 {
        Self::get_prefix_length_page(&self.page)
    }

    pub fn get_page_to_left_page(page: &Page) -> PageNo {
        PageNo::from_bytes(&page.get_page_bytes()[27..35])
    }

    pub fn get_page_to_left(&self) -> PageNo {
        Self::get_page_to_left_page(&self.page)
    }

    fn set_page_to_left(&mut self, page_no: PageNo) {
        self.page.get_page_bytes_mut()[27..35].copy_from_slice(&page_no.get_bytes());
    }

    pub fn get_dir_left_key(&self) -> Option<Vec<u8>> {
        if self.get_entries_size() == 0 {
            return None;
        }
        Some(self.get_key_at_index(0))
    }

    pub fn is_empty(&self) -> bool {
        self.get_page_to_left() == PageNo::from_u64(0)
    }

    pub fn set_left_fence_key(&mut self, key: &[u8]) {
        assert!(
            self.get_entries_size() == 0,
            "Cannot set left fence key on a page that already has entries."
        );
        let free_space = self.get_free_space() as usize;
        let offset = self.calculate_entries_offset() - key.len();
        self.page.get_page_bytes_mut()[offset..offset + key.len()].copy_from_slice(key);
        let offset_bytes = (offset as u16).to_le_bytes();
        self.page.get_page_bytes_mut()[21..23].copy_from_slice(&offset_bytes);
        self.page.get_page_bytes_mut()[23] = key.len() as u8;
        self.set_free_space(free_space as u16 - key.len() as u16);
    }

    pub fn has_left_fence_page(page: &Page) -> bool {
        page.get_page_bytes()[23] != 0
    }

    pub fn has_left_fence(&self) -> bool {
        Self::has_left_fence_page(&self.page)
    }

    fn clear_left_fence_key(&mut self) {
        self.page.get_page_bytes_mut()[23] = 0;
        self.page.get_page_bytes_mut()[21..23].copy_from_slice(&[0, 0]);
    }

    fn get_left_fence_key_size_page(page: &Page) -> u8 {
        page.get_page_bytes()[23]
    }

    fn get_left_fence_key_offset_page(page: &Page) -> u16 {
        let bytes = &page.get_page_bytes()[21..23];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    fn get_left_fence_key_page(page: &Page) -> &[u8] {
        let offset = Self::get_left_fence_key_offset_page(page) as usize;
        let size = Self::get_left_fence_key_size_page(page) as usize;
        &page.get_page_bytes()[offset..offset + size]
    }

    fn get_left_fence_key(&self) -> &[u8] {
        Self::get_left_fence_key_page(&self.page)
    }

    pub fn set_right_fence_key(&mut self, key: &[u8]) {
        assert!(
            self.get_entries_size() == 0,
            "Cannot set right fence key on a page that already has entries."
        );
        let free_space = self.get_free_space() as usize;
        let offset = self.calculate_entries_offset() - key.len();
        self.page.get_page_bytes_mut()[offset..offset + key.len()].copy_from_slice(key);
        let offset_bytes = (offset as u16).to_le_bytes();
        self.page.get_page_bytes_mut()[24..26].copy_from_slice(&offset_bytes);
        self.page.get_page_bytes_mut()[26] = key.len() as u8;
        self.set_free_space(free_space as u16 - key.len() as u16);
    }

    pub fn has_right_fence_page(page: &Page) -> bool {
        page.get_page_bytes()[26] != 0
    }

    pub fn has_right_fence(&self) -> bool {
        Self::has_right_fence_page(&self.page)
    }

    fn clear_right_fence_key(&mut self) {
        self.page.get_page_bytes_mut()[26] = 0;
        self.page.get_page_bytes_mut()[24..26].copy_from_slice(&[0, 0]);
    }

    fn get_right_fence_key_offset_page(page: &Page) -> u16 {
        let bytes = &page.get_page_bytes()[24..26];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    fn get_right_fence_key_size_page(page: &Page) -> u8 {
        page.get_page_bytes()[26]
    }

    fn get_right_fence_key_size(&self) -> u8 {
        Self::get_right_fence_key_size_page(&self.page)
    }

    fn get_right_fence_key_page(page: &Page) -> &[u8] {
        let offset = Self::get_right_fence_key_offset_page(page) as usize;
        let size = Self::get_right_fence_key_size_page(page) as usize;
        &page.get_page_bytes()[offset..offset + size]
    }

    fn get_right_fence_key(&self) -> &[u8] {
        Self::get_right_fence_key_page(&self.page)
    }

    pub fn set_prefix_length(&mut self, prefix_length: u8) {
        assert!(
            self.get_entries_size() == 0,
            "Cannot set prefix length on a page that already has entries."
        );
        assert!(
            prefix_length <= self.get_right_fence_key_size(),
            "Prefix length cannot be larger than the right fence key size."
        );
        self.page.get_page_bytes_mut()[20] = prefix_length;
    }

    fn get_slot_at_index_page(page: &Page, index: usize) -> DirSlot {
        assert!(index < Self::get_no_entries_in_page(page) as usize);
        let slot_offset = DirPage::HEADER_SIZE + index * DirPage::SLOT_SIZE;
        let offset_bytes = &page.get_page_bytes()[slot_offset..slot_offset + 2];
        let offset = u16::from_le_bytes(offset_bytes.try_into().unwrap());
        let key_len = page.get_page_bytes()[slot_offset + 2];
        DirSlot { offset, key_len }
    }

    fn get_slot_at_index(&self, index: usize) -> DirSlot {
        Self::get_slot_at_index_page(&self.page, index)
    }

    fn set_slot_at_index(&mut self, index: usize, slot: DirSlot) {
        let slot_offset = DirPage::HEADER_SIZE + index * DirPage::SLOT_SIZE;
        let offset_bytes = slot.offset.to_le_bytes();
        self.page.get_page_bytes_mut()[slot_offset..slot_offset + 2].copy_from_slice(&offset_bytes);
        self.page.get_page_bytes_mut()[slot_offset + 2] = slot.key_len;
    }

    fn get_value_at_slot_page<'a>(page: &'a Page, slot: &DirSlot) -> &'a [u8] {
        let val_offset = (slot.offset + slot.key_len as u16) as usize;
        &page.get_page_bytes()[val_offset..val_offset + DirPage::VALUE_SIZE]
    }

    fn get_value_at_slot(&self, slot: &DirSlot) -> &[u8] {
        Self::get_value_at_slot_page(&self.page, slot)
    }

    fn get_key_at_slot_page<'a>(page: &'a Page, slot: &DirSlot) -> &'a [u8] {
        let key_offset = slot.offset as usize;
        &page.get_page_bytes()[key_offset..key_offset + slot.key_len as usize]
    }

    fn get_key_at_slot(&self, slot: &DirSlot) -> &[u8] {
        Self::get_key_at_slot_page(&self.page, slot)
    }

    fn get_key_prefix_page(page: &Page) -> &[u8] {
        let prefix_length = Self::get_prefix_length_page(page) as usize;
        if prefix_length == 0 {
            return &[];
        }
        &Self::get_left_fence_key_page(page)[0..prefix_length]
    }

    fn get_key_prefix(&self) -> &[u8] {
        Self::get_key_prefix_page(&self.page)
    }

    fn get_index_for_key_page(page: &Page, key_suffix: &[u8]) -> (bool, usize) {
        let entries = Self::get_no_entries_in_page(page) as usize;

        // binary search for the key suffix in the slots
        let mut low = 0;
        let mut high = entries;

        while low < high {
            let mid = low + (high - low) / 2;
            let slot = Self::get_slot_at_index_page(page, mid);
            let key_at_slot = Self::get_key_at_slot_page(page, &slot);

            match key_suffix.cmp(key_at_slot) {
                Ordering::Less => high = mid, // Needle is smaller, look in the left half
                Ordering::Equal => return (true, mid),
                Ordering::Greater => low = mid + 1, // Needle is larger, look in the right half
            }
        }
        // low is the insertion point if the key wasn't found
        (false, low)
    }

    fn get_index_for_key(&self, key_suffix: &[u8]) -> (bool, usize) {
        Self::get_index_for_key_page(&self.page, key_suffix)
    }

    fn shift_slots_right_from(&mut self, from_index: usize) {
        let entries = self.get_entries_size() as usize;
        if entries == from_index {
            return;
        }
        self.page.get_page_bytes_mut().copy_within(
            DirPage::HEADER_SIZE + from_index * DirPage::SLOT_SIZE
                ..DirPage::HEADER_SIZE + entries * DirPage::SLOT_SIZE,
            DirPage::HEADER_SIZE + (from_index + 1) * DirPage::SLOT_SIZE,
        );
    }

    fn shift_slots_left_from(&mut self, from_index: usize) {
        let entries = self.get_entries_size() as usize;
        self.page.get_page_bytes_mut().copy_within(
            DirPage::HEADER_SIZE + (from_index + 1) * DirPage::SLOT_SIZE
                ..DirPage::HEADER_SIZE + entries * DirPage::SLOT_SIZE,
            DirPage::HEADER_SIZE + from_index * DirPage::SLOT_SIZE,
        );
    }

    // There are two ways to in which a child page no can be added to the dir page,
    // - if we are updating an existing entry, we need to find the correct entry (the key may
    //   not be an exact match)
    // - if a child page has split then we need to update on entry and add a new entry.
    // This function is to update an existing key with a new page number.
    fn update_child_page_no(&mut self, key: &[u8], page_no: u64) {
        let entries = self.get_entries_size() as usize;
        // Page empty - we can just add the page number as the left most page and return.
        if entries == 0 {
            self.set_page_to_left(PageNo::from_u64(page_no));
            return;
        }

        // Sanity check - we are updating an entry in this dir page
        // as we have just updated a child page. This means we have just
        // found the old child page reference when looking for the leaf page
        if self.has_left_fence() && key < self.get_left_fence_key() {
            self.set_page_to_left(PageNo::from_u64(page_no));
            return;
        }

        if self.has_right_fence() && key > self.get_right_fence_key() {
            let slot_to_update = self.get_slot_at_index(entries - 1);
            let val_offset = (slot_to_update.offset + slot_to_update.key_len as u16) as usize;
            let val_bytes = page_no.to_le_bytes();
            self.page.get_page_bytes_mut()[val_offset..val_offset + DirPage::VALUE_SIZE]
                .copy_from_slice(&val_bytes);
            return;
        }

        let prefix_length = self.get_prefix_length() as usize;
        let prefix = self.get_key_prefix();
        assert!(
            key.len() >= prefix_length,
            "BUG: Key length is smaller than the prefix length of the page."
        );
        assert!(
            key.starts_with(prefix),
            "BUG: Key does not match the prefix of the page."
        );
        let key_suffix = &key[prefix_length..];

        // Get first key and check if the key belongs to the left most page.
        let slot = self.get_slot_at_index(0);
        if key_suffix < self.get_key_at_slot(&slot) {
            // The key belongs to the left most page. We just need to update the page number for the left most page.
            self.set_page_to_left(PageNo::from_u64(page_no));
            return;
        }

        // The key does not belong to the left most page. We need to find the correct entry and update the page number.
        let (found, index) = self.get_index_for_key(key_suffix);
        let index_to_update = if found { index } else { index - 1 };
        let slot_to_update = self.get_slot_at_index(index_to_update);
        let val_offset = (slot_to_update.offset + slot_to_update.key_len as u16) as usize;
        let val_bytes = page_no.to_le_bytes();
        self.page.get_page_bytes_mut()[val_offset..val_offset + DirPage::VALUE_SIZE]
            .copy_from_slice(&val_bytes);
    }

    fn reset_with_new_right_fence(&mut self, new_right_fence: &[u8]) -> bool {
        // TODO - This is suboptimal approach. Should create a new page buffer,
        // rewrite the buffer with the new fence - if it does not fit then throw
        // away the page buffer and request a split. If it does fit then reset
        // the self page to use the new buffer. Currently we are copying the page
        // twice - the new approach would only require one copy.
        // The get_key_values is effectively a copy - we should be able to iterate
        // over the key/values.
        //
        // Need a full copy of the left fence as we are going to nuke it in the page.
        let page_copy = self.page.get_page_bytes_mut().to_vec();
        let old_prefix_length = self.get_prefix_length() as usize;
        let left_fence = self.get_left_fence_key().to_vec();
        let prefix_length: usize = if old_prefix_length > 0 {
            // Only set compression if it was already set.
            left_fence
                .iter()
                .zip(new_right_fence)
                .take_while(|(a, b)| a == b)
                .count()
        } else {
            0
        };
        // Get full copy of all tuples
        let entries = self.get_key_values();
        self.reset(self.get_pg_size());
        self.set_left_fence_key(left_fence.as_ref());
        self.set_right_fence_key(new_right_fence);
        self.set_prefix_length(prefix_length as u8);
        for tuple in entries {
            let ok = self.add_child_page(tuple.0.as_ref(), tuple.1);
            if !ok {
                // Cannot rebuild page with new compression, page not big enough.
                // Reset page back back to original bits and trigger a split.
                self.page.get_page_bytes_mut().copy_from_slice(&page_copy);
                return false;
            }
        }
        true
    }

    fn reset_with_new_left_fence(&mut self, new_left_fence: &[u8]) -> bool {
        // Copy the page increase we have to roll it back if there is not enough room after
        // recompressing.
        let page_copy = self.page.get_page_bytes_mut().to_vec();
        let old_prefix_length = self.get_prefix_length() as usize;
        // Need a full copy of the right fence as we are going to nuke it in the page.
        let right_fence = self.get_right_fence_key().to_vec();
        let prefix_length: usize = if old_prefix_length > 0 {
            // Only set compression if it was already set.
            new_left_fence
                .iter()
                .zip(right_fence.as_slice())
                .take_while(|(a, b)| a == b)
                .count()
        } else {
            0
        };
        // Get full copy of all tuples
        let entries = self.get_key_values();
        self.reset(self.get_pg_size());
        self.set_left_fence_key(new_left_fence);
        self.set_right_fence_key(right_fence.as_ref());
        self.set_prefix_length(prefix_length as u8);
        for tuple in entries {
            let ok = self.add_child_page(tuple.0.as_ref(), tuple.1);
            if !ok {
                // Cannot rebuild page with new compression, page not big enough.
                // Reset page back back to original bits and trigger a split.
                self.page.get_page_bytes_mut().copy_from_slice(&page_copy);
                return false;
            }
        }
        true
    }

    // Called when a child page has split and we need to add a
    // new entry for the new page in the dir_page.
    fn add_child_page(&mut self, key: &[u8], page_no: u64) -> bool {
        if self.has_left_fence() && key < self.get_left_fence_key() {
            if !self.reset_with_new_left_fence(key) {
                // Reset failed as cannot rebuild same page with new compression as not enough space.
                // Trigger a split first.
                return false;
            }
            // recursively call add_child_page on reset page.
            return self.add_child_page(key, page_no);
        }

        if self.has_right_fence() && key > self.get_right_fence_key() {
            if !self.reset_with_new_right_fence(key) {
                // Reset failed as cannot rebuild same page with new compression as not enough space.
                // Trigger a split first.
                return false;
            }
            // recursively call add_child_page on reset page.
            return self.add_child_page(key, page_no);
        }

        let prefix_length = self.get_prefix_length() as usize;
        assert!(
            key.len() >= prefix_length,
            "BUG Key length is smaller than the prefix length of the page."
        );
        assert!(
            key.starts_with(self.get_key_prefix()),
            "BUG: Key does not match the prefix of the page."
        );
        let key_suffix = &key[prefix_length..];

        let key_suffix_len = key.len() - prefix_length;
        let new_entry_size = key_suffix_len + DirPage::VALUE_SIZE; // 8 bytes for the page number
        let new_entry_total_size = new_entry_size + DirPage::SLOT_SIZE;
        let free_space = self.get_free_space() as usize;

        if new_entry_total_size > free_space {
            return false;
        }

        let (found, index) = self.get_index_for_key(key_suffix);
        assert!(
            !found,
            "BUG: Key already exists in the page when adding a new child page"
        );
        self.add_key_value_at_index(index, key_suffix, &page_no.to_le_bytes());
        true
    }

    pub fn store_child_pages(&mut self, child_entries: &[tree_dir_entry::TreeDirEntry]) -> bool {
        // Child has not split - just update the page number for the child page.
        // This means we only have one child entry and we just need to update the page number for that entry.
        if child_entries.len() == 1 {
            self.update_child_page_no(child_entries[0].get_key(), child_entries[0].get_page_no());
            return true;
        }

        // Child pages have split - need to handle. Take copy of page to allow rollback
        let page_copy = self.page.get_page_bytes_mut().to_vec();
        self.update_child_page_no(child_entries[0].get_key(), child_entries[0].get_page_no());
        for child_entry in child_entries.iter().skip(1) {
            if !self.add_child_page(child_entry.get_key(), child_entry.get_page_no()) {
                self.page.get_page_bytes_mut().copy_from_slice(&page_copy);
                return false;
            }
        }
        true
    }

    fn calculate_entries_offset(&self) -> usize {
        let free_space = self.get_free_space() as usize;
        let entries = self.get_entries_size() as usize;
        let header_plus_slots_size = DirPage::HEADER_SIZE + entries * DirPage::SLOT_SIZE;
        header_plus_slots_size + free_space
    }

    fn add_key_value_at_index(&mut self, index: usize, key: &[u8], value: &[u8]) {
        // Sanity check
        let new_entry_size = key.len() + value.len();
        let new_entry_total_size = new_entry_size + DirPage::SLOT_SIZE;
        let free_space = self.get_free_space() as usize;
        assert!(new_entry_total_size <= free_space);

        // Find offset where the key/value entry can be added.
        let entries = self.get_entries_size() as usize;
        let old_entries_offset = self.calculate_entries_offset();
        let new_entry_offset = old_entries_offset - new_entry_size;

        // Add key/value at the offset
        self.page.get_page_bytes_mut()[new_entry_offset..new_entry_offset + key.len()]
            .copy_from_slice(key);
        self.page.get_page_bytes_mut()
            [new_entry_offset + key.len()..new_entry_offset + key.len() + value.len()]
            .copy_from_slice(value);

        // Create a slot and add it.
        let slot = DirSlot {
            offset: new_entry_offset as u16,
            key_len: key.len() as u8,
        };
        self.shift_slots_right_from(index);
        self.set_slot_at_index(index, slot);

        // Update entries and free space.
        self.set_entries_size((entries + 1) as u16);
        self.set_free_space(free_space as u16 - new_entry_total_size as u16);
    }

    fn get_page_no_at_index_page(page: &Page, index: usize) -> PageNo {
        let slot = Self::get_slot_at_index_page(page, index);
        PageNo::from_bytes(&Self::get_value_at_slot_page(page, &slot)[0..8])
    }

    fn get_page_no_at_index(&self, index: usize) -> PageNo {
        Self::get_page_no_at_index_page(&self.page, index)
    }

    pub fn get_key_suffix_and_value_at_index(&self, index: usize) -> (&[u8], &[u8]) {
        let slot = self.get_slot_at_index(index);
        let key = self.get_key_at_slot(&slot);
        let value = self.get_value_at_slot(&slot);
        (key, value)
    }

    fn get_key_suffix_at_index(&self, index: usize) -> &[u8] {
        let slot = self.get_slot_at_index(index);
        self.get_key_at_slot(&slot)
    }

    fn get_key_at_index(&self, index: usize) -> Vec<u8> {
        let slot = self.get_slot_at_index(index);
        let key_suffix = self.get_key_at_slot(&slot);
        let prefix_length = self.get_prefix_length() as usize;
        if prefix_length == 0 {
            return key_suffix.to_vec();
        }
        let left_fence_key = self.get_left_fence_key();

        let mut key = Vec::with_capacity(prefix_length + key_suffix.len());
        key.extend_from_slice(&left_fence_key[..prefix_length]);
        key.extend_from_slice(key_suffix);
        key
    }

    fn get_key_values(&self) -> Vec<(Vec<u8>, u64)> {
        let mut key_values = Vec::with_capacity(self.get_entries_size() as usize);
        for i in 0..self.get_entries_size() as usize {
            let key = self.get_key_at_index(i).to_vec();
            let slot = self.get_slot_at_index(i);
            let value = u64::from_le_bytes(self.get_value_at_slot(&slot)[0..8].try_into().unwrap());
            key_values.push((key, value));
        }
        key_values
    }

    fn split_page_1(&self, _db_config: &DbConfig, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        // First page - no left or right pages. This means no
        // prefix, no right fence key and no left fence key.
        // When split the page on the left will have no left fence but will
        // have a right fence.
        // The current page has no prefix.
        // When split the new page on the right will have a left fence
        // but no right fence. Both pages will have no prefix.
        let mut left_page = DirPage::new(
            self.page.get_pg_ctr_bytes().len(),
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = DirPage::new(
            self.page.get_pg_ctr_bytes().len(),
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        let entries = self.get_entries_size() as usize;
        let mid = entries / 2;

        // Get the key suffix for mid key - there is no prefix
        // so this will be the full key.
        let mid_key = self.get_key_suffix_at_index(mid);
        // Page to the left remains the same for the new page on the left.
        left_page.set_page_to_left(self.get_page_to_left());
        left_page.set_right_fence_key(self.get_key_suffix_at_index(mid - 1));
        left_page.set_prefix_length(0);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, key, value);
        }

        // For the right page there is no right fence or prefix.
        // Set the left fence to the mid_key - this also the page to the left.
        right_page.set_page_to_left(self.get_page_no_at_index(mid));
        right_page.set_left_fence_key(self.get_key_suffix_at_index(mid + 1));
        right_page.set_prefix_length(0);
        for i in (mid + 1)..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(i - (mid + 1), key, value);
        }

        (left_page, right_page, mid_key.to_vec())
    }

    fn split_page_2(&self, _db_config: &DbConfig, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        // Left Page. Has right fence but no left fence. There is no prefix
        // and a right fence key.
        // New page to the left will have no left fence and the right fence will be the mid key, it
        // will have no prefix.
        // New page to the right will have a left fence which is the mid key and the right of the
        // current page. The new right page will have a prefix.
        let mut left_page = DirPage::new(
            self.page.get_pg_ctr_bytes().len(),
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = DirPage::new(
            self.page.get_pg_ctr_bytes().len(),
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        let entries = self.get_entries_size() as usize;
        let mid = entries / 2;

        // No prefix so we can just copy the key suffixes as they are.
        let mid_key = self.get_key_suffix_at_index(mid);
        left_page.set_page_to_left(self.get_page_to_left());
        left_page.set_right_fence_key(self.get_key_suffix_at_index(mid - 1));
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, key, value);
        }

        // Right fence for new page to the right remains the same.
        let right_lowest_key = self.get_key_at_index(mid + 1);
        let right_fence_right_key = self.get_key_at_index(entries - 1);
        assert!(
            right_fence_right_key > right_lowest_key,
            "BUG: Right page right fence key is not greater than right page left fence key."
        );
        let right_prefix_length = right_lowest_key
            .as_slice()
            .iter()
            .zip(right_fence_right_key.as_slice())
            .take_while(|(a, b)| a == b)
            .count();
        right_page.set_page_to_left(self.get_page_no_at_index(mid));
        right_page.set_left_fence_key(right_lowest_key.as_ref());
        right_page.set_right_fence_key(right_fence_right_key.as_ref());
        right_page.set_prefix_length(right_prefix_length as u8);
        for i in (mid + 1)..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // Use the prefix length to only store the key suffix.
            right_page.add_key_value_at_index(i - (mid + 1), &key[right_prefix_length..], value);
        }

        (left_page, right_page, mid_key.to_vec())
    }

    fn split_page_3(&self, _db_config: &DbConfig, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        // Right Page - has left fence but no right fence. This means no prefix
        // and no right fence key.
        // New page to the left will have a left fence and right fence with a prefix.
        // New page to the right will have a left fence and no right fence and no prefix.
        let mut left_page = DirPage::new(
            self.page.get_pg_ctr_bytes().len(),
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = DirPage::new(
            self.page.get_pg_ctr_bytes().len(),
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        let entries = self.get_entries_size() as usize;
        let mid = entries / 2;

        // Create page to the left.
        // No prefix so we can just copy the key suffixes as they are.
        let low_key = &self.get_key_at_index(0);
        // No prefix in self so can use suffix as the full key for the mid key.
        let mid_key = self.get_key_suffix_at_index(mid);
        let left_page_right_fence_key = self.get_key_suffix_at_index(mid - 1);
        assert!(
            left_page_right_fence_key > low_key,
            "BUG: Left page right fence key is not greater than left page left fence key."
        );
        left_page.set_page_to_left(self.get_page_to_left());
        left_page.set_left_fence_key(low_key);
        left_page.set_right_fence_key(left_page_right_fence_key);
        let left_prefix_length = low_key
            .iter()
            .zip(left_page_right_fence_key)
            .take_while(|(a, b)| a == b)
            .count();
        left_page.set_prefix_length(left_prefix_length as u8);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // Use the prefix length to only store the key suffix.
            left_page.add_key_value_at_index(i, &key[left_prefix_length..], value);
        }

        // Create page to the right.
        right_page.set_left_fence_key(self.get_key_suffix_at_index(mid + 1));
        right_page.set_page_to_left(self.get_page_no_at_index(mid));
        for i in (mid + 1)..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(i - (mid + 1), key, value);
        }

        (left_page, right_page, mid_key.to_vec())
    }

    fn split_page_4(&self, _db_config: &DbConfig, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        // Center Page - has right and left fence and also a prefix.
        // This means we need to calculate the new prefix length for the left and right pages after the split.
        let mut left_page = DirPage::new(
            self.page.get_pg_ctr_bytes().len(),
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = DirPage::new(
            self.page.get_pg_ctr_bytes().len(),
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        let entries = self.get_entries_size() as usize;
        let mid = entries / 2;

        // Could have a prefix so need full keys.
        let low_key = self.get_key_at_index(0);
        let mid_key = self.get_key_at_index(mid);
        let left_page_right_fence_key = self.get_key_at_index(mid - 1);
        left_page.set_page_to_left(self.get_page_to_left());
        left_page.set_left_fence_key(&low_key);
        left_page.set_right_fence_key(&left_page_right_fence_key);
        assert!(
            left_page_right_fence_key > low_key,
            "BUG:Left page right fence key is not greater than left page left fence key."
        );
        let left_prefix_length = low_key
            .iter()
            .zip(left_page_right_fence_key.as_slice())
            .take_while(|(a, b)| a == b)
            .count();
        // The offset of the suffix in the key is the prefix length of the page.
        let left_prefix_offset = left_prefix_length - self.get_prefix_length() as usize;
        left_page.set_prefix_length(left_prefix_length as u8);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, &key[left_prefix_offset..], value);
        }

        let right_page_low_key = self.get_key_at_index(mid + 1);
        let right_page_high_key = self.get_key_at_index(entries - 1);
        let right_prefix_length = right_page_low_key
            .iter()
            .zip(right_page_high_key.as_slice())
            .take_while(|(a, b)| a == b)
            .count();
        let right_suffix_offset = right_prefix_length - self.get_prefix_length() as usize;
        right_page.set_page_to_left(self.get_page_no_at_index(mid));
        right_page.set_left_fence_key(right_page_low_key.as_slice());
        right_page.set_right_fence_key(&right_page_high_key);
        assert!(
            right_page_low_key < right_page_high_key,
            "BUG: Right page left fence key is not less than right page right fence key."
        );
        right_page.set_prefix_length(right_prefix_length as u8);
        for i in (mid + 1)..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(i - (mid + 1), &key[right_suffix_offset..], value);
        }

        (left_page, right_page, mid_key)
    }

    pub fn split_page(&self, db_config: &DbConfig, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        assert!(
            self.get_entries_size() > 2,
            "Cannot split a page with fewer than 3 entries."
        );
        // TODO the individual split methods have a lot of code in common, we can probably
        // refactor to share some of the code. The main differences are in how the prefix lengths
        // are calculated and fence keys are handled.

        // First page - no left or right pages.
        if !self.has_left_fence() && !self.has_right_fence() {
            // There will be no prefix. When the page is split the
            // there will be a Left Page and a Right neither will
            // have a prefix.
            return self.split_page_1(db_config, version);
        }

        // Left Page - has right fence but no left fence.
        if !self.has_left_fence() {
            return self.split_page_2(db_config, version);
        }

        // Right Page - has left fence but no right fence.
        if !self.has_right_fence() {
            return self.split_page_3(db_config, version);
        }

        // Center Page - has both left and right fences.
        self.split_page_4(db_config, version)
    }

    /**
     * The approach removes the bytes from the page and moves the entries
     * around to fill the gap. An alternative approach is to leave the
     * hole in the entries and attempt to fill it in when adding new entries.
     */
    fn remove_key_value_at_index(&mut self, index: usize) {
        let entries = self.get_entries_size() as usize;
        assert!(index < entries);
        let slot = self.get_slot_at_index(index);
        let entry_size = slot.key_len as usize + DirPage::VALUE_SIZE;

        let free_space = self.get_free_space() as usize;
        let header_plus_slots_size = DirPage::HEADER_SIZE + entries * DirPage::SLOT_SIZE;
        let entries_size = self.get_pg_size() - (header_plus_slots_size + free_space);
        let entries_offset = self.get_pg_size() - entries_size;
        let entry_offset = slot.offset as usize;

        // Shift slots to left to remove the slot at index.
        self.shift_slots_left_from(index);
        let new_entry_count = entries - 1;

        // Shift entries to left to remove the entry at index.
        // | Head | Entry_to_remove | Tail |
        // ->
        // | Head | Tail |
        if entry_offset == entries_offset {
            // No Head, just shift the tail to the left.
            // If the entry to remove is the last entry, we can just update the free
            // space and entries without shifting.
            self.set_free_space((free_space + entry_size + DirPage::SLOT_SIZE) as u16);
            self.set_entries_size((new_entry_count) as u16);
            return;
        }

        // Need to move some bytes in the entries and update the slot offsets for the
        // entries in the head that are being shifted.
        let head = entry_offset - entries_offset;
        self.page.get_page_bytes_mut().copy_within(
            entries_offset..entries_offset + head,
            entries_offset + entry_size,
        );

        // Need to update the slots in the head to reflect the shift in entries.
        let mut slot_offset = DirPage::HEADER_SIZE;
        for _i in 0..new_entry_count {
            let slot_offset_bytes = &self.page.get_page_bytes()[slot_offset..slot_offset + 2];
            let slot_entry_offset = u16::from_le_bytes(slot_offset_bytes.try_into().unwrap());
            if slot_entry_offset < entry_offset as u16 {
                // This slot is in the head, need to update the offset to reflect the shift.
                let new_offset = slot_entry_offset + entry_size as u16;
                let new_offset_bytes = new_offset.to_le_bytes();
                self.page.get_page_bytes_mut()[slot_offset..slot_offset + 2]
                    .copy_from_slice(&new_offset_bytes);
            }
            slot_offset += DirPage::SLOT_SIZE;
        }

        // Update entries and free space.
        self.set_entries_size(new_entry_count as u16);
        self.set_free_space((free_space + entry_size + DirPage::SLOT_SIZE) as u16);
    }

    pub fn get_next(&self, key: &[u8]) -> PageNo {
        Self::get_next_page(&self.page, key)
    }

    pub fn get_next_page(page: &Page, key: &[u8]) -> PageNo {
        // There is only the page to the left.
        let entries = Self::get_no_entries_in_page(page);
        if entries == 0 {
            return Self::get_page_to_left_page(page);
        }

        if Self::has_left_fence_page(page) && key < Self::get_left_fence_key_page(page) {
            return Self::get_page_to_left_page(page);
        }

        if Self::has_right_fence_page(page) && key > Self::get_right_fence_key_page(page) {
            return Self::get_page_no_at_index_page(page, entries as usize - 1);
        }

        // If we get here then if there is a left and right fence the key is between them
        // and the key should have the prefix if there is one.
        // If there is no left or right fence then there is no prefix and the prefix
        // length is zero.
        assert!(
            key.len() >= Self::get_prefix_length_page(page) as usize,
            "BUG: Key length is smaller than the prefix length of the page."
        );
        assert!(
            key.starts_with(Self::get_key_prefix_page(page)),
            "BUG: Key does not match the prefix of the page."
        );

        let key_suffix = &key[Self::get_prefix_length_page(page) as usize..];

        let slot = Self::get_slot_at_index_page(page, 0);
        let first_key = Self::get_key_at_slot_page(page, &slot);
        if key_suffix < first_key {
            return Self::get_page_to_left_page(page);
        }

        let last_entry = Self::get_slot_at_index_page(page, entries as usize - 1);
        let last_key = Self::get_key_at_slot_page(page, &last_entry);
        if key_suffix > last_key {
            return Self::get_page_no_at_index_page(page, entries as usize - 1);
        }

        let (found, index) = Self::get_index_for_key_page(page, key_suffix);
        if found {
            Self::get_page_no_at_index_page(page, index)
        } else {
            Self::get_page_no_at_index_page(page, index - 1)
        }
    }

    pub fn remove_key_page(&mut self, key: &[u8], page_no: u64) {
        let entries = self.get_entries_size();

        // There should only be the left most page.
        if entries == 0 {
            assert!(PageNo::from_u64(page_no) == self.get_page_to_left());
            self.set_page_to_left(PageNo::new(0, 0));
            return;
        }

        if self.has_right_fence() && key > self.get_right_fence_key() {
            let index = entries - 1;
            assert_eq!(
                PageNo::from_u64(page_no),
                self.get_page_no_at_index(index as usize)
            );
            self.remove_key_value_at_index(index as usize);
            return;
        }

        // If removing the left most page need to move the next page into its place.
        // There is a next page as entries > 0 from above.
        if PageNo::from_u64(page_no) == self.get_page_to_left() {
            let slot = self.get_slot_at_index(0);
            let new_left_most_page = PageNo::from_bytes(self.get_value_at_slot(&slot));
            // TODO should just copy bytes instead of uwrapping and rewrapping the page number.
            self.set_page_to_left(new_left_most_page);
            self.remove_key_value_at_index(0);
            return;
        }

        // Now check if its the first entry we need to remove
        let prefix_length = self.get_prefix_length() as usize;
        let key_suffix = &key[prefix_length..];
        let slot = self.get_slot_at_index(0);
        let first_key = self.get_key_at_slot(&slot);
        if key_suffix < first_key {
            assert_eq!(PageNo::from_u64(page_no), self.get_page_no_at_index(0));
            self.remove_key_value_at_index(0);
            return;
        }

        // Now get the index for the key and remove the entry.
        let (found, index) = self.get_index_for_key(key_suffix);
        if found {
            assert_eq!(PageNo::from_u64(page_no), self.get_page_no_at_index(index));
            self.remove_key_value_at_index(index);
        } else {
            assert!(
                index > 0,
                "Not found. Index should be positive. key {:?}, page_no {}, entries in page {},
            left_fence {:?}, right_fence {:?}, prefix_length {:?}",
                key,
                page_no,
                entries,
                self.get_left_fence_key(),
                self.get_right_fence_key(),
                self.get_prefix_length()
            );
            assert_eq!(
                PageNo::from_u64(page_no),
                self.get_page_no_at_index(index - 1),
                "Removing key {:?}, page_no {} but expected page_no {:?} at index {}",
                key,
                page_no,
                self.get_page_no_at_index(index - 1),
                index - 1
            );
            self.remove_key_value_at_index(index - 1);
        }
    }

    pub fn get_all_child_pages(&self) -> Vec<PageNo> {
        let mut child_pages = Vec::new();
        let pg_to_left = self.get_page_to_left();
        if pg_to_left.get_blk_offset() > 0 {
            child_pages.push(pg_to_left);
        }
        let entries = self.get_entries_size();
        if entries == 0 {
            return child_pages;
        }
        for i in 0..entries as usize {
            let slot = self.get_slot_at_index(i);
            let page_no = PageNo::from_bytes(&self.get_value_at_slot(&slot)[0..8]);
            child_pages.push(page_no);
        }
        child_pages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_new() {
        let page_config = DbConfig::builder()
            .block_size(1024)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(dir_page.get_page_number().get_blk_offset(), 1);
        assert_eq!(dir_page.get_version(), 0);
        assert_eq!(dir_page.get_entries_size(), 0);
        assert_eq!(
            dir_page.get_free_space(),
            1024 - DirPage::HEADER_SIZE as u16
        );
        assert!(!dir_page.has_left_fence());
        assert!(!dir_page.has_right_fence());
        assert_eq!(dir_page.get_prefix_length(), 0);
        assert_eq!(dir_page.get_page_to_left(), PageNo::from_u64(0));
        assert_eq!(dir_page.get_dir_left_key(), None);
    }

    #[test]
    #[should_panic(expected = "Page type is not DirPage")]
    fn test_invalid_page() {
        let page_config = DbConfig::builder()
            .block_size(1028)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut leaf_page = Page::new(page_config.block_size, page_config.page_size);
        leaf_page.set_type(PageType::LeafPage);
        let _dir_page = DirPage::from_page(leaf_page);
    }

    #[test]
    #[should_panic(expected = "Cannot set left fence key on a page that already has entries.")]
    fn test_cannot_set_left_fence_after_adding_entries() {
        let page_config = DbConfig::builder()
            .block_size(1028)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(dir_page.get_page_bytes().len(), 1024);
        let key1 = b"key1";
        let page_no1 = 2;
        dir_page.add_child_page(key1, page_no1);
        assert_eq!(dir_page.get_entries_size(), 1);
        // Cannot set left or right fence after adding entries.
        dir_page.set_left_fence_key(b"key0");
    }

    #[test]
    #[should_panic(expected = "Cannot set right fence key on a page that already has entries.")]
    fn test_cannot_set_right_fence_after_adding_entries() {
        let page_config = DbConfig::builder()
            .block_size(1028)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(dir_page.get_page_bytes().len(), 1024);
        assert_eq!(dir_page.get_all_child_pages(), vec![]);
        let key1 = b"key1";
        let page_no1 = 2;
        dir_page.add_child_page(key1, page_no1);
        assert_eq!(dir_page.get_entries_size(), 1);
        assert_eq!(dir_page.get_all_child_pages(), vec![PageNo::from_u64(2)]);
        // Cannot set left or right fence after adding entries.
        dir_page.set_right_fence_key(b"key0");
    }

    #[test]
    #[should_panic(expected = "Cannot split a page with fewer than 3 entries.")]
    fn test_cannot_split_page_with_less_than_3_entries() {
        let page_config = DbConfig::builder()
            .block_size(1028)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(dir_page.get_page_bytes().len(), 1024);
        let key1 = b"key1";
        let page_no1 = 2;
        dir_page.add_child_page(key1, page_no1);
        let key2 = b"key2";
        let page_no2 = 3;
        dir_page.add_child_page(key2, page_no2);

        assert_eq!(dir_page.get_entries_size(), 2);
        assert_eq!(
            dir_page.get_all_child_pages(),
            vec![PageNo::from_u64(2), PageNo::from_u64(3)]
        );
        dir_page.split_page(&page_config, 45);
    }

    #[test]
    #[should_panic(expected = "Cannot set prefix length on a page that already has entries.")]
    fn test_cannot_set_right_prefix_after_adding_entries() {
        let page_config = DbConfig::builder()
            .block_size(1028)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(dir_page.get_page_bytes().len(), 1024);
        let key1 = b"key1";
        let page_no1 = 2;
        dir_page.add_child_page(key1, page_no1);
        assert_eq!(dir_page.get_entries_size(), 1);
        // Cannot set left or right fence after adding entries.
        dir_page.set_prefix_length(3);
    }

    #[test]
    #[should_panic(expected = "Prefix length cannot be larger than the right fence key size.")]
    fn test_prefix_larger_than_right_fence() {
        let page_config = DbConfig::builder()
            .block_size(1028)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(dir_page.get_page_bytes().len(), 1024);
        let key1 = b"key1";
        let key2 = b"key2";
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(key2);
        dir_page.set_prefix_length(5);
    }

    #[test]
    fn test_add_child_page() {
        let page_config = DbConfig::builder()
            .block_size(1028)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(dir_page.get_page_bytes().len(), 1024);
        let key1 = b"key1";
        let key2 = b"key2";
        let page_no1 = 2;
        let page_no2 = 3;
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(b"key3");
        dir_page.set_prefix_length(3);
        dir_page.set_page_to_left(PageNo::from_u64(1));
        dir_page.add_child_page(key1, page_no1);
        dir_page.add_child_page(key2, page_no2);
        assert_eq!(dir_page.get_entries_size(), 2);
        assert_eq!(dir_page.get_next(key1), PageNo::from_u64(page_no1));
        assert_eq!(dir_page.get_next(key2), PageNo::from_u64(page_no2));
        assert_eq!(dir_page.get_left_fence_key(), key1);
        assert_eq!(dir_page.get_right_fence_key(), b"key3");
        assert_eq!(dir_page.get_prefix_length(), 3);
        assert_eq!(dir_page.get_page_to_left(), PageNo::from_u64(1));
    }

    #[test]
    fn test_add_child_page_reset_right_fence() {
        let page_config = DbConfig::builder()
            .block_size(1024)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let key1 = b"key1";
        let key2 = b"key2";
        let page_no1 = 2;
        let page_no2 = 3;
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(b"key3");
        dir_page.set_prefix_length(3);
        dir_page.set_page_to_left(PageNo::from_u64(1));
        dir_page.add_child_page(key1, page_no1);
        dir_page.add_child_page(key2, page_no2);
        assert_eq!(dir_page.get_entries_size(), 2);
        assert_eq!(dir_page.get_next(key1), PageNo::from_u64(page_no1));
        assert_eq!(dir_page.get_next(key2), PageNo::from_u64(page_no2));
        assert_eq!(dir_page.get_left_fence_key(), key1);
        assert_eq!(dir_page.get_right_fence_key(), b"key3");
        assert_eq!(dir_page.get_prefix_length(), 3);
        assert_eq!(dir_page.get_page_to_left(), PageNo::from_u64(1));
        let key3 = b"key4";
        dir_page.add_child_page(key3, 4);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_right_fence_key(), key3);
        assert_eq!(dir_page.get_prefix_length(), 3);
        assert_eq!(dir_page.get_page_to_left(), PageNo::from_u64(1));
    }

    #[test]
    fn test_add_child_page_reset_left_fence() {
        let page_config = DbConfig::builder()
            .block_size(1024)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let key1 = b"key2";
        let key2 = b"key3";
        let page_no1 = 2;
        let page_no2 = 3;
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(b"key4");
        dir_page.set_prefix_length(3);
        dir_page.set_page_to_left(PageNo::from_u64(1));
        dir_page.add_child_page(key1, page_no1);
        dir_page.add_child_page(key2, page_no2);
        assert_eq!(dir_page.get_entries_size(), 2);
        assert_eq!(dir_page.get_next(key1), PageNo::from_u64(page_no1));
        assert_eq!(dir_page.get_next(key2), PageNo::from_u64(page_no2));
        assert_eq!(dir_page.get_left_fence_key(), key1);
        assert_eq!(dir_page.get_right_fence_key(), b"key4");
        assert_eq!(dir_page.get_prefix_length(), 3);
        assert_eq!(dir_page.get_page_to_left(), PageNo::from_u64(1));
        let key3 = b"key1";
        dir_page.add_child_page(key3, 4);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_right_fence_key(), b"key4");
        assert_eq!(dir_page.get_left_fence_key(), key3);
        assert_eq!(dir_page.get_prefix_length(), 3);
        assert_eq!(dir_page.get_page_to_left(), PageNo::from_u64(1));
    }

    #[test]
    fn test_reset_left_fence_full() {
        let page_config = DbConfig::builder()
            .block_size(160)
            .page_size(112)
            .block_sanity_size(160 - 112)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let key1 = b"aaaaaaaaaaaaaaaaaaaa";
        let key2 = b"aaaaaaaaaaaaaaaaaaaz";
        let key3 = b"aaaaaaaaaaaaaaaaaaab";
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(key2);
        dir_page.set_prefix_length(19);
        dir_page.set_page_to_left(PageNo::from_u64(1));
        dir_page.add_child_page(key1, 2);
        dir_page.add_child_page(key2, 3);
        dir_page.add_child_page(key3, 4);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_free_space(), 1);
        let reset = dir_page.reset_with_new_left_fence(b"aaaa");
        assert!(!reset);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_free_space(), 1);
    }

    #[test]
    fn test_add_left_fence_full() {
        let page_config = DbConfig::builder()
            .block_size(160)
            .page_size(112)
            .block_sanity_size(160 - 112)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let key1 = b"aaaaaaaaaaaaaaaaaaaa";
        let key2 = b"aaaaaaaaaaaaaaaaaaaz";
        let key3 = b"aaaaaaaaaaaaaaaaaaab";
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(key2);
        dir_page.set_prefix_length(19);
        dir_page.set_page_to_left(PageNo::from_u64(1));
        dir_page.add_child_page(key1, 2);
        dir_page.add_child_page(key2, 3);
        dir_page.add_child_page(key3, 4);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_free_space(), 1);
        let ok = dir_page.add_child_page(b"aaaa", 5);
        assert!(!ok);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_free_space(), 1);
    }

    #[test]
    fn test_reset_right_fence_full() {
        let page_config = DbConfig::builder()
            .block_size(160)
            .page_size(112)
            .block_sanity_size(160 - 112)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let key1 = b"aaaaaaaaaaaaaaaaaaaa";
        let key2 = b"aaaaaaaaaaaaaaaaaaaz";
        let key3 = b"aaaaaaaaaaaaaaaaaaab";
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(key2);
        dir_page.set_prefix_length(19);
        dir_page.set_page_to_left(PageNo::from_u64(1));
        dir_page.add_child_page(key1, 2);
        dir_page.add_child_page(key2, 3);
        dir_page.add_child_page(key3, 4);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_free_space(), 1);
        let reset = dir_page.reset_with_new_right_fence(b"aaaaaaaaaaaaaaaaaaazaaa");
        assert!(!reset);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_free_space(), 1);
    }

    #[test]
    fn test_add_right_fence_full() {
        let page_config = DbConfig::builder()
            .block_size(160)
            .page_size(112)
            .block_sanity_size(160 - 112)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let key1 = b"aaaaaaaaaaaaaaaaaaaa";
        let key2 = b"aaaaaaaaaaaaaaaaaaaz";
        let key3 = b"aaaaaaaaaaaaaaaaaaab";
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(key2);
        dir_page.set_prefix_length(19);
        dir_page.set_page_to_left(PageNo::from_u64(1));
        dir_page.add_child_page(key1, 2);
        dir_page.add_child_page(key2, 3);
        dir_page.add_child_page(key3, 4);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_free_space(), 1);
        let ok = dir_page.add_child_page(b"aaaaaaaaaaaaaaaaaaazaaa", 5);
        assert!(!ok);
        assert_eq!(dir_page.get_entries_size(), 3);
        assert_eq!(dir_page.get_free_space(), 1);
    }

    #[test]
    fn test_get_next_page() {
        let page_config = DbConfig::builder()
            .block_size(1024)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);

        // Add left page.
        dir_page.set_page_to_left(PageNo::from_u64(1));

        // Add keys
        let key1 = b"key2";
        let page_no1 = 2;
        dir_page.add_child_page(key1, page_no1);
        let key2 = b"key5";
        let page_no2 = 5;
        dir_page.add_child_page(key2, page_no2);
        let key3 = b"key7";
        let page_no3 = 7;
        dir_page.add_child_page(key3, page_no3);
        let key4 = b"key8";
        let page_no8 = 8;
        dir_page.add_child_page(key4, page_no8);

        assert_eq!(dir_page.get_next(b"key0"), PageNo::from_u64(1));
        assert_eq!(dir_page.get_next(b"key1"), PageNo::from_u64(1));
        assert_eq!(dir_page.get_next(b"key2"), PageNo::from_u64(2));
        assert_eq!(dir_page.get_next(b"key3"), PageNo::from_u64(2));
        assert_eq!(dir_page.get_next(b"key4"), PageNo::from_u64(2));
        assert_eq!(dir_page.get_next(b"key5"), PageNo::from_u64(5));
        assert_eq!(dir_page.get_next(b"key6"), PageNo::from_u64(5));
        assert_eq!(dir_page.get_next(b"key7"), PageNo::from_u64(7));
        assert_eq!(dir_page.get_next(b"key8"), PageNo::from_u64(8));
        assert_eq!(dir_page.get_next(b"key9"), PageNo::from_u64(8));

        dir_page.remove_key_page(b"key0", 1);
        assert_eq!(dir_page.get_page_to_left(), PageNo::from_u64(2));

        dir_page.remove_key_page(b"key6", 5);
        assert_eq!(dir_page.get_next(b"key6"), PageNo::from_u64(2));

        dir_page.remove_key_page(b"key9", 8);
        assert_eq!(dir_page.get_next(b"key8"), PageNo::from_u64(7));
    }

    #[test]
    fn test_split_page() {
        let page_config = DbConfig::builder()
            .block_size(1024)
            .page_size(1024)
            .block_sanity_size(0)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut dir_page = DirPage::create_new(&page_config, PageNo::new(0, 1), 0);
        for i in 0..20 {
            let key = (i as u64).to_le_bytes().to_vec();
            dir_page.add_child_page(&key, i as u64);
        }
        let (left_page, right_page, _) = dir_page.split_page(&page_config, 0);
        assert_eq!(left_page.get_entries_size(), 10);
        assert_eq!(right_page.get_entries_size(), 9);
        for i in 1..10 {
            let key = (i as u64).to_le_bytes().to_vec();
            assert_eq!(left_page.get_next(&key), PageNo::from_u64(i as u64));
        }
        for i in 11..20 {
            let key = (i as u64).to_le_bytes().to_vec();
            assert_eq!(right_page.get_next(&key), PageNo::from_u64(i as u64));
        }
    }
}

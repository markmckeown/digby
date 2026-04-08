use crate::page::PageTrait;
use crate::page::PageType;
use crate::tree_dir_entry;
use crate::{Page, block_layer::PageConfig};
use core::panic;
use std::cmp::Ordering;

pub struct DirPage {
    page: Page,
}

impl PageTrait for DirPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> u64 {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: u64) {
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
// | Page No (8 bytes) | VersionHolder(8 bytes) | Entries(u16) | Free_Space(u16) |
// | prefix_length (u8) | left_fence_key_offset (u16) | left_fence_key_size (u8) | right_fence_key_offset (u16) | right_fence_key_size (u8) |
// | page_to_the_left (u64) |
// | slot | slot | slot | ...
// | heap
// | key | value | key | value | right_fence_key | left_fence_key | ...
//
impl DirPage {
    const HEADER_SIZE: usize = 35; // 8 + 8 + 2 + 2 + 1 + 2 +1 + 2 + 1 + 8
    const VALUE_SIZE: usize = 8; // u64 page number of child page
    const SLOT_SIZE: usize = 3; // 2 (offset) + 1 (key_len)

    pub fn create_new(page_config: &PageConfig, page_number: u64, version: u64) -> Self {
        DirPage::new(
            page_config.block_size,
            page_config.page_size,
            page_number,
            version,
        )
    }

    fn new(block_size: usize, page_size: usize, page_number: u64, version: u64) -> Self {
        let mut page = Page::new(block_size, page_size);
        page.set_type(PageType::DirPage);
        page.set_page_number(page_number);
        let mut dir_page = DirPage { page };
        dir_page.set_free_space(page_size as u16 - DirPage::HEADER_SIZE as u16);
        dir_page.set_version(version);
        dir_page.set_page_to_left(0);
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

    pub fn reset(&mut self, page_size: usize) {
        self.set_free_space(page_size as u16 - DirPage::HEADER_SIZE as u16);
        self.set_entries_size(0);
        self.clear_left_fence_key();
        self.clear_right_fence_key();
        self.set_prefix_length(0);
    }

    pub fn get_entries_size(&self) -> u16 {
        let bytes = &self.page.get_page_bytes()[16..18];
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

    fn get_prefix_length(&self) -> u8 {
        self.page.get_page_bytes()[20]
    }

    fn get_page_to_left(&self) -> u64 {
        let bytes = &self.page.get_page_bytes()[27..35];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    fn set_page_to_left(&mut self, page_no: u64) {
        let bytes = page_no.to_le_bytes();
        self.page.get_page_bytes_mut()[27..35].copy_from_slice(&bytes);
    }


    // Get the left sided key in the page.
    pub fn get_dir_left_key(&self) -> Option<Vec<u8>> {
        if self.get_entries_size() == 0 {
            return None;
        }
        Some(self.get_key_at_index(0))
    }

    pub fn is_empty(&self) -> bool {
        self.get_page_to_left() == 0
    }

    pub fn set_left_fence_key(&mut self, key: &[u8]) {
        assert!(
            key.len() <= u8::MAX as usize,
            "Left fence key size larger than u8 can hold."
        );
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

    pub fn has_left_fence(&self) -> bool {
        self.page.get_page_bytes()[23] != 0
    }

    fn clear_left_fence_key(&mut self) {
        self.page.get_page_bytes_mut()[23] = 0;
        self.page.get_page_bytes_mut()[21..23].copy_from_slice(&[0, 0]);
    }

    pub fn get_left_fence_key_size(&self) -> u8 {
        self.page.get_page_bytes()[23]
    }

    pub fn get_left_fence_key_offset(&self) -> u16 {
        let bytes = &self.page.get_page_bytes()[21..23];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    fn get_left_fence_key(&self) -> &[u8] {
        let offset = self.get_left_fence_key_offset() as usize;
        let size = self.get_left_fence_key_size() as usize;
        &self.page.get_page_bytes()[offset..offset + size]
    }

    pub fn set_right_fence_key(&mut self, key: &[u8]) {
        assert!(
            key.len() <= u8::MAX as usize,
            "Right fence key size larger than u8 can hold."
        );
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

    pub fn has_right_fence(&self) -> bool {
        self.page.get_page_bytes()[26] != 0
    }


    fn clear_right_fence_key(&mut self) {
        self.page.get_page_bytes_mut()[26] = 0;
        self.page.get_page_bytes_mut()[24..26].copy_from_slice(&[0, 0]);
    }

    pub fn get_right_fence_key_offset(&self) -> u16 {
        let bytes = &self.page.get_page_bytes()[24..26];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn get_right_fence_key_size(&self) -> u8 {
        self.page.get_page_bytes()[26]
    }

    fn get_right_fence_key(&self) -> &[u8] {
        let offset = self.get_right_fence_key_offset() as usize;
        let size = self.get_right_fence_key_size() as usize;
        &self.page.get_page_bytes()[offset..offset + size]
    }

    pub fn set_prefix_length(&mut self, prefix_length: u8) {
        assert!(
            prefix_length <= u8::MAX as u8,
            "Prefix length larger than u8 can hold."
        );
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

    fn get_slot_at_index(&self, index: usize) -> DirSlot {
        assert!(index < self.get_entries_size() as usize);
        let slot_offset = DirPage::HEADER_SIZE + index * DirPage::SLOT_SIZE;
        let offset_bytes = &self.page.get_page_bytes()[slot_offset..slot_offset + 2];
        let offset = u16::from_le_bytes(offset_bytes.try_into().unwrap());
        let key_len = self.page.get_page_bytes()[slot_offset + 2];
        DirSlot { offset, key_len }
    }

    fn set_slot_at_index(&mut self, index: usize, slot: DirSlot) {
        let slot_offset = DirPage::HEADER_SIZE + index * DirPage::SLOT_SIZE;
        let offset_bytes = slot.offset.to_le_bytes();
        self.page.get_page_bytes_mut()[slot_offset..slot_offset + 2].copy_from_slice(&offset_bytes);
        self.page.get_page_bytes_mut()[slot_offset + 2] = slot.key_len;
    }

    fn get_value_at_slot(&self, slot: &DirSlot) -> &[u8] {
        let val_offset = (slot.offset + slot.key_len as u16) as usize;
        &self.page.get_page_bytes()[val_offset..val_offset + DirPage::VALUE_SIZE]
    }

    fn get_key_at_slot(&self, slot: &DirSlot) -> &[u8] {
        let key_offset = slot.offset as usize;
        &self.page.get_page_bytes()[key_offset..key_offset + slot.key_len as usize]
    }

    fn get_key_prefix(&self) -> &[u8] {
        let prefix_length = self.get_prefix_length() as usize;
        if prefix_length == 0 {
            return &[];
        }
        &self.get_left_fence_key()[0..prefix_length]
    }

    fn get_index_for_key(&self, key_suffix: &[u8]) -> (bool, usize) {
        let entries = self.get_entries_size() as usize;

        // binary search for the key suffix in the slots
        let mut low = 0;
        let mut high = entries;

        while low < high {
            let mid = low + (high - low) / 2;
            let slot = self.get_slot_at_index(mid);
            let key_at_slot = self.get_key_at_slot(&slot);

            match key_at_slot.cmp(key_suffix) {
                Ordering::Less => low = mid + 1,
                Ordering::Equal => return (true, mid),
                Ordering::Greater => high = mid,
            }
        }
        // low is the insertion point if the key wasn't found
        (false, low)
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
    // - if we are updating an existing entry, we need to find the correct entry (the key may not be an exact match)
    // - if a child page has split then we need to update on entry and add a new entry.
    fn update_child_page_no(&mut self, key: &[u8], page_no: u64) {
        let prefix_length = self.get_prefix_length() as usize;
        assert!(
            key.len() >= prefix_length,
            "Key length is smaller than the prefix length of the page."
        );
        assert!(
            key.starts_with(self.get_key_prefix()),
            "Key does not match the prefix of the page."
        );
        let key_suffix = &key[prefix_length..];

        let entries = self.get_entries_size() as usize;
        // Page empty - we can just add the page number as the left most page and return.
        if entries == 0 {
            self.set_page_to_left(page_no);
            return;
        }

        // Get first key and check if the key belongs to the left most page.
        let slot = self.get_slot_at_index(0);
        if key_suffix < self.get_key_at_slot(&slot) {
            // The key belongs to the left most page. We just need to update the page number for the left most page.
            self.set_page_to_left(page_no);
            return;
        }

        // The key does not belong to the left most page. We need to find the correct entry and update the page number.
        let (found, index) = self.get_index_for_key(key_suffix);
        let index_to_update;
        if found {
            index_to_update = index;
        } else {
            index_to_update = index - 1;
        }
        let slot_to_update = self.get_slot_at_index(index_to_update);
        let val_offset = (slot_to_update.offset + slot_to_update.key_len as u16) as usize;
        let val_bytes = page_no.to_le_bytes();
        self.page.get_page_bytes_mut()[val_offset..val_offset + DirPage::VALUE_SIZE]
            .copy_from_slice(&val_bytes);
    }


    fn reset_with_new_right_fence(&mut self, new_right_fence: &[u8]) -> bool {
        // Need a full copy of the left fence as we are going to nuke it in the page.
        let page_copy = self.page.get_page_bytes_mut().to_vec();
        let old_prefix_length = self.get_prefix_length() as usize;
        let left_fence = self.get_left_fence_key().to_vec();
        let prefix_length: usize;
        if old_prefix_length > 0 {
            // Only set compression if it was already set.
            prefix_length = left_fence
            .iter()
            .zip(new_right_fence)
            .take_while(|(a, b)| a == b)
            .count();
        } else {
           prefix_length = 0;
        }
        // Get full copy of all tuples
        let entries = self.get_key_values();
        self.reset(self.page.page_size);
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
        let prefix_length: usize;
        if old_prefix_length > 0 {
            // Only set compression if it was already set.
            prefix_length = new_left_fence
            .iter()
            .zip(right_fence.as_slice())
            .take_while(|(a, b)| a == b)
            .count();
        } else {
           prefix_length = 0;
        }
        // Get full copy of all tuples
        let entries = self.get_key_values();
        self.reset(self.page.page_size);
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
            "Key length is smaller than the prefix length of the page."
        );
        assert!(
            key.starts_with(self.get_key_prefix()),
            "Key does not match the prefix of the page."
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
            found == false,
            "Key already exists in the page when adding a new child page"
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
        for i in 1..child_entries.len() {
            if !self.add_child_page(child_entries[i].get_key(), child_entries[i].get_page_no()) {
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

    pub fn get_page_no_for_key(&self, key: &[u8]) -> Option<u64> {
        let prefix_length = self.get_prefix_length() as usize;
        if prefix_length > 0 {
            if key.len() < prefix_length {
                return None;
            }
            if !key.starts_with(self.get_key_prefix()) {
                return None;
            }
            //assert!(key.len() >= prefix_length, "Key length is smaller than the prefix length of the page.");
            //assert!(key.starts_with(self.get_key_prefix()), "Key does not match the prefix of the page.");
        }
        let (found, index) = self.get_index_for_key(&key[prefix_length..]);
        if !found {
            return None;
        }
        Some(self.get_page_no_at_index(index))
    }

    fn get_page_no_at_index(&self, index: usize) -> u64 {
        let slot = self.get_slot_at_index(index);
        u64::from_le_bytes(self.get_value_at_slot(&slot)[0..8].try_into().unwrap())
    }

    pub fn get_key_suffix_and_value_at_index(&self, index: usize) -> (&[u8], &[u8]) {
        let slot = self.get_slot_at_index(index);
        let key = self.get_key_at_slot(&slot);
        let value = self.get_value_at_slot(&slot);
        (&key, &value)
    }

    fn get_key_suffix_at_index(&self, index: usize) -> &[u8] {
        let slot = self.get_slot_at_index(index);
        self.get_key_at_slot(&slot)
    }

    fn get_key_at_index(&self, index: usize) -> Vec<u8> {
        let slot = self.get_slot_at_index(index);
        let key_suffix = self.get_key_at_slot(&slot);
        let prefix_length = self.get_prefix_length() as usize;
        let right_fence_key = self.get_right_fence_key();

        let mut key = Vec::with_capacity(prefix_length + key_suffix.len());
        key.extend_from_slice(&right_fence_key[..prefix_length]);
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

    fn split_page_1(&self, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        // First page - no left or right pages. This means no
        // prefix, no right fence key and no left fence key.
        // When split the page on the left will have no left fence but will
        // have a right fence.
        // The current page has no prefix.
        // When split the new page on the right will have a left fence
        // but no right fence. Both pages will have no prefix.
        let mut left_page = DirPage::create_new(
            &PageConfig {
                block_size: self.page.block_size,
                page_size: self.page.page_size,
            },
            0,
            version,
        );
        let mut right_page = DirPage::create_new(
            &PageConfig {
                block_size: self.page.block_size,
                page_size: self.page.page_size,
            },
            0,
            version,
        );

        let entries = self.get_entries_size() as usize;
        let mid = entries / 2;

        // Get the key suffix for mid key - there is no prefix
        // so this will be the full key.
        let mid_key = self.get_key_suffix_at_index(mid);
        // Page to the left remains the same for the new page on the left.
        left_page.set_page_to_left(self.get_page_to_left());
        // Set the right fence to the mid_key which will be the left key
        // for the new page on the right.
        left_page.set_right_fence_key(mid_key);
        left_page.set_prefix_length(0);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, &key, value);
        }

        // For the right page there is no right fence or prefix.
        // Set the left fence to the mid_key - this also the page to the left.
        right_page.set_page_to_left(self.get_page_no_at_index(mid));
        right_page.set_left_fence_key(mid_key);
        right_page.set_prefix_length(0);
        let mut right_offset = 0;
        for i in (mid + 1)..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(right_offset, &key, value);
            right_offset += 1;
        }

        (left_page, right_page, mid_key.to_vec())
    }

    fn split_page_2(&self, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        // Left Page. Has right fence but no left fence. There is no prefix
        // and a right fence key.
        // New page to the left will have no left fence and the right fence will be the mid key, it
        // will have no prefix.
        // New page to the right will have a left fence which is the mid key and the right of the
        // current page. The new right page will have a prefix.
        assert!(
            self.get_key_prefix().len() == 0,
            "Page has a prefix when splitting page with only a right fence."
        );
        let mut left_page = DirPage::create_new(
            &PageConfig {
                block_size: self.page.block_size,
                page_size: self.page.page_size,
            },
            0,
            version,
        );
        let mut right_page = DirPage::create_new(
            &PageConfig {
                block_size: self.page.block_size,
                page_size: self.page.page_size,
            },
            0,
            version,
        );

        let entries = self.get_entries_size() as usize;
        let mid = entries / 2;

        // No prefix so we can just copy the key suffixes as they are.
        let mid_key = self.get_key_suffix_at_index(mid);
        left_page.set_page_to_left(self.get_page_to_left());
        left_page.set_right_fence_key(mid_key);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, &key, value);
        }

        let right_fence_key = self.get_right_fence_key();
        let right_prefix_length = mid_key
            .iter()
            .zip(right_fence_key)
            .take_while(|(a, b)| a == b)
            .count();
        right_page.set_page_to_left(self.get_page_no_at_index(mid));
        right_page.set_left_fence_key(mid_key);
        right_page.set_right_fence_key(right_fence_key);
        right_page.set_prefix_length(right_prefix_length as u8);
        let mut right_offset = 0;
        for i in (mid + 1)..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // Use the prefix length to only store the key suffix.
            right_page.add_key_value_at_index(right_offset, &key[right_prefix_length..], value);
            right_offset += 1;
        }

        (left_page, right_page, mid_key.to_vec())
    }

    fn split_page_3(&self, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        // Right Page - has left fence but no right fence. This means no prefix
        // and no right fence key.
        // New page to the left will have a left fence and right fence with a prefix.
        // New page to the right will have a left fence and no right fence and no prefix.
        assert!(
            self.get_key_prefix().len() == 0,
            "Page has a prefix when splitting page with only a left fence."
        );
        let mut left_page = DirPage::create_new(
            &PageConfig {
                block_size: self.page.block_size,
                page_size: self.page.page_size,
            },
            0,
            version,
        );
        let mut right_page = DirPage::create_new(
            &PageConfig {
                block_size: self.page.block_size,
                page_size: self.page.page_size,
            },
            0,
            version,
        );

        let entries = self.get_entries_size() as usize;
        let mid = entries / 2;

        // Create page to the left.
        // No prefix so we can just copy the key suffixes as they are.
        let low_key = self.get_left_fence_key();
        // No prefix in self so can use suffix as the full key for the mid key.
        let mid_key = self.get_key_suffix_at_index(mid);
        left_page.set_page_to_left(self.get_page_to_left());
        left_page.set_left_fence_key(low_key);
        left_page.set_right_fence_key(mid_key);
        let left_prefix_length = low_key
            .iter()
            .zip(mid_key)
            .take_while(|(a, b)| a == b)
            .count();
        left_page.set_prefix_length(left_prefix_length as u8);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // Use the prefix length to only store the key suffix.
            left_page.add_key_value_at_index(i, &key[left_prefix_length..], value);
        }

        // Create page to the right.
        right_page.set_left_fence_key(mid_key);
        right_page.set_page_to_left(self.get_page_no_at_index(mid));
        let mut right_offset = 0;
        for i in (mid + 1)..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(right_offset, &key, value);
            right_offset += 1;
        }

        (left_page, right_page, mid_key.to_vec())
    }

    fn split_page_4(&self, version: u64) -> (DirPage, DirPage, Vec<u8>) {
        // Center Page - has right and left fence and also a prefix.
        // This means we need to calculate the new prefix length for the left and right pages after the split.
        let mut left_page = DirPage::create_new(
            &PageConfig {
                block_size: self.page.block_size,
                page_size: self.page.page_size,
            },
            0,
            version,
        );
        let mut right_page = DirPage::create_new(
            &PageConfig {
                block_size: self.page.block_size,
                page_size: self.page.page_size,
            },
            0,
            version,
        );

        let entries = self.get_entries_size() as usize;
        let mid = entries / 2;

        // Could have a prefix so need full keys.
        let low_key = self.get_left_fence_key();
        // Note we get full key for the mid.
        let mid_key = self.get_key_at_index(mid);
        left_page.set_page_to_left(self.get_page_to_left());
        left_page.set_left_fence_key(low_key);
        left_page.set_right_fence_key(mid_key.as_slice());
        let left_prefix_length = low_key
            .iter()
            .zip(mid_key.as_slice())
            .take_while(|(a, b)| a == b)
            .count();
        let left_prefix_offset = left_prefix_length - self.get_prefix_length() as usize; // The offset of the suffix in the key is the prefix length of the page.
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, &key[left_prefix_offset..], value);
        }

        let right_prefix_length = mid_key
            .iter()
            .zip(self.get_right_fence_key())
            .take_while(|(a, b)| a == b)
            .count();
        let right_suffix_offset = right_prefix_length - self.get_prefix_length() as usize;
        right_page.set_page_to_left(self.get_page_no_at_index(mid));
        right_page.set_left_fence_key(mid_key.as_slice());
        right_page.set_right_fence_key(self.get_right_fence_key());
        right_page.set_prefix_length(right_prefix_length as u8);
        let mut right_offset = 0;
        for i in (mid + 1)..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(right_offset, &key[right_suffix_offset..], value);
            right_offset += 1;
        }

        (left_page, right_page, mid_key.to_vec())
    }

    pub fn split_page(&self, version: u64) -> (DirPage, DirPage, Vec<u8>) {
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
            return self.split_page_1(version);
        }

        // Left Page - has right fence but no left fence.
        if !self.has_left_fence() {
            return self.split_page_2(version);
        }

        // Right Page - has left fence but no right fence.
        if !self.has_right_fence() {
            return self.split_page_3(version);
        }

        // Center Page - has both left and right fences.
        return self.split_page_4(version);
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
        let entry_size = slot.key_len as usize + DirPage::VALUE_SIZE as usize;

        let free_space = self.get_free_space() as usize;
        let header_plus_slots_size = DirPage::HEADER_SIZE + entries * DirPage::SLOT_SIZE;
        let entries_size = self.page.page_size - (header_plus_slots_size + free_space);
        let entries_offset = self.page.page_size - entries_size;
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
            // If the entry to remove is the last entry, we can just update the free space and entries without shifting.
            self.set_free_space((free_space + entry_size + DirPage::SLOT_SIZE) as u16);
            self.set_entries_size((new_entry_count) as u16);
            return;
        }

        // Need to move some bytes in the entries and update the slot offsets for the entries in the head that are being shifted.
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

    pub fn get_next_page(&self, key: &[u8]) -> u64 {
        let entries = self.get_entries_size();
        if entries == 0 {
            return self.get_page_to_left();
        }

        let key_suffix = &key[self.get_prefix_length() as usize..];

        let slot = self.get_slot_at_index(0);
        let first_key = self.get_key_at_slot(&slot);
        if key_suffix < first_key {
            return self.get_page_to_left();
        }

        let last_entry = self.get_slot_at_index(entries as usize - 1);
        let last_key = self.get_key_at_slot(&last_entry);
        if key_suffix > last_key {
            return self.get_page_no_at_index(entries as usize - 1);
        }

        let (found, index) = self.get_index_for_key(key_suffix);
        if found {
            self.get_page_no_at_index(index)
        } else {
            self.get_page_no_at_index(index - 1)
        }
    }

    pub fn remove_key_page(&mut self, key: &[u8], page_no: u64) {
        let entries = self.get_entries_size();

        // There should only be the left most page.
        if entries == 0 {
            assert!(page_no == self.get_page_to_left());
            self.set_page_to_left(0);
            return;
        }

        // If removing the left most page need to move the next page into its place.
        // There is a next page as entries > 0 from above.
        if page_no == self.get_page_to_left() {
            let slot = self.get_slot_at_index(0);
            assert!(key < self.get_key_at_slot(&slot));
            let new_left_most_page = self.get_value_at_slot(&slot);
            // TODO should just copy bytes instead of uwrapping and rewrapping the page number.
            self.set_page_to_left(u64::from_le_bytes(new_left_most_page.try_into().unwrap()));
            self.remove_key_value_at_index(0);
            return;
        }

        let key_suffix = &key[self.get_prefix_length() as usize..];
        // Now get the index for the key and remove the entry.
        let (found, index) = self.get_index_for_key(key_suffix);
        if found {
            assert_eq!(page_no, self.get_page_no_at_index(index));
            self.remove_key_value_at_index(index);
        } else {
            assert_eq!(page_no, self.get_page_no_at_index(index - 1));
            self.remove_key_value_at_index(index - 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_new() {
        let page_config = PageConfig {
            block_size: 1024,
            page_size: 1024,
        };
        let dir_page = DirPage::create_new(&page_config, 1, 0);
        assert_eq!(dir_page.get_page_number(), 1);
        assert_eq!(dir_page.get_version(), 0);
        assert_eq!(dir_page.get_entries_size(), 0);
        assert_eq!(
            dir_page.get_free_space(),
            1024 - DirPage::HEADER_SIZE as u16
        );
    }

    #[test]
    fn test_add_child_page() {
        let page_config = PageConfig {
            block_size: 1024,
            page_size: 1024,
        };
        let mut dir_page = DirPage::create_new(&page_config, 1, 0);
        let key1 = b"key1";
        let key2 = b"key2";
        let page_no1 = 2;
        let page_no2 = 3;
        dir_page.set_left_fence_key(key1);
        dir_page.set_right_fence_key(b"key3");
        dir_page.set_prefix_length(3);
        dir_page.set_page_to_left(1);
        dir_page.add_child_page(key1, page_no1);
        dir_page.add_child_page(key2, page_no2);
        assert_eq!(dir_page.get_entries_size(), 2);
        assert_eq!(dir_page.get_page_no_for_key(key1).unwrap(), page_no1);
        assert_eq!(dir_page.get_page_no_for_key(key2).unwrap(), page_no2);
        assert_eq!(dir_page.get_left_fence_key(), key1);
        assert_eq!(dir_page.get_right_fence_key(), b"key3");
        assert_eq!(dir_page.get_prefix_length(), 3);
        assert_eq!(dir_page.get_page_to_left(), 1);
    }

    #[test]
    fn test_get_next_page() {
        let page_config = PageConfig {
            block_size: 1024,
            page_size: 1024,
        };
        let mut dir_page = DirPage::create_new(&page_config, 1, 0);

        // Add left page.
        dir_page.set_page_to_left(1);

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

        assert_eq!(dir_page.get_next_page(b"key0"), 1);
        assert_eq!(dir_page.get_next_page(b"key1"), 1);
        assert_eq!(dir_page.get_next_page(b"key2"), 2);
        assert_eq!(dir_page.get_next_page(b"key3"), 2);
        assert_eq!(dir_page.get_next_page(b"key4"), 2);
        assert_eq!(dir_page.get_next_page(b"key5"), 5);
        assert_eq!(dir_page.get_next_page(b"key6"), 5);
        assert_eq!(dir_page.get_next_page(b"key7"), 7);
        assert_eq!(dir_page.get_next_page(b"key8"), 8);
        assert_eq!(dir_page.get_next_page(b"key9"), 8);

        dir_page.remove_key_page(b"key0", 1);
        assert_eq!(dir_page.get_page_to_left(), 2);

        dir_page.remove_key_page(b"key6", 5);
        assert_eq!(dir_page.get_next_page(b"key6"), 2);

        dir_page.remove_key_page(b"key9", 8);
        assert_eq!(dir_page.get_next_page(b"key8"), 7);
    }

    #[test]
    fn test_split_page() {
        let page_config = PageConfig {
            block_size: 1024,
            page_size: 1024,
        };
        let mut dir_page = DirPage::create_new(&page_config, 1, 0);
        for i in 0..20 {
            let key = (i as u64).to_le_bytes().to_vec();
            dir_page.add_child_page(&key, i as u64);
        }
        let (left_page, right_page, _) = dir_page.split_page(0);
        assert_eq!(left_page.get_entries_size(), 10);
        assert_eq!(right_page.get_entries_size(), 9);
        for i in 1..10 {
            let key = (i as u64).to_le_bytes().to_vec();
            assert_eq!(left_page.get_page_no_for_key(&key).unwrap(), i as u64);
        }
        for i in 11..20 {
            let key = (i as u64).to_le_bytes().to_vec();
            assert_eq!(right_page.get_page_no_for_key(&key).unwrap(), i as u64);
        }
    }
}

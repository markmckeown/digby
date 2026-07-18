use crate::VersionHolder;
use crate::page::PageTrait;
use crate::page::PageType;
use crate::page_no::PageNo;
use crate::tuple::Overflow;
use crate::tuple::Tuple;
use crate::tuple::TupleTrait;
use crate::{Page, db_config::DbConfig};
use core::panic;
use std::cmp::Ordering;

pub struct LeafPage {
    page: Page,
}

impl PageTrait for LeafPage {
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

pub struct LeafSlot {
    offset: u16,
    key_len: u8,
    val_len: u16,
}

// Header
// | Page No (8 bytes) | VersionHolder(8 bytes) | Entries(u16) | Free_Space(u16) |
// | prefix_length (u8) | left_fence_key_offset (u16) | left_fence_key_size (u8) | right_fence_key_offset (u16) | right_fence_key_size (u8) |
// | slot | slot | free space ...
// | key | value | key | value | right_fence_key | left_fence_key |
//
// TODO - possible re-arranging the ordering of the above might be more efficient.
//
// The arrays of slots is the index into the key/values of the tuples. The slot contains the offset into
// the page, the key length and the value length. The slots are sorted in ascending order - when looking
// for a key there is a binary search into the slots. When adding a key/value it is added to the free space
// from the bottom (key/values grow up) and the slots are split with some moved down to fit in the new slot.
//
// When a tuple is deleted the key/values are moved down to reclaim the space and the slots move also -
// it would be possible to leave holes and attempt to fill the holes when adding new tuples. The added
// complexity of this does not seem worth it.
//
// If updating a tuple we overwrite the key/value if the size is the same, otherwise it is a delete and add.
//
// The page uses delta compression for keys, the prefix_length field indicates the length of the common
// prefix for all keys in the page. The slot only stores the suffix of the key after the prefix. This allows
// us to save space on the keys when there are many keys with a common prefix, which is often the
// case in B+ trees where keys in the same leaf page often share a common prefix due to the way they are
// inserted and split. The get_index_for_key method performs a binary search on the key suffixes in the slots to find
// the index of a key, which allows for efficient lookups while still benefiting from the space savings of
// delta compression.
//
// If there is a left fence then all keys will be greater than or equals to the left fence, if the right
// fence is set then then all the keys are less than or equal to the it.
//
// If key is to be stored that is less than the left most page or greater than the right most key then
// then the page will need to rebuilt as compression/prefix may have changed. This should not happen often
// as the b-tree will generally only add keys to a page that lie within its range - deleting keys which triggers
// deleting pages can cause keys out of range to be added.
//
// Fences are not reset when deleting keys, so the left or right fence may not actually be a key in the page.
// If fences were reset on deleting then the compression in the page can change and even though a key has been
// deleted the page may need to split - this avoids unnecessary rebuilding of the page and splitting.
//
// If the page is on the very left of the tree, it will not have a left fence key and the left key size will be
// zero and there will be no prefix. Its quite possible to add keys that are less than the existing value. As
// there is no left fence there is no compression in this page.
//
// Similarly for the rightmost page no right fence key is stored and no compression.
//
// TODO - not clear the value of the right most fence, the left most fence is used to get the prefix
// for compression and we easily look up the largest key. The literature generally has both fences.
//
// The first leaf page, ie the initial root page, will have no fences and no prefix.
// When it first splits the page to the left will have no left fence but will have a right fence,
// and when this page splits the page to the left will have no left fence and the page to the right
// will have both fences.
// The page to the right after the root page split will have a left fence but no right fence, when it
// splits the page to the right will have no right fence but the page to the left will have both fences.
//
// The other case when the leaf page will have no fences is when it has only a couple if entries, ie
// large tuples.
//
// When a leaf page splits tail compression is used. If the largest key in the left page
// is "aeaf" and the smallest key in the page to the right is "aecd" then the directory entry
// for the for the new page to the right can be "aec". Anything less than "aec" will go to
// the page to the left.
//

impl LeafPage {
    const HEADER_SIZE: usize = 27; // 8 + 8 + 2 + 2 + 1 + 2 +1 + 2 + 1
    const SLOT_SIZE: usize = 5; // 2 (offset) + 1 (key_len) + 2 (val_len)

    pub fn create_new(page_config: &DbConfig, page_number: PageNo, version: u64) -> Self {
        LeafPage::new(
            page_config.block_size,
            page_config.page_size,
            page_number,
            version,
        )
    }

    fn new(block_size: usize, page_size: usize, page_number: PageNo, version: u64) -> Self {
        let mut page = Page::new(block_size, page_size);
        page.set_type(PageType::LeafPage);
        page.set_page_number(page_number);
        page.set_version(version);
        let mut leaf_page = LeafPage { page };
        leaf_page.set_free_space(page_size as u16 - LeafPage::HEADER_SIZE as u16);
        leaf_page.set_entries_size(0);
        leaf_page.clear_left_fence_key();
        leaf_page.clear_right_fence_key();
        leaf_page.set_prefix_length(0);
        leaf_page
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::LeafPage {
            panic!("Page type is not Leaf");
        }
        LeafPage { page }
    }

    fn reset(&mut self, page_size: usize) {
        self.set_free_space(page_size as u16 - LeafPage::HEADER_SIZE as u16);
        self.set_entries_size(0);
        self.clear_left_fence_key();
        self.clear_right_fence_key();
        self.set_prefix_length(0);
    }

    pub fn get_no_page_entries(&self) -> u16 {
        Self::get_entries_size(&self.page)
    }

    pub fn get_entries_size(page: &Page) -> u16 {
        let bytes = &page.get_page_bytes()[16..18];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn is_empty(&self) -> bool {
        LeafPage::get_entries_size(&self.page) == 0
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

    fn get_prefix_length(page: &Page) -> u8 {
        page.get_page_bytes()[20]
    }

    fn reset_with_new_right_fence(&mut self, new_right_fence: &[u8]) -> bool {
        // Need a full copy of the left fence as we are going to nuke it in the page.
        let page_copy = self.page.get_page_bytes_mut().to_vec();
        let old_prefix_length = Self::get_prefix_length(&self.page) as usize;
        let left_fence = Self::get_left_fence_key(&self.page).to_vec();

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
        let entties = self.get_all_tuples();
        self.reset(self.get_pg_size());
        self.set_left_fence_key(left_fence.as_ref());
        self.set_right_fence_key(new_right_fence);
        self.set_prefix_length(prefix_length as u8);
        for tuple in entties {
            let (ok, _) = self.add_tuple(&tuple);
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
        // Need a full copy of the left fence as we are going to nuke it in the page.
        let page_copy = self.page.get_page_bytes_mut().to_vec();
        let old_prefix_length = Self::get_prefix_length(&self.page) as usize;
        let right_fence = Self::get_right_fence_key(&self.page).to_vec();

        let prefix_length: usize = if old_prefix_length > 0 {
            // Only set compression if it was already set.
            new_left_fence
                .iter()
                .zip(&right_fence)
                .take_while(|(a, b)| a == b)
                .count()
        } else {
            0
        };
        // Get full copy of all tuples
        let entries = self.get_all_tuples();
        self.reset(self.get_pg_size());
        self.set_left_fence_key(new_left_fence);
        self.set_right_fence_key(right_fence.as_ref());
        self.set_prefix_length(prefix_length as u8);
        for tuple in entries {
            let (ok, _) = self.add_tuple(&tuple);
            if !ok {
                // Cannot rebuild page with new compression, page not big enough.
                // Reset page back back to original bits and trigger a split.
                self.page.get_page_bytes_mut().copy_from_slice(&page_copy);
                return false;
            }
        }
        true
    }

    fn set_left_fence_key(&mut self, key: &[u8]) {
        assert!(
            Self::get_entries_size(&self.page) == 0,
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

    fn has_left_fence(page: &Page) -> bool {
        page.get_page_bytes()[23] != 0
    }

    fn get_left_fence_key_size(page: &Page) -> u8 {
        page.get_page_bytes()[23]
    }

    fn get_left_fence_key_offset(page: &Page) -> u16 {
        let bytes = &page.get_page_bytes()[21..23];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    fn clear_left_fence_key(&mut self) {
        self.page.get_page_bytes_mut()[21..23].copy_from_slice(&[0, 0]);
        self.page.get_page_bytes_mut()[23] = 0;
    }

    fn get_left_fence_key(page: &Page) -> &[u8] {
        let offset = Self::get_left_fence_key_offset(page) as usize;
        let size = Self::get_left_fence_key_size(page) as usize;
        &page.get_page_bytes()[offset..offset + size]
    }

    // Get the left most key in the page. Note we do not use the
    // left fence, when tuples are deleted the fences are not
    // reset so we need to reconstruct the the left most key.
    pub fn get_left_key(&self) -> Option<Vec<u8>> {
        if Self::get_entries_size(&self.page) == 0 {
            return None;
        }
        Some(self.get_key_at_index(0))
    }

    fn set_right_fence_key(&mut self, key: &[u8]) {
        assert!(
            Self::get_entries_size(&self.page) == 0,
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

    fn has_right_fence(page: &Page) -> bool {
        page.get_page_bytes()[26] != 0
    }

    fn get_right_fence_key_offset(page: &Page) -> u16 {
        let bytes = &page.get_page_bytes()[24..26];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    fn clear_right_fence_key(&mut self) {
        self.page.get_page_bytes_mut()[24..26].copy_from_slice(&[0, 0]);
        self.page.get_page_bytes_mut()[26] = 0;
    }

    fn get_right_fence_key_size(page: &Page) -> u8 {
        page.get_page_bytes()[26]
    }

    fn get_right_fence_key(page: &Page) -> &[u8] {
        let offset = Self::get_right_fence_key_offset(page) as usize;
        let size = Self::get_right_fence_key_size(page) as usize;
        &page.get_page_bytes()[offset..offset + size]
    }

    fn set_prefix_length(&mut self, prefix_length: u8) {
        assert!(
            Self::get_entries_size(&self.page) == 0,
            "Cannot set prefix length on a page that already has entries."
        );
        assert!(
            prefix_length <= Self::get_right_fence_key_size(&self.page),
            "Prefix length cannot be larger than the right fence key size."
        );
        self.page.get_page_bytes_mut()[20] = prefix_length;
    }

    fn get_slot_at_index(page: &Page, index: usize) -> LeafSlot {
        assert!(index < Self::get_entries_size(page) as usize);
        let slot_offset = LeafPage::HEADER_SIZE + index * LeafPage::SLOT_SIZE;
        let offset_bytes = &page.get_page_bytes()[slot_offset..slot_offset + 2];
        let offset = u16::from_le_bytes(offset_bytes.try_into().unwrap());
        let key_len = page.get_page_bytes()[slot_offset + 2];
        let val_len_bytes = &page.get_page_bytes()[slot_offset + 3..slot_offset + 5];
        let val_len = u16::from_le_bytes(val_len_bytes.try_into().unwrap());
        LeafSlot {
            offset,
            key_len,
            val_len,
        }
    }

    fn set_slot_at_index(&mut self, index: usize, slot: LeafSlot) {
        let slot_offset = LeafPage::HEADER_SIZE + index * LeafPage::SLOT_SIZE;
        let offset_bytes = slot.offset.to_le_bytes();
        self.page.get_page_bytes_mut()[slot_offset..slot_offset + 2].copy_from_slice(&offset_bytes);
        self.page.get_page_bytes_mut()[slot_offset + 2] = slot.key_len;
        let val_len_bytes = slot.val_len.to_le_bytes();
        self.page.get_page_bytes_mut()[slot_offset + 3..slot_offset + 5]
            .copy_from_slice(&val_len_bytes);
    }

    fn get_value_at_slot<'a>(page: &'a Page, slot: &LeafSlot) -> &'a [u8] {
        let val_offset = (slot.offset + slot.key_len as u16) as usize;
        &page.get_page_bytes()[val_offset..val_offset + slot.val_len as usize]
    }

    fn get_key_at_slot<'a>(page: &'a Page, slot: &LeafSlot) -> &'a [u8] {
        let key_offset = slot.offset as usize;
        &page.get_page_bytes()[key_offset..key_offset + slot.key_len as usize]
    }

    fn get_key_prefix(page: &Page) -> &[u8] {
        let prefix_length = Self::get_prefix_length(page) as usize;
        if prefix_length == 0 {
            return &[];
        }
        &Self::get_left_fence_key(page)[0..prefix_length]
    }

    fn get_index_for_key(page: &Page, key_suffix: &[u8]) -> (bool, usize) {
        let entries = Self::get_entries_size(page) as usize;

        // binary search for the key suffix in the slots
        let mut low = 0;
        let mut high = entries;

        while low < high {
            let mid = low + (high - low) / 2;
            let slot = Self::get_slot_at_index(page, mid);
            let key_at_slot = Self::get_key_at_slot(page, &slot);

            match key_suffix.cmp(key_at_slot) {
                Ordering::Less => high = mid, // Needle is smaller, look in the left half
                Ordering::Equal => return (true, mid),
                Ordering::Greater => low = mid + 1, // Needle is larger, look in the right half
            }
        }
        // low is the insertion point if the key wasn't found
        (false, low)
    }

    fn shift_slots_right_from(&mut self, from_index: usize) {
        let entries = Self::get_entries_size(&self.page) as usize;
        if entries == from_index {
            return;
        }
        self.page.get_page_bytes_mut().copy_within(
            LeafPage::HEADER_SIZE + from_index * LeafPage::SLOT_SIZE
                ..LeafPage::HEADER_SIZE + entries * LeafPage::SLOT_SIZE,
            LeafPage::HEADER_SIZE + (from_index + 1) * LeafPage::SLOT_SIZE,
        );
    }

    fn shift_slots_left_from(&mut self, from_index: usize) {
        let entries = Self::get_entries_size(&self.page) as usize;
        self.page.get_page_bytes_mut().copy_within(
            LeafPage::HEADER_SIZE + (from_index + 1) * LeafPage::SLOT_SIZE
                ..LeafPage::HEADER_SIZE + entries * LeafPage::SLOT_SIZE,
            LeafPage::HEADER_SIZE + from_index * LeafPage::SLOT_SIZE,
        );
    }

    // Add a tuple and return any tuple that gets overwritten.
    // If cannot add the tuple to the page because it cannot fit then return false.
    // However it will return the replaced tuple even if the new tuple cannot fit
    // and the tuple will have been removed - the expectation is that the page
    // will be split to fit the tuple.
    //
    pub fn add_tuple(&mut self, tuple: &Tuple) -> (bool, Option<Tuple>) {
        let tuple_key = tuple.get_key();
        let prefix_length = Self::get_prefix_length(&self.page) as usize;
        // If using compression and a key comes in larger than the right fence reset.
        // The page to the right of this page could have been deleted, then a key that
        // originally belonged to that page is added again and is now routed to this page
        // so need to account for this.
        // Always rebuild if we have a right fence and key is larger than right fence.
        if Self::has_right_fence(&self.page) && tuple_key > Self::get_right_fence_key(&self.page) {
            if !self.reset_with_new_right_fence(tuple_key) {
                // Reset failed as cannot rebuild same page with new compression as not enough space.
                // Trigger a split first. Note as the key is bigger than the right fence we know we
                // do not have it in this page so fine to return None in tuple.
                return (false, None);
            }
            // recursively call add_tuple on reset page.
            return self.add_tuple(tuple);
        }

        // This is needed as we are using tail compression in the dir pages.
        // The dir page holds a truncated version of the left most key only,
        // to it can send tuples here that are less than the left most key.
        if Self::has_left_fence(&self.page) && tuple_key < Self::get_left_fence_key(&self.page) {
            if !self.reset_with_new_left_fence(tuple_key) {
                // Reset failed as cannot rebuild same page with new compression as not enough space.
                // Trigger a split first. Note as the key is bigger than the right fence we know we
                // do not have it in this page so fine to return None in tuple.
                return (false, None);
            }
            // recursively call add_tuple on reset page.
            return self.add_tuple(tuple);
        }

        if prefix_length > 0 {
            assert!(
                tuple_key.len() >= prefix_length,
                "BUG: Tuple key length is smaller than the prefix length of the page."
            );
            assert!(
                tuple_key.starts_with(Self::get_key_prefix(&self.page)),
                "BUG: Tuple key does not start with the prefix of the page."
            );
        }
        let key_suffix = &tuple_key[prefix_length..];
        let (found, index) = Self::get_index_for_key(&self.page, key_suffix);

        let mut existing_tuple: Option<Tuple> = None;
        if found {
            let slot = Self::get_slot_at_index(&self.page, index);
            existing_tuple = Some(Self::get_tuple_at_index(&self.page, index));
            if slot.val_len == tuple.get_version_value().len() as u16 {
                // If the new value has the same length as the old value, we can just overwrite the value in place
                // without needing to shift entries around.
                // TODO - if size is less we could also just overwrite and leave some unused space.
                let val_offset = (slot.offset + slot.key_len as u16) as usize;
                self.page.get_page_bytes_mut()[val_offset..val_offset + slot.val_len as usize]
                    .copy_from_slice(tuple.get_version_value());
                return (true, existing_tuple);
            } else {
                // If the new value has a different length than the old value, we need to remove the old entry
                // and add a new entry for the key with the new value.
                self.remove_key_value_at_index(index);
            }
        }

        let key_suffix_len = tuple_key.len() - prefix_length;
        let new_entry_size = key_suffix_len + tuple.get_version_value().len();
        let new_entry_total_size = new_entry_size + LeafPage::SLOT_SIZE;
        let free_space = self.get_free_space() as usize;

        if new_entry_total_size > free_space {
            return (false, existing_tuple);
        }

        self.add_key_value_at_index(index, key_suffix, tuple.get_version_value());
        (true, existing_tuple)
    }

    fn calculate_entries_offset(&self) -> usize {
        let free_space = self.get_free_space() as usize;
        let entries = Self::get_entries_size(&self.page) as usize;
        let header_plus_slots_size = LeafPage::HEADER_SIZE + entries * LeafPage::SLOT_SIZE;
        header_plus_slots_size + free_space
    }

    fn add_key_value_at_index(&mut self, index: usize, key: &[u8], value: &[u8]) {
        // Sanity check
        let new_entry_size = key.len() + value.len();
        let new_entry_total_size = new_entry_size + LeafPage::SLOT_SIZE;
        let free_space = self.get_free_space() as usize;
        assert!(new_entry_total_size <= free_space);

        // Find offset where the key/value entry can be added.
        let entries = Self::get_entries_size(&self.page) as usize;
        let old_entries_offset = self.calculate_entries_offset();
        let new_entry_offset = old_entries_offset - new_entry_size;

        // Add key/value at the offset
        self.page.get_page_bytes_mut()[new_entry_offset..new_entry_offset + key.len()]
            .copy_from_slice(key);
        self.page.get_page_bytes_mut()
            [new_entry_offset + key.len()..new_entry_offset + key.len() + value.len()]
            .copy_from_slice(value);

        // Create a slot and add it.
        let slot = LeafSlot {
            offset: new_entry_offset as u16,
            key_len: key.len() as u8,
            val_len: value.len() as u16,
        };
        self.shift_slots_right_from(index);
        self.set_slot_at_index(index, slot);

        // Update entries and free space.
        self.set_entries_size((entries + 1) as u16);
        self.set_free_space(free_space as u16 - new_entry_total_size as u16);
    }

    pub fn get_tuple_from_page(page: &Page, key: &[u8]) -> Option<Tuple> {
        if Self::has_right_fence(page) && key > Self::get_right_fence_key(page) {
            return None;
        }
        if Self::has_left_fence(page) && key < Self::get_left_fence_key(page) {
            return None;
        }

        let prefix_length = Self::get_prefix_length(page) as usize;
        let (found, index) = Self::get_index_for_key(page, &key[prefix_length..]);
        if !found {
            return None;
        }
        Some(Self::get_tuple_at_index(page, index))
    }

    pub fn get_tuple(&self, key: &[u8]) -> Option<Tuple> {
        Self::get_tuple_from_page(&self.page, key)
    }

    fn get_tuple_at_index(page: &Page, index: usize) -> Tuple {
        let slot = Self::get_slot_at_index(page, index);
        let key_prefix = Self::get_key_prefix(page);
        let key = Self::get_key_at_slot(page, &slot);
        let value = Self::get_value_at_slot(page, &slot);
        let mut full_key = Vec::with_capacity(key_prefix.len() + key.len());
        full_key.extend_from_slice(key_prefix);
        full_key.extend_from_slice(key);
        let version_holder = VersionHolder::from_bytes(&value[0..8]);
        Tuple::new_with_overflow(
            &full_key,
            &value[8..],
            version_holder.get_version(),
            Overflow::try_from(version_holder.get_flags()).unwrap(),
        )
    }

    fn get_key_suffix_and_value_at_index(&self, index: usize) -> (&[u8], &[u8]) {
        let slot = Self::get_slot_at_index(&self.page, index);
        let key = Self::get_key_at_slot(&self.page, &slot);
        let value = Self::get_value_at_slot(&self.page, &slot);
        (key, value)
    }

    fn get_key_suffix_at_index(&self, index: usize) -> &[u8] {
        let slot = Self::get_slot_at_index(&self.page, index);
        Self::get_key_at_slot(&self.page, &slot)
    }

    fn get_key_at_index(&self, index: usize) -> Vec<u8> {
        let slot: LeafSlot = Self::get_slot_at_index(&self.page, index);
        let key_suffix = Self::get_key_at_slot(&self.page, &slot);
        let key_prefix = Self::get_key_prefix(&self.page);
        let mut full_key = Vec::with_capacity(key_prefix.len() + key_suffix.len());
        full_key.extend_from_slice(key_prefix);
        full_key.extend_from_slice(key_suffix);
        full_key
    }

    pub fn get_all_tuples(&self) -> Vec<Tuple> {
        let entries = Self::get_entries_size(&self.page);
        let mut tuples = Vec::new();
        for i in 0..entries {
            let tuple = Self::get_tuple_at_index(&self.page, i as usize);
            tuples.push(tuple);
        }
        tuples
    }

    fn split_page_1(&self, version: u64) -> (LeafPage, LeafPage, Option<Vec<u8>>) {
        // First page - no left or right pages. This means no
        // prefix, no right fence key and no left fence key.
        // When split the page on the left will have not have a left fence but will
        // have a right fence. The new page on the right will have a left fence
        // but no right fence. Both pages will have no prefix.
        assert!(
            Self::get_key_prefix(&self.page).is_empty(),
            "BUG: Page has a prefix when splitting page with no fences."
        );
        let mut left_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        let entries = Self::get_entries_size(&self.page) as usize;
        let mid = entries / 2;

        // No prefix so we can just copy the key suffixes as they are.
        let mid_key = self.get_key_suffix_at_index(mid);
        let left_page_right_fence_key = self.get_key_suffix_at_index(mid - 1);
        left_page.set_right_fence_key(left_page_right_fence_key);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, key, value);
        }

        let split_key = LeafPage::tail_compress_key(left_page_right_fence_key, mid_key);

        right_page.set_left_fence_key(mid_key);
        for i in mid..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(i - mid, key, value);
        }

        (left_page, right_page, Some(split_key))
    }

    fn split_page_2(&self, version: u64) -> (LeafPage, LeafPage, Option<Vec<u8>>) {
        // Left Page - has right fence but no left fence. This means no prefix
        // and a right fence key.
        assert!(
            Self::get_key_prefix(&self.page).is_empty(),
            "BUG: Page has a prefix when splitting page with only a right fence."
        );
        let mut left_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        let entries = Self::get_entries_size(&self.page) as usize;
        let mid = entries / 2;

        // No prefix so we can just copy the key suffixes as they are.
        let mid_key = self.get_key_suffix_at_index(mid);
        let left_page_right_fence_key = self.get_key_suffix_at_index(mid - 1);
        left_page.set_right_fence_key(left_page_right_fence_key);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, key, value);
        }

        let split_key = LeafPage::tail_compress_key(left_page_right_fence_key, mid_key);

        let right_fence_key = Self::get_right_fence_key(&self.page);
        right_page.set_right_fence_key(right_fence_key);
        right_page.set_left_fence_key(mid_key);
        let right_prefix_length = mid_key
            .iter()
            .zip(right_fence_key)
            .take_while(|(a, b)| a == b)
            .count();
        right_page.set_prefix_length(right_prefix_length as u8);
        for i in mid..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(i - mid, &key[right_prefix_length..], value);
        }

        (left_page, right_page, Some(split_key))
    }

    fn split_page_3(&self, version: u64) -> (LeafPage, LeafPage, Option<Vec<u8>>) {
        // Right Page - has left fence but no right fence. This means no prefix
        // and no right fence key.
        let mut left_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        let entries = Self::get_entries_size(&self.page) as usize;
        let mid = entries / 2;

        // Create page to the left.
        // No prefix so we can just copy the key suffixes as they are.
        let mid_key = self.get_key_suffix_at_index(mid);
        let left_page_right_fence_key = self.get_key_suffix_at_index(mid - 1);
        let left_page_left_fence_key = Self::get_left_fence_key(&self.page);
        left_page.set_left_fence_key(left_page_left_fence_key);
        left_page.set_right_fence_key(left_page_right_fence_key);
        let left_prefix_length = left_page_left_fence_key
            .iter()
            .zip(left_page_right_fence_key)
            .take_while(|(a, b)| a == b)
            .count();
        left_page.set_prefix_length(left_prefix_length as u8);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, &key[left_prefix_length..], value);
        }

        let split_key = LeafPage::tail_compress_key(left_page_right_fence_key, mid_key);

        // Create page to the right.
        right_page.set_left_fence_key(mid_key);
        right_page.set_prefix_length(0);
        for i in mid..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(i - mid, key, value);
        }

        (left_page, right_page, Some(split_key))
    }

    fn split_page_4(&self, version: u64) -> (LeafPage, LeafPage, Option<Vec<u8>>) {
        // Center Page - has right and left fence and also a Prefix.
        // This means we need to calculate the new prefix length for the left and right pages after the split.
        let mut left_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        let entries = Self::get_entries_size(&self.page) as usize;
        let mid = entries / 2;

        let mid_key = self.get_key_at_index(mid);
        let last_key = self.get_key_at_index(mid - 1);
        let left_page_left_fence_key = Self::get_left_fence_key(&self.page);
        left_page.set_left_fence_key(left_page_left_fence_key);
        left_page.set_right_fence_key(last_key.as_slice());
        let left_prefix_length = left_page_left_fence_key
            .iter()
            .zip(last_key.as_slice())
            .take_while(|(a, b)| a == b)
            .count();
        let left_suffix_offset = left_prefix_length - Self::get_prefix_length(&self.page) as usize;
        left_page.set_prefix_length(left_prefix_length as u8);
        for i in 0..mid {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            // This should avoid moving bytes around - we will be appending slots.
            left_page.add_key_value_at_index(i, &key[left_suffix_offset..], value);
        }

        let split_key = LeafPage::tail_compress_key(&last_key, &mid_key);

        right_page.set_left_fence_key(&mid_key);
        let right_page_right_fence_key = Self::get_right_fence_key(&self.page);
        right_page.set_right_fence_key(right_page_right_fence_key);
        let right_prefix_length = mid_key
            .iter()
            .zip(right_page_right_fence_key)
            .take_while(|(a, b)| a == b)
            .count();
        right_page.set_prefix_length(right_prefix_length as u8);
        let right_suffix_offset =
            right_prefix_length - Self::get_prefix_length(&self.page) as usize;
        for i in mid..entries {
            let (key, value) = self.get_key_suffix_and_value_at_index(i);
            right_page.add_key_value_at_index(i - mid, &key[right_suffix_offset..], value);
        }

        (left_page, right_page, Some(split_key))
    }

    fn split_page_low_entry_count(&self, version: u64) -> (LeafPage, LeafPage, Option<Vec<u8>>) {
        let mut left_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            self.page.get_page_number(),
            version,
        );
        let mut right_page = LeafPage::new(
            self.page.block_size,
            self.get_pg_size(),
            PageNo::from_u64(0),
            version,
        );

        // No fences.
        left_page.set_left_fence_key(&[]);
        right_page.set_left_fence_key(&[]);
        left_page.set_right_fence_key(&[]);
        right_page.set_right_fence_key(&[]);
        left_page.set_prefix_length(0);
        right_page.set_prefix_length(0);

        let mut left_key = None;
        let first_tuple = Self::get_tuple_at_index(&self.page, 0);
        left_page.add_tuple(&first_tuple);
        if Self::get_entries_size(&self.page) == 2 {
            let second_tuple = Self::get_tuple_at_index(&self.page, 1);
            right_page.add_tuple(&second_tuple);
            left_key = Some(second_tuple.get_key().to_vec());
        }
        // No tail compression on left key.
        (left_page, right_page, left_key)
    }

    pub fn split_page(&self, version: u64) -> (LeafPage, LeafPage, Option<Vec<u8>>) {
        let entries = Self::get_entries_size(&self.page);
        assert!(entries > 0, "Page must have at least one entry to split.");

        // Handle low entry count tables separately
        if entries <= 2 {
            return self.split_page_low_entry_count(version);
        }

        // First page - no left or right pages.
        if !Self::has_left_fence(&self.page) && !Self::has_right_fence(&self.page) {
            // There will be no prefix. When the page is split the
            // there will be a Left Page and a Right neither will
            // have a prefix.
            return self.split_page_1(version);
        }

        // Left Page - has right fence but no left fence.
        if !Self::has_left_fence(&self.page) {
            return self.split_page_2(version);
        }

        // Right Page - has left fence but no right fence.
        if !Self::has_right_fence(&self.page) {
            return self.split_page_3(version);
        }

        // Center Page - has both left and right fences.
        self.split_page_4(version)
    }

    pub fn tail_compress_key(last_key: &[u8], mid_key: &[u8]) -> Vec<u8> {
        let mut tail_offset = last_key
            .iter()
            .zip(mid_key)
            .take_while(|(a, b)| a == b)
            .count();
        tail_offset += 1;
        assert!(tail_offset <= mid_key.len(), "Tail compression failure");
        mid_key[..tail_offset].to_vec()
    }

    /**
     * Remove key and value. Returns true of the key was found and removed,
     * false if the key was not found.
     *
     * This does not reset fences.
     *
     */
    pub fn delete_key(&mut self, key: &[u8]) -> Option<Tuple> {
        if Self::has_right_fence(&self.page) && key > Self::get_right_fence_key(&self.page) {
            return None;
        }
        if Self::has_left_fence(&self.page) && key < Self::get_left_fence_key(&self.page) {
            return None;
        }

        let prefix_length = Self::get_prefix_length(&self.page) as usize;
        let (found, index) = Self::get_index_for_key(&self.page, &key[prefix_length..]);
        if !found {
            return None;
        }
        let tuple = Self::get_tuple_at_index(&self.page, index);
        self.remove_key_value_at_index(index);
        Some(tuple)
    }

    /**
     * The approach removes the bytes from the page and shovels the entries
     * around to fill the gap. An alternative approach is to leave the
     * hole in the entries and attempt to fill it in when adding new entries.
     */
    fn remove_key_value_at_index(&mut self, index: usize) {
        let entries = Self::get_entries_size(&self.page) as usize;
        assert!(index < entries);
        let slot = Self::get_slot_at_index(&self.page, index);
        let entry_size = slot.key_len as usize + slot.val_len as usize;

        let free_space = self.get_free_space() as usize;
        let header_plus_slots_size = LeafPage::HEADER_SIZE + entries * LeafPage::SLOT_SIZE;
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
            // If the entry to remove is the last entry, we can just update the free space and entries without shifting.
            self.set_free_space((free_space + entry_size + LeafPage::SLOT_SIZE) as u16);
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
        let mut slot_offset = LeafPage::HEADER_SIZE;
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
            slot_offset += LeafPage::SLOT_SIZE;
        }

        // Update entries and free space.
        self.set_entries_size(new_entry_count as u16);
        self.set_free_space((free_space + entry_size + LeafPage::SLOT_SIZE) as u16);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::vec;

    #[test]
    fn test_tail_compression1() {
        let last_key = "aacf";
        let mid_key = "abcd";
        let tail = LeafPage::tail_compress_key(last_key.as_bytes(), mid_key.as_bytes());
        assert_eq!(tail, "ab".as_bytes());
    }

    #[test]
    fn test_tail_compression2() {
        let last_key = "aeaf";
        let mid_key = "aecd";
        let tail = LeafPage::tail_compress_key(last_key.as_bytes(), mid_key.as_bytes());
        assert_eq!(tail, "aec".as_bytes());
    }

    #[test]
    #[should_panic(expected = "Page type is not Leaf")]
    fn test_not_leaf_page() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut dir_page = Page::new(page_config.block_size, page_config.page_size);
        dir_page.set_type(PageType::DirPage);
        let _leaf_page = LeafPage::from_page(dir_page);
    }

    #[test]
    fn test_split() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 23);
        assert_eq!(leaf_page.get_page_bytes().len(), 4000);
        assert_eq!(leaf_page.get_version(), 23);
        assert!(!LeafPage::has_left_fence(leaf_page.get_page()));
        assert!(!LeafPage::has_right_fence(leaf_page.get_page()));
        assert_eq!(LeafPage::get_prefix_length(leaf_page.get_page()), 0);
        let mut tuples = vec![];
        for i in 0..20 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            let tuple = Tuple::new(&key, &value, i as u64);
            assert!(leaf_page.add_tuple(&tuple).0);
            tuples.push(tuple);
        }
        // The tuples are added in order of increasing key, but the order they are stored in the page is not guaranteed to be the
        // same as the order they were added, so we need to sort them by key before we can verify that they are split correctly.
        // This is beacuse the tuple key is created as a string - the node treats it as a byte array so 11 comes before 2, so
        // we sort the tuples to match the order the page will store them in.
        tuples.sort_by_key(|t| t.get_key().to_vec());
        assert!(!LeafPage::has_right_fence(leaf_page.get_page()));
        assert!(!LeafPage::has_left_fence(leaf_page.get_page()));

        let (mut left_page, mut right_page, _) = leaf_page.split_page(0);
        assert_eq!(LeafPage::get_entries_size(right_page.get_page()), 10);
        assert_eq!(LeafPage::get_entries_size(left_page.get_page()), 10);
        assert!(LeafPage::has_right_fence(left_page.get_page()));
        assert!(!LeafPage::has_left_fence(left_page.get_page()));
        assert!(LeafPage::has_left_fence(right_page.get_page()));
        assert!(!LeafPage::has_right_fence(right_page.get_page()));
        for i in 0..10 {
            assert!(
                left_page
                    .get_tuple(tuples.get(i).unwrap().get_key())
                    .unwrap()
                    .equals(tuples.get(i).unwrap())
            );
        }
        for i in 10..20 {
            assert!(
                right_page
                    .get_tuple(tuples.get(i).unwrap().get_key())
                    .unwrap()
                    .equals(tuples.get(i).unwrap())
            );
        }

        let (mut left_page1, mut left_page2, _) = left_page.split_page(0);
        assert_eq!(LeafPage::get_entries_size(left_page1.get_page()), 5);
        assert_eq!(LeafPage::get_entries_size(left_page2.get_page()), 5);
        for i in 0..5 {
            assert!(
                left_page1
                    .get_tuple(tuples.get(i).unwrap().get_key())
                    .unwrap()
                    .equals(tuples.get(i).unwrap())
            );
        }
        for i in 5..10 {
            assert!(
                left_page2
                    .get_tuple(tuples.get(i).unwrap().get_key())
                    .unwrap()
                    .equals(tuples.get(i).unwrap())
            );
        }
        let (mut right_page1, mut right_page2, _) = right_page.split_page(0);
        assert_eq!(LeafPage::get_entries_size(right_page1.get_page()), 5);
        assert_eq!(LeafPage::get_entries_size(right_page2.get_page()), 5);
        for i in 10..15 {
            assert!(
                right_page1
                    .get_tuple(tuples.get(i).unwrap().get_key())
                    .unwrap()
                    .equals(tuples.get(i).unwrap())
            );
        }
        for i in 15..20 {
            assert!(
                right_page2
                    .get_tuple(tuples.get(i).unwrap().get_key())
                    .unwrap()
                    .equals(tuples.get(i).unwrap())
            );
        }

        // left_page1
        assert!(LeafPage::has_right_fence(left_page1.get_page()));
        assert!(!LeafPage::has_left_fence(left_page1.get_page()));
        assert_eq!(LeafPage::get_prefix_length(left_page1.get_page()), 0);

        // left_page2
        assert!(LeafPage::has_left_fence(left_page2.get_page()));
        assert!(LeafPage::has_right_fence(left_page2.get_page()));
        assert!(LeafPage::get_prefix_length(left_page2.get_page()) > 0);

        // right_page1
        assert!(LeafPage::has_right_fence(right_page1.get_page()));
        assert!(LeafPage::has_left_fence(right_page1.get_page()));
        assert!(LeafPage::get_prefix_length(right_page1.get_page()) > 0);

        // right_page2
        assert!(LeafPage::has_left_fence(right_page2.get_page()));
        assert!(!LeafPage::has_right_fence(right_page2.get_page()));
        assert_eq!(LeafPage::get_prefix_length(right_page2.get_page()), 0);
    }

    #[test]
    #[should_panic(expected = "Cannot set left fence key on a page that already has entries.")]
    fn test_set_left_fence_empty_page() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 0);
        assert_eq!(leaf_page.get_left_key(), None);
        let tuple_1 = Tuple::new(b"a", b"a_value", 123);
        assert!(leaf_page.add_tuple(&tuple_1).0);
        leaf_page.set_left_fence_key(b"left_fence");
    }

    #[test]
    #[should_panic(expected = "Cannot set right fence key on a page that already has entries.")]
    fn test_set_right_fence_empty_page() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 0);
        assert_eq!(leaf_page.get_left_key(), None);
        let tuple_1 = Tuple::new(b"a", b"a_value", 123);
        assert!(leaf_page.add_tuple(&tuple_1).0);
        leaf_page.set_right_fence_key(b"right_fence");
    }

    #[test]
    #[should_panic(expected = "Cannot set prefix length on a page that already has entries.")]
    fn test_set_prefix_empty_page() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let tuple_1 = Tuple::new(b"a", b"a_value", 123);
        assert!(leaf_page.add_tuple(&tuple_1).0);
        leaf_page.set_prefix_length(5);
    }

    #[test]
    #[should_panic(expected = "Prefix length cannot be larger than the right fence key size.")]
    fn test_set_prefix_bad_fence() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        leaf_page.set_right_fence_key(b"left_fence");
        leaf_page.set_prefix_length(15);
    }

    #[test]
    fn test_page_reset_left_fence_overflow() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 129,
            block_sanity_size: 4096 - 129,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let left_fence_key = b"aaaaaaaaaaaaaaa";
        let right_fence_key = b"aaaaaaaaaaaaaaz";
        leaf_page.set_left_fence_key(left_fence_key);
        leaf_page.set_right_fence_key(right_fence_key);
        let tuple_1 = Tuple::new(left_fence_key, 0u64.to_le_bytes().as_slice(), 123);
        let tuple_2 = Tuple::new(right_fence_key, 0u64.to_le_bytes().as_slice(), 123);

        assert!(leaf_page.add_tuple(&tuple_1).0);
        assert!(leaf_page.add_tuple(&tuple_2).0);
        assert_eq!(leaf_page.get_free_space(), 0);
        assert!(!leaf_page.reset_with_new_left_fence(b"aaaaaaaaaaaaaa00"));
        assert!(leaf_page.get_tuple(tuple_1.get_key()).is_some());
        assert!(leaf_page.get_tuple(tuple_2.get_key()).is_some());
    }

    #[test]
    fn test_add_page_reset_left_fence_overflow() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 129,
            block_sanity_size: 4096 - 129,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let left_fence_key = b"aaaaaaaaaaaaaaa";
        let right_fence_key = b"aaaaaaaaaaaaaaz";
        leaf_page.set_left_fence_key(left_fence_key);
        leaf_page.set_right_fence_key(right_fence_key);
        let tuple_1 = Tuple::new(left_fence_key, 0u64.to_le_bytes().as_slice(), 123);
        let tuple_2 = Tuple::new(right_fence_key, 0u64.to_le_bytes().as_slice(), 123);

        assert!(leaf_page.add_tuple(&tuple_1).0);
        assert!(leaf_page.add_tuple(&tuple_2).0);
        assert_eq!(leaf_page.get_free_space(), 0);
        let tuple_3 = Tuple::new(b"aaaaaaaaaaaaaa00", 0u64.to_le_bytes().as_slice(), 123);
        assert!(!leaf_page.add_tuple(&tuple_3).0);
        assert!(leaf_page.get_tuple(tuple_1.get_key()).is_some());
        assert!(leaf_page.get_tuple(tuple_2.get_key()).is_some());
    }

    #[test]
    fn test_page_reset_right_fence_overflow() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 129,
            block_sanity_size: 4096 - 129,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let left_fence_key = b"aaaaaaaaaaaaaaa";
        let right_fence_key = b"aaaaaaaaaaaaaay";
        leaf_page.set_left_fence_key(left_fence_key);
        leaf_page.set_right_fence_key(right_fence_key);
        let tuple_1 = Tuple::new(left_fence_key, 0u64.to_le_bytes().as_slice(), 123);
        let tuple_2 = Tuple::new(right_fence_key, 0u64.to_le_bytes().as_slice(), 123);

        assert!(leaf_page.add_tuple(&tuple_1).0);
        assert!(leaf_page.add_tuple(&tuple_2).0);
        assert_eq!(leaf_page.get_free_space(), 0);
        assert!(!leaf_page.reset_with_new_right_fence(b"aaaaaaaaaaaaaazz"));
        assert!(leaf_page.get_tuple(tuple_1.get_key()).is_some());
        assert!(leaf_page.get_tuple(tuple_2.get_key()).is_some());
    }

    #[test]
    fn test_delete_out_of_range() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let left_fence_key = b"aaaaaaaaaaaaaaa";
        let right_fence_key = b"aaaaaaaaaaaaaay";
        leaf_page.set_left_fence_key(left_fence_key);
        leaf_page.set_right_fence_key(right_fence_key);
        let tuple_1 = Tuple::new(left_fence_key, 0u64.to_le_bytes().as_slice(), 123);
        let tuple_2 = Tuple::new(right_fence_key, 0u64.to_le_bytes().as_slice(), 123);
        let tuple_3 = Tuple::new(b"aaaaaaaaaaaaaab", 0u64.to_le_bytes().as_slice(), 123);

        assert!(leaf_page.add_tuple(&tuple_1).0);
        assert!(leaf_page.add_tuple(&tuple_2).0);
        assert!(leaf_page.add_tuple(&tuple_3).0);
        assert!(leaf_page.delete_key(b"aaaaaaaaaaaaaa0").is_none());
        assert!(leaf_page.delete_key(b"aaaaaaaaaaaaaaz").is_none());
    }

    #[test]
    fn test_multi_length_keys() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let tuple_1 = Tuple::new(b"a", b"a_value", 123);
        let tuple_2 = Tuple::new(b"aa", b"aa_value", 123);
        let tuple_3 = Tuple::new(b"aaa", b"aaa_value", 123);
        let tuple_4 = Tuple::new(b"ab", b"ab_value", 123);
        assert!(leaf_page.add_tuple(&tuple_2).0);
        assert!(leaf_page.add_tuple(&tuple_1).0);
        assert!(leaf_page.add_tuple(&tuple_3).0);
        assert!(leaf_page.add_tuple(&tuple_4).0);
        let tuples = leaf_page.get_all_tuples();
        assert_eq!(tuples.len(), 4);
        assert_eq!(tuples[0].get_key(), b"a");
        assert_eq!(tuples[1].get_key(), b"aa");
        assert_eq!(tuples[2].get_key(), b"aaa");
        assert_eq!(tuples[3].get_key(), b"ab");
    }

    #[test]
    fn test_add_and_remove_tuple() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let tuple_a = Tuple::new(b"a", b"a_value", 123);
        let tuple_b = Tuple::new(b"b", b"b_value", 123);
        let tuple_c = Tuple::new(b"c", b"c_value", 123);

        assert!(leaf_page.get_tuple(tuple_a.get_key()).is_none());
        assert!(leaf_page.add_tuple(&tuple_a).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 1);
        assert!(
            leaf_page
                .get_tuple(tuple_a.get_key())
                .unwrap()
                .equals(&tuple_a)
        );
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());

        assert!(leaf_page.add_tuple(&tuple_c).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 2);
        assert!(
            leaf_page
                .get_tuple(tuple_a.get_key())
                .unwrap()
                .equals(&tuple_a)
        );
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c)
        );

        assert!(leaf_page.add_tuple(&tuple_b).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 3);
        assert!(
            leaf_page
                .get_tuple(tuple_a.get_key())
                .unwrap()
                .equals(&tuple_a)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_b.get_key())
                .unwrap()
                .equals(&tuple_b)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c)
        );

        assert!(leaf_page.delete_key(tuple_b.get_key()).is_some());
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 2);
        assert!(
            leaf_page
                .get_tuple(tuple_a.get_key())
                .unwrap()
                .equals(&tuple_a)
        );
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c)
        );

        assert!(leaf_page.delete_key(tuple_c.get_key()).is_some());
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 1);
        assert!(
            leaf_page
                .get_tuple(tuple_a.get_key())
                .unwrap()
                .equals(&tuple_a)
        );
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());

        assert!(leaf_page.delete_key(tuple_a.get_key()).is_some());
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 0);
        assert!(leaf_page.get_tuple(tuple_a.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());
    }

    #[test]
    fn test_overwrite_tuple() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4000,
            block_sanity_size: 96,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let tuple_a = Tuple::new(b"a", b"a_value", 123);
        let tuple_b = Tuple::new(b"b", b"b_value", 123);
        let tuple_c = Tuple::new(b"c", b"c_value", 123);
        let tuple_c_same_value_size = Tuple::new(b"c", b"c_valu1", 123);
        let tuple_c_updated = Tuple::new(b"c", b"c_value_updated", 124);
        let tuple_d = Tuple::new(b"d", b"d_value", 123);
        let tuple_e = Tuple::new(b"e", b"e_value", 123);

        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());
        assert!(leaf_page.add_tuple(&tuple_c).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 1);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c)
        );

        assert!(leaf_page.add_tuple(&tuple_c_same_value_size).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 1);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c_same_value_size)
        );

        assert!(leaf_page.add_tuple(&tuple_c_updated).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 1);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c_updated)
        );

        assert!(leaf_page.delete_key(tuple_c.get_key()).is_some());
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 0);
        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());

        assert!(leaf_page.add_tuple(&tuple_c).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 1);
        assert!(leaf_page.add_tuple(&tuple_d).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 2);
        assert!(leaf_page.add_tuple(&tuple_e).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 3);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_d.get_key())
                .unwrap()
                .equals(&tuple_d)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_e.get_key())
                .unwrap()
                .equals(&tuple_e)
        );
        assert!(leaf_page.add_tuple(&tuple_c_same_value_size).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 3);
        assert!(leaf_page.add_tuple(&tuple_c_updated).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 3);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c_updated)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_d.get_key())
                .unwrap()
                .equals(&tuple_d)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_e.get_key())
                .unwrap()
                .equals(&tuple_e)
        );

        assert!(leaf_page.delete_key(tuple_c.get_key()).is_some());
        assert!(leaf_page.delete_key(tuple_d.get_key()).is_some());
        assert!(leaf_page.delete_key(tuple_e.get_key()).is_some());
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 0);
        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_d.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_e.get_key()).is_none());

        assert!(leaf_page.add_tuple(&tuple_a).0);
        assert!(leaf_page.add_tuple(&tuple_b).0);
        assert!(
            leaf_page
                .get_tuple(tuple_a.get_key())
                .unwrap()
                .equals(&tuple_a)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_b.get_key())
                .unwrap()
                .equals(&tuple_b)
        );
        assert!(leaf_page.add_tuple(&tuple_c).0);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c)
        );
        assert!(leaf_page.add_tuple(&tuple_c_same_value_size).0);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c_same_value_size)
        );
        assert!(leaf_page.add_tuple(&tuple_c_updated).0);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c_updated)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_a.get_key())
                .unwrap()
                .equals(&tuple_a)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_b.get_key())
                .unwrap()
                .equals(&tuple_b)
        );

        assert!(leaf_page.delete_key(tuple_b.get_key()).is_some());
        assert!(leaf_page.delete_key(tuple_c.get_key()).is_some());
        assert!(leaf_page.delete_key(tuple_a.get_key()).is_some());
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 0);

        assert!(leaf_page.add_tuple(&tuple_b).0);
        assert!(leaf_page.add_tuple(&tuple_c).0);
        assert!(leaf_page.add_tuple(&tuple_d).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 3);
        assert!(leaf_page.add_tuple(&tuple_c_same_value_size).0);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c_same_value_size)
        );
        assert!(leaf_page.add_tuple(&tuple_c_updated).0);
        assert!(
            leaf_page
                .get_tuple(tuple_c.get_key())
                .unwrap()
                .equals(&tuple_c_updated)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_b.get_key())
                .unwrap()
                .equals(&tuple_b)
        );
        assert!(
            leaf_page
                .get_tuple(tuple_d.get_key())
                .unwrap()
                .equals(&tuple_d)
        );
    }

    #[test]
    fn test_reset() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };

        let key1: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];
        let key2: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 2];
        let key3: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 3];
        let key4: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 4];
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let tuple1 = Tuple::new(&key1, b"value1", 123);
        let tuple2 = Tuple::new(&key2, b"value2", 123);
        let tuple3 = Tuple::new(&key3, b"value3", 123);
        let tuple4 = Tuple::new(&key4, b"value4", 123);
        leaf_page.set_left_fence_key(&key1);
        leaf_page.set_right_fence_key(&key4);
        leaf_page.set_prefix_length(7);

        assert!(leaf_page.add_tuple(&tuple1).0);
        assert!(leaf_page.add_tuple(&tuple2).0);
        assert!(leaf_page.add_tuple(&tuple3).0);
        assert!(leaf_page.add_tuple(&tuple4).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 4);

        let key5: [u8; 8] = [0, 0, 0, 0, 0, 0, 1, 0];
        let tuple5 = Tuple::new(&key5, b"value5", 123);
        assert!(leaf_page.add_tuple(&tuple5).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 5);
        assert_eq!(LeafPage::get_prefix_length(leaf_page.get_page()), 6);
        assert_eq!(LeafPage::get_key_prefix(leaf_page.get_page()), &key1[..6]);
        assert_eq!(LeafPage::get_left_fence_key(leaf_page.get_page()), &key1);
        assert_eq!(LeafPage::get_right_fence_key(leaf_page.get_page()), &key5);
    }

    #[test]
    fn test_reset_no_prefix() {
        // Left most page - no prefix and no left fence.
        // No compression.
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };

        let key1: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];
        let key2: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 2];
        let key3: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 3];
        let key4: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 4];
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let tuple1 = Tuple::new(&key1, b"value1", 123);
        let tuple2 = Tuple::new(&key2, b"value2", 123);
        let tuple3 = Tuple::new(&key3, b"value3", 123);
        let tuple4 = Tuple::new(&key4, b"value4", 123);
        leaf_page.set_right_fence_key(&key4);
        leaf_page.set_prefix_length(0);

        assert!(leaf_page.add_tuple(&tuple1).0);
        assert!(leaf_page.add_tuple(&tuple2).0);
        assert!(leaf_page.add_tuple(&tuple3).0);
        assert!(leaf_page.add_tuple(&tuple4).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 4);

        let key5: [u8; 8] = [0, 0, 0, 0, 0, 0, 1, 0];
        let tuple5 = Tuple::new(&key5, b"value5", 123);
        assert!(leaf_page.add_tuple(&tuple5).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 5);
        assert_eq!(LeafPage::get_prefix_length(leaf_page.get_page()), 0);
        assert_eq!(LeafPage::get_right_fence_key(leaf_page.get_page()), &key5);
    }

    #[test]
    fn test_reset_to_small_to_reset() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 125,
            block_sanity_size: 4096 - 125,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };

        // Page is too small for the reset
        let key1: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];
        let key2: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 2];
        let key3: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 3];
        let key4: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 4];
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let tuple1 = Tuple::new(&key1, b"value1", 123);
        let tuple2 = Tuple::new(&key2, b"value2", 123);
        let tuple3 = Tuple::new(&key3, b"value3", 123);
        let tuple4 = Tuple::new(&key4, b"value4", 123);
        leaf_page.set_left_fence_key(&key1);
        leaf_page.set_right_fence_key(&key4);
        leaf_page.set_prefix_length(7);

        assert!(leaf_page.add_tuple(&tuple1).0);
        assert!(leaf_page.add_tuple(&tuple2).0);
        assert!(leaf_page.add_tuple(&tuple3).0);
        assert!(leaf_page.add_tuple(&tuple4).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 4);

        let key5: [u8; 8] = [0, 0, 0, 0, 0, 0, 1, 0];
        let tuple5 = Tuple::new(&key5, b"value5", 123);
        // Page is too small
        assert!(!leaf_page.add_tuple(&tuple5).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 4);
        assert_eq!(LeafPage::get_prefix_length(leaf_page.get_page()), 7);
        assert_eq!(LeafPage::get_key_prefix(leaf_page.get_page()), &key1[..7]);
        assert_eq!(LeafPage::get_left_fence_key(leaf_page.get_page()), &key1);
        assert_eq!(LeafPage::get_right_fence_key(leaf_page.get_page()), &key4);
    }

    #[test]
    fn test_reset_to_small_after_reset_to_add_tuple() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 129,
            block_sanity_size: 4096 - 129,
            compressor_type: crate::compressor::CompressorType::None,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        };

        // Page is too small - it can be reset but not with the new tuple
        let key1: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];
        let key2: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 2];
        let key3: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 3];
        let key4: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 4];
        let mut leaf_page = LeafPage::create_new(&page_config, PageNo::new(0, 1), 0);
        let tuple1 = Tuple::new(&key1, b"value1", 123);
        let tuple2 = Tuple::new(&key2, b"value2", 123);
        let tuple3 = Tuple::new(&key3, b"value3", 123);
        let tuple4 = Tuple::new(&key4, b"value4", 123);
        leaf_page.set_left_fence_key(&key1);
        leaf_page.set_right_fence_key(&key4);
        leaf_page.set_prefix_length(7);

        assert!(leaf_page.add_tuple(&tuple1).0);
        assert!(leaf_page.add_tuple(&tuple2).0);
        assert!(leaf_page.add_tuple(&tuple3).0);
        assert!(leaf_page.add_tuple(&tuple4).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 4);

        let key5: [u8; 8] = [0, 0, 0, 0, 0, 0, 1, 0];
        let tuple5 = Tuple::new(&key5, b"value5", 123);
        // Page is too small
        assert!(!leaf_page.add_tuple(&tuple5).0);
        assert_eq!(LeafPage::get_entries_size(leaf_page.get_page()), 4);
        assert_eq!(LeafPage::get_prefix_length(leaf_page.get_page()), 6);
        assert_eq!(LeafPage::get_key_prefix(leaf_page.get_page()), &key1[..6]);
        assert_eq!(LeafPage::get_left_fence_key(leaf_page.get_page()), &key1);
        assert_eq!(LeafPage::get_right_fence_key(leaf_page.get_page()), &key5);
    }
}

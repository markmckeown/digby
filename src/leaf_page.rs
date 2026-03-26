use crate::{Page, block_layer::PageConfig};
use crate::page::PageType;
use crate::page::PageTrait;
use crate::tuple::Tuple;
use crate::tuple::TupleTrait;
use std::cmp::Ordering;

pub struct LeafPage {
    page: Page,
}

impl PageTrait for LeafPage {
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

pub struct Slot {
    offset: u16,
    key_len: u8,
    val_len: u16
}


// Header
// | Page No (8 bytes) | VersionHolder(8 bytes) | Entries(u16) | Free_Space(u16) |
// | prefix_length (u8) | slot | slot | slot | ...
// | heap
// | key | value | key | value |
impl LeafPage {
    const HEADER_SIZE: usize = 21; // 8 + 8 + 2 + 2 + 1
    const SLOT_SIZE: usize = 5; // 2 (offset) + 1 (key_len) + 2 (val_len)

    pub fn create_new(page_config: &PageConfig, page_number: u64) -> Self {
        LeafPage::new(page_config.block_size, page_config.page_size, page_number)
    }

    fn new(block_size: usize, page_size: usize, page_number: u64) -> Self {
        let mut page = Page::new(block_size, page_size);
        page.set_type(PageType::LeafPage);
        page.set_page_number(page_number);
        let mut leaf_page = LeafPage { page };
        leaf_page.set_free_space(page_size as u16 - LeafPage::HEADER_SIZE as u16);
        leaf_page
    }


    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::LeafPage {
            panic!("Page type is not Leaf");
        }
        LeafPage { page }
    }

    pub fn get_entries(&self) -> u16 {
        let bytes = &self.page.get_page_bytes()[16..18];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn set_entries(&mut self, entries: u16) {
        let bytes = entries.to_le_bytes();
        self.page.get_page_bytes_mut()[16..18].copy_from_slice(&bytes);
    }

    pub fn get_free_space(&self) -> u16 {
        let bytes = &self.page.get_page_bytes()[18..20];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn set_free_space(&mut self, free_space: u16) {
        let bytes = free_space.to_le_bytes();
        self.page.get_page_bytes_mut()[18..20].copy_from_slice(&bytes);
    }

    pub fn get_prefix_length(&self) -> u8 {
        self.page.get_page_bytes()[20]
    }

    pub fn set_prefix_length(&mut self, prefix_length: u8) {
        self.page.get_page_bytes_mut()[20] = prefix_length;
    }
    
    pub fn get_slot_at_index(&self, index: usize) -> Slot {
        assert!(index < self.get_entries() as usize);
        let slot_offset = LeafPage::HEADER_SIZE + index * LeafPage::SLOT_SIZE;
        let offset_bytes = &self.page.get_page_bytes()[slot_offset..slot_offset + 2];
        let offset = u16::from_le_bytes(offset_bytes.try_into().unwrap());
        let key_len = self.page.get_page_bytes()[slot_offset + 2];
        let val_len_bytes = &self.page.get_page_bytes()[slot_offset + 3..slot_offset + 5];
        let val_len = u16::from_le_bytes(val_len_bytes.try_into().unwrap());
        Slot { offset, key_len, val_len }
    }

    pub fn set_slot_at_index(&mut self, index: usize, slot: Slot) {
        let slot_offset = LeafPage::HEADER_SIZE + index * LeafPage::SLOT_SIZE;
        let offset_bytes = slot.offset.to_le_bytes();
        self.page.get_page_bytes_mut()[slot_offset..slot_offset + 2].copy_from_slice(&offset_bytes);
        self.page.get_page_bytes_mut()[slot_offset + 2] = slot.key_len;
        let val_len_bytes = slot.val_len.to_le_bytes();
        self.page.get_page_bytes_mut()[slot_offset + 3..slot_offset + 5].copy_from_slice(&val_len_bytes);
    }

    pub fn get_value_at_slot(&self, slot: &Slot) -> &[u8] {
        let val_offset = (slot.offset + slot.key_len as u16) as usize;
        &self.page.get_page_bytes()[val_offset..val_offset + slot.val_len as usize]
    }

    pub fn get_key_at_slot(&self, slot: &Slot) -> &[u8] {
        let key_offset = slot.offset as usize;
        &self.page.get_page_bytes()[key_offset..key_offset + slot.key_len as usize]
    }

    pub fn get_key_prefix(&self) -> &[u8] {
        let prefix_length = self.get_prefix_length() as usize;
        if prefix_length == 0 {
            return &[];
        }
        let slot_0 = self.get_slot_at_index(0);
        &self.get_key_at_slot(&slot_0)[0 .. prefix_length]
    }

    pub fn get_index_for_key(&self, key_suffix: &[u8]) -> (bool, usize) {
        let entries = self.get_entries() as usize;

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

    pub fn shift_slots_right_from(&mut self, from_index: usize) {
        let entries = self.get_entries() as usize;
        if entries == from_index {
            return;
        }
        self.page.get_page_bytes_mut().copy_within(
            LeafPage::HEADER_SIZE + from_index * LeafPage::SLOT_SIZE..LeafPage::HEADER_SIZE + entries * LeafPage::SLOT_SIZE,
            LeafPage::HEADER_SIZE + (from_index + 1) * LeafPage::SLOT_SIZE
        );
    }

    pub fn shift_slots_left_from(&mut self, from_index: usize) {
        let entries = self.get_entries() as usize;
        self.page.get_page_bytes_mut().copy_within(
            LeafPage::HEADER_SIZE + (from_index + 1) * LeafPage::SLOT_SIZE..LeafPage::HEADER_SIZE + entries * LeafPage::SLOT_SIZE,
            LeafPage::HEADER_SIZE + from_index * LeafPage::SLOT_SIZE
        );
    }

    pub fn add_tuple(&mut self, tuple: &Tuple) {
        let prefix_length = self.get_prefix_length() as usize;
        assert!(tuple.get_key().len() >= prefix_length, "Tuple key length is smaller than the prefix length of the page.");
        assert!(tuple.get_key().starts_with(self.get_key_prefix()), "Tuple key does not match the prefix of the page.");
        let key_suffix = &tuple.get_key()[prefix_length..];
        let (found, index) = self.get_index_for_key(key_suffix);
        assert!(!found, "Key already exists in the page.");
        self.add_key_value_at_index(index, key_suffix, tuple.get_version_value());
        
    }

    pub fn add_key_value_at_index(&mut self, index: usize, key: &[u8], value: &[u8]) {
        // Sanity check
        let new_entry_size = key.len() + value.len();
        let new_entry_total_size = new_entry_size + LeafPage::SLOT_SIZE;
        let free_space = self.get_free_space() as usize;
        assert!(new_entry_total_size <= free_space);

        // Find offset where the key/value entry can be added.
        let entries = self.get_entries() as usize;
        let header_plus_slots_size = LeafPage::HEADER_SIZE + entries * LeafPage::SLOT_SIZE;
        let entries_size = self.page.page_size - (header_plus_slots_size + free_space);
        let old_entries_offset = self.page.page_size - entries_size;
        let new_entry_offset = old_entries_offset - new_entry_size;
        
        // Add key/value at the offset
        self.page.get_page_bytes_mut()[new_entry_offset..new_entry_offset + key.len()].copy_from_slice(key);
        self.page.get_page_bytes_mut()[new_entry_offset + key.len()..new_entry_offset + key.len() + value.len()].copy_from_slice(value);
        
        // Create a slot and add it.
        let slot = Slot { offset: new_entry_offset as u16, key_len: key.len() as u8, val_len: value.len() as u16 };
        self.shift_slots_right_from(index);
        self.set_slot_at_index(index, slot);

        // Update entries and free space.
        self.set_entries((entries + 1) as u16);
        self.set_free_space(free_space as u16 - new_entry_total_size as u16);
    }

    
    pub fn get_tuple(&self, key: &[u8]) -> Option<Tuple> {
        let prefix_length = self.get_prefix_length() as usize;
        if prefix_length > 0 {
            assert!(key.len() >= prefix_length, "Key length is smaller than the prefix length of the page.");
            assert!(key.starts_with(self.get_key_prefix()), "Key does not match the prefix of the page.");
        }
        let (found, index) = self.get_index_for_key(&key[prefix_length..]);
        if !found {
            return None;
        }
        Some(self.get_tuple_at_index(index))
    }    

    pub fn get_tuple_at_index(&self, index: usize) -> Tuple {
        let slot = self.get_slot_at_index(index);
        let key_prefix = self.get_key_prefix();
        let key = self.get_key_at_slot(&slot);
        let value = self.get_value_at_slot(&slot);
        let mut full_key = Vec::with_capacity(key_prefix.len() + key.len());
        full_key.extend_from_slice(key_prefix);
        full_key.extend_from_slice(key);
        Tuple::new(&full_key, &value[8..], u64::from_le_bytes(value[0..8].try_into().unwrap()))
    }

    


    /**
     * Remove key and value. Returns true of the key was found and removed, 
     * false if the key was not found.
     */
    pub fn remove_key(&mut self, key: &[u8]) -> bool {
        let prefix_length = self.get_prefix_length() as usize;
        if prefix_length > 0 {
            assert!(key.len() >= prefix_length, "Key length is smaller than the prefix length of the page.");
            assert!(key.starts_with(self.get_key_prefix()), "Key does not match the prefix of the page.");
        }
         let (found, index) = self.get_index_for_key(&key[prefix_length..]);
         if !found {
             return false;
         }
         self.remove_key_value_at_index(index);
         true
    }

    /**
     * The approach removes the bytes from the page and shovels the entries
     * around to fill the gap. An alternative approach is to leave the 
     * hole in the entries and attempt to fill it in when adding new entries.
     */
    pub fn remove_key_value_at_index(&mut self, index: usize) {
        let entries = self.get_entries() as usize;
        assert!(index < entries);
        let slot = self.get_slot_at_index(index);
        let entry_size = slot.key_len as usize + slot.val_len as usize;
        
        let free_space = self.get_free_space() as usize;
        let header_plus_slots_size = LeafPage::HEADER_SIZE + entries * LeafPage::SLOT_SIZE;
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
            self.set_free_space((free_space + entry_size + LeafPage::SLOT_SIZE) as u16);
            self.set_entries((new_entry_count) as u16);
            return;
        }

        // Need to move some bytes in the entries and update the slot offsets for the entries in the head that are being shifted.
        let head = entry_offset - entries_offset;
        self.page.get_page_bytes_mut().copy_within(entries_offset .. entries_offset + head, entries_offset + entry_size);
        
        // Need to update the slots in the head to reflect the shift in entries.
        let mut slot_offset = LeafPage::HEADER_SIZE;
        for _i in 0..new_entry_count {
            let slot_offset_bytes = &self.page.get_page_bytes()[slot_offset..slot_offset + 2];
            let slot_entry_offset = u16::from_le_bytes(slot_offset_bytes.try_into().unwrap());
            if slot_entry_offset < entry_offset as u16 {
                // This slot is in the head, need to update the offset to reflect the shift.
                let new_offset = slot_entry_offset + entry_size as u16;
                let new_offset_bytes = new_offset.to_le_bytes();
                self.page.get_page_bytes_mut()[slot_offset..slot_offset + 2].copy_from_slice(&new_offset_bytes);
            }
            slot_offset += LeafPage::SLOT_SIZE;
        }

        // Update entries and free space.
        self.set_entries(new_entry_count as u16);
        self.set_free_space((free_space + entry_size + LeafPage::SLOT_SIZE) as u16);
    }


}

#[cfg(test)]
mod tests {
    use super::*;

   
     #[test]
     fn test_add_and_remove_tuple() {
        let page_config = PageConfig { block_size: 4096, page_size: 4000 };
        let mut leaf_page = LeafPage::create_new(&page_config, 1);
        let tuple_a = Tuple::new(b"a", b"a_value", 123);
        let tuple_b = Tuple::new(b"b", b"b_value", 123);
        let tuple_c = Tuple::new(b"c", b"c_value", 123);

        assert!(leaf_page.get_tuple(tuple_a.get_key()).is_none());
        leaf_page.add_tuple(&tuple_a);
        assert_eq!(leaf_page.get_entries(), 1);
        assert!(leaf_page.get_tuple(tuple_a.get_key()).unwrap().equals(&tuple_a));
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());

        leaf_page.add_tuple(&tuple_c);
        assert_eq!(leaf_page.get_entries(), 2);
        assert!(leaf_page.get_tuple(tuple_a.get_key()).unwrap().equals(&tuple_a));
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_c.get_key()).unwrap().equals(&tuple_c));
        
        leaf_page.add_tuple(&tuple_b);
        assert_eq!(leaf_page.get_entries(), 3);
        assert!(leaf_page.get_tuple(tuple_a.get_key()).unwrap().equals(&tuple_a));
        assert!(leaf_page.get_tuple(tuple_b.get_key()).unwrap().equals(&tuple_b));
        assert!(leaf_page.get_tuple(tuple_c.get_key()).unwrap().equals(&tuple_c));

        assert!(leaf_page.remove_key(tuple_b.get_key()));
        assert_eq!(leaf_page.get_entries(), 2);
        assert!(leaf_page.get_tuple(tuple_a.get_key()).unwrap().equals(&tuple_a));
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_c.get_key()).unwrap().equals(&tuple_c));

        assert!(leaf_page.remove_key(tuple_c.get_key()));
        assert_eq!(leaf_page.get_entries(), 1);
        assert!(leaf_page.get_tuple(tuple_a.get_key()).unwrap().equals(&tuple_a));
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());

        assert!(leaf_page.remove_key(tuple_a.get_key()));
        assert_eq!(leaf_page.get_entries(), 0);
        assert!(leaf_page.get_tuple(tuple_a.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_b.get_key()).is_none());
        assert!(leaf_page.get_tuple(tuple_c.get_key()).is_none());
     }


}
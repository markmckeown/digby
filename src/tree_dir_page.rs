use crate::page::Page;
use crate::page::PageTrait;
use crate::TreeDirEntry;
use std::io::Cursor;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::collections::VecDeque;



// Header 24 bytes.
// | Checksum(u32)   | Page No (u32) | VersionHolder (8 bytes)  | Entries (u16) | FreeSpace (u16) | 
// | LeftLeafPage (u32) |
//
// | TreeDirEntry | TreeDirEntry ...|
//
// | IndexEntry | IndexEntry |
pub struct TreeDirPage {
    page: Page
}

impl PageTrait for TreeDirPage {
    fn get_bytes(&self) -> &[u8] {
        self.page.get_bytes()
    }

    fn get_page_number(& self) -> u32 {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self,  page_no: u32) -> () {
        self.page.set_page_number(page_no)
    }

    fn get_page(&mut self) -> &mut Page {
        &mut self.page
    }

    fn get_version(& self) -> u64 {
        self.page.get_version()     
    }

    fn set_version(&mut self, version: u64) -> () {
        self.page.set_version(version);   
    }
}

impl TreeDirPage {
    const HEADER_SIZE: u16 =  24;

    pub fn new(page_size: u64, page_number: u32, version: u64) -> Self {
        let mut tree_page_dir =  TreeDirPage {
            page: Page::new(page_size),
        };
        tree_page_dir.page.set_type(crate::page::PageType::TreeDirPage);
        tree_page_dir.page.set_page_number(page_number);
        tree_page_dir.set_version(version);
        assert!(page_size < u16::MAX as u64);
        tree_page_dir.set_free_space(page_size  as u16 - TreeDirPage::HEADER_SIZE);
        tree_page_dir.set_entries(0);
        tree_page_dir.set_page_to_left(0);
        tree_page_dir
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != crate::page::PageType::TreeDirPage {
            panic!("Invalid page type for TreePageDir");
        }

        let tree_page_dir = TreeDirPage { page: page };
        tree_page_dir
    }

    pub fn get_page_to_left(&self) -> u32 {
        let index = 20;
        let slice = &self.page.get_bytes()[index..index + 4];
        let array: [u8; 4] = slice.try_into().unwrap();
        u32::from_le_bytes(array)
    }

    pub fn set_page_to_left(&mut self, page_no: u32) -> () {
        let index = 20;
        self.page.get_bytes_mut()[index..index+4].copy_from_slice(&page_no.to_le_bytes());
    }

 pub fn get_entries(&self) -> u16 {
        let index = 16;
        let slice = &self.page.get_bytes()[index..index + 2];
        let array: [u8; 2] = slice.try_into().unwrap();
        u16::from_le_bytes(array)
    }

    pub fn set_entries(&mut self, entries: u16) -> () {
        let index = 16;
        self.page.get_bytes_mut()[index..index+2].copy_from_slice(&entries.to_le_bytes());
    }

    pub fn get_free_space(&self) -> u16 {
        let index = 18;
        let slice = &self.page.get_bytes()[index..index + 2];
        let array: [u8; 2] = slice.try_into().unwrap();
        u16::from_le_bytes(array)
    }

    pub fn set_free_space(&mut self, entries: u16) -> () {
        let index = 18;
        self.page.get_bytes_mut()[index..index+2].copy_from_slice(&entries.to_le_bytes());
    }


    pub fn can_fit_entries(&self, entries: &Vec<TreeDirEntry>) -> bool {
        if entries.len() == 1 {
            // if only one entry then its just an update, nothing to add.
            return true;
        }

        let mut size: usize = 0;
        let mut count = 0;
        for entry in entries {
            if count == 0 {
                // skip first one as its an update.
                count = count + 1;
                continue;
            }
            size = size + entry.get_byte_size() + 2; 
        }
        let free_space: usize = self.get_free_space() as usize;
        free_space >= size
    }

    // If there is only one entry then its just an update. Any other entries are new entries
    // that should be added.
    pub fn add_entries(&mut self, entries: Vec<TreeDirEntry>, page_size: usize) -> () {
        assert!(!entries.is_empty(), "Cannot add zero entries to tree dir page.");
        assert!(self.can_fit_entries(&entries), "Cannot fit entries into tree dir page.");
        assert!(entries.windows(2).all(|w| w[0].get_key() <= w[1].get_key()), 
                "Entries must be sorted for adding to tree dir page");
        
        // Convert to Deque
        let mut deque: VecDeque<TreeDirEntry>  = entries.into();
        // pop first entry
        let entry = deque.pop_front().unwrap();
        
        // Empty page - there must be more than one entry. Set first to be the left of
        // page then add other entries
        if self.get_entries() == 0 {
            // If the page is empty then there must be more than one entry.
            assert!(!deque.is_empty(), "Cannot add single entry to an empty tree dir page.");
            self.set_page_to_left(entry.get_page_no());
            while !deque.is_empty() {
                self.add_tree_dir_in_page(deque.pop_front().unwrap(), page_size);
            }
            return;
        }

        // Need to check if first entry is to the left of the left key, if it is then set left page.
        if entry.get_key() < self.get_dir_left_key(page_size).unwrap().as_ref() {
            self.set_page_to_left(entry.get_page_no());
        } else {
            self.set_page_no_for_key(entry.get_key().to_vec(), entry.get_page_no(), page_size);
        }
        while !deque.is_empty() {
            self.add_tree_dir_in_page(deque.pop_front().unwrap(), page_size);
        }
    }


    // Store entry in page. The check for left-hand-page should already be done. This just
    // adds the entry to the page. It will replace any existing matching key.
    // TODO this is inefficient, should use memmove.
    fn add_tree_dir_in_page(&mut self, table_dir_entry: TreeDirEntry, page_size: usize) -> () {

        // TODO inefficent way to do this.
        let sorted = self.build_sorted_tree_dir_entries(table_dir_entry, page_size);
        // Clear the page and re-add all tree_dir_entries
        self.set_entries(0);
        self.set_free_space(page_size as u16 - TreeDirPage::HEADER_SIZE); // Reset free space

        for entry in sorted {
            self.append_tree_dir_entry(&entry, page_size as u64);
        }
    }

    // Add a directory entry to the top of the stack of entries. This should be called from 
    // store_tree_dir_in_page which sorts the entries before adding them.
    fn append_tree_dir_entry(&mut self, tree_dir_entry: &TreeDirEntry, page_size: u64) -> () {
        let tree_dir_entry_size: usize = tree_dir_entry.get_byte_size();
            
        let current_entries = self.get_entries();
        let current_entries_size: usize = current_entries as usize * 2; // Each entry has 2 bytes for index
        let free_space = self.get_free_space();

        let tree_dir_entry_offset : usize = (page_size as usize) - (free_space as usize + current_entries_size);
        let page_bytes = self.page.get_bytes_mut();
        page_bytes[tree_dir_entry_offset..tree_dir_entry_offset + tree_dir_entry_size as usize].copy_from_slice(tree_dir_entry.get_serialized());

        let mut cursor = Cursor::new(&mut page_bytes[page_size as usize - (current_entries_size + 2 as usize)..]);
        cursor.write_u16::<byteorder::LittleEndian>(tree_dir_entry_offset as u16).expect("Failed to write tuple offset");
        self.set_entries(current_entries + 1);
        self.set_free_space(free_space - (tree_dir_entry_size as u16 + 2));
    }

    // Create a sorted list of entries with the new entry - replace any existing entry with the same key.
    fn build_sorted_tree_dir_entries(&self, tree_dir_entry: TreeDirEntry, page_size: usize) -> Vec<TreeDirEntry> {
        let mut dir_entries = self.get_all_dir_entries(page_size);
        dir_entries.push(tree_dir_entry);
        dir_entries.sort_by(|b, a| b.get_key().cmp(a.get_key()));
        dir_entries
    }

    // Get all tuples in the page - used for rebuilding the page when adding or updating an entry.
    fn get_all_dir_entries(&self, page_size: usize) -> Vec<TreeDirEntry> {
        let entries = self.get_entries();
        let mut dir_entries = Vec::new();
        for i in 0..entries {
            let dir_entry = self.get_dir_entry_index(i, page_size);
            dir_entries.push(dir_entry);
        }
        dir_entries
    }

    // Page is full and need to split - take the right half entries and reset the entries
    // count and the free space.
    pub fn get_right_half_entries(&mut self, page_size: usize) -> Vec<TreeDirEntry> {
        let entries = self.get_entries();
        let start = (entries+1)/2;
        let mut tree_dir_entries = Vec::new();
        let mut size_removed: u16 = 0;
        for i in start..entries {
            let tree_dir_entry = self.get_dir_entry_index(i, page_size);
            size_removed =  tree_dir_entry.get_byte_size() as u16 + 2;
            tree_dir_entries.push(tree_dir_entry);
        }

        self.set_free_space(self.get_free_space() + size_removed);
        self.set_entries(start);
        tree_dir_entries
    }

    // Get the entry at an index - used in binary search. 
    fn get_dir_entry_index(&self, index: u16, page_size: usize) -> TreeDirEntry {
        let entries = self.get_entries();

        assert!(index < entries);

        let offset = (index * 2) + 2;
        let mut cursor = Cursor::new(&self.page.get_bytes()[page_size - offset as usize ..]);
        let tree_dir_index = cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;
        
        let mut tree_dir_cursor = Cursor::new(&self.page.get_bytes()[tree_dir_index..]);
        let _page_no = tree_dir_cursor.read_u32::<byteorder::LittleEndian>().unwrap();
        let key_len = tree_dir_cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;
        let tree_dir_entry_size = key_len + 4 + 2;
        TreeDirEntry::from_bytes(self.page.get_bytes()[tree_dir_index..tree_dir_index + tree_dir_entry_size].to_vec())
    }

    // Get the left sided key in the page.
    fn get_dir_left_key(&self, page_size: usize) -> Option<Vec<u8>> {
        if self.get_entries() == 0 {
            return None;
        }
        Some(self.get_dir_entry_index(0, page_size).get_key().to_vec())
    }

    // Get the page for a key. The key can be: 
    //   Less than the left most key so use the page to the left
    //   Equal to a key, so use that page.
    //   Between two keys so use the first key in the pair of keys
    //   Greater than the right most key so use it.
    //
    pub fn get_next_page(&self, key: Vec<u8>, page_size: usize) -> u32 {
        let entries = self.get_entries();
        assert!(entries != 0);
        if key < self.get_dir_entry_index(0, page_size).get_key().to_vec() {
            return self.get_page_to_left()
        }

        let last_entry = self.get_dir_entry_index(entries - 1, page_size);
        if key > last_entry.get_key().to_vec() { 
            return last_entry.get_page_no()
        }

        let mut left: u16 = 0;
        let mut right: u16 = entries - 1;

        while left <= right {
            let mid = left + (right - left) / 2;
            let entry: TreeDirEntry = self.get_dir_entry_index(mid, page_size);
            if entry.get_key() == key {
                return entry.get_page_no()
            } else if entry.get_key().to_vec() < key {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        self.get_dir_entry_index(right, page_size).get_page_no()
    }


    fn set_page_no_for_key(&mut self, key: Vec<u8>, new_page_no: u32, page_size: usize) {
        let entries = self.get_entries();
        assert!(entries != 0);

        let mut left: u16 = 0;
        let mut right: u16 = entries - 1;
        let mut index: u16 = 0;
        let mut is_set = false;
        while left <= right {
            let mid = left + (right - left) / 2;
            let entry: TreeDirEntry = self.get_dir_entry_index(mid, page_size);
            let entry_key = entry.get_key().to_vec();
            if entry_key == key {
                is_set = true;
                index = mid;
                break;
            } else if entry_key < key {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        if !is_set {
            index = right;
        }

        let offset = (index * 2) + 2;
        let mut cursor = Cursor::new(&self.page.get_bytes()[page_size - offset as usize ..]);
        let tree_dir_index = cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;
        
        let page_bytes = self.page.get_bytes_mut();
        page_bytes[tree_dir_index..tree_dir_index + 4 as usize].copy_from_slice(new_page_no.to_le_bytes().as_ref());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_entries() {
        // Split root page to create two child pages.
        let tree_dir_entry_1 = TreeDirEntry::new(b"d".to_vec(), 45);
        let tree_dir_entry_2 = TreeDirEntry::new(b"s".to_vec(), 75);
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        entries.push(tree_dir_entry_1);
        entries.push(tree_dir_entry_2);
        let mut tree_dir_page = TreeDirPage::new(4096, 3, 567);
        tree_dir_page.add_entries(entries,4096);

        assert_eq!(tree_dir_page.get_page_to_left(), 45);
        assert_eq!(tree_dir_page.get_entries(), 1);
        assert_eq!(tree_dir_page.get_next_page(b"a".to_vec(), 4096), 45);
        assert_eq!(tree_dir_page.get_next_page(b"f".to_vec(), 4096), 45);
        assert_eq!(tree_dir_page.get_next_page(b"d".to_vec(), 4096), 45);
        assert_eq!(tree_dir_page.get_next_page(b"s".to_vec(), 4096), 75);
        assert_eq!(tree_dir_page.get_next_page(b"u".to_vec(), 4096), 75);

        let tree_dir_entry_3 = TreeDirEntry::new(b"a".to_vec(), 23);
        entries = Vec::new();
        entries.push(tree_dir_entry_3);
        tree_dir_page.add_entries(entries,4096);
        assert_eq!(tree_dir_page.get_page_to_left(), 23);
        assert_eq!(tree_dir_page.get_entries(), 1);
        assert_eq!(tree_dir_page.get_next_page(b"a".to_vec(), 4096), 23);

        let tree_dir_entry_4 = TreeDirEntry::new(b"t".to_vec(), 99);
        entries = Vec::new();
        entries.push(tree_dir_entry_4);
        tree_dir_page.add_entries(entries,4096);
        assert_eq!(tree_dir_page.get_page_to_left(), 23);
        assert_eq!(tree_dir_page.get_entries(), 1);
        assert_eq!(tree_dir_page.get_next_page(b"s".to_vec(), 4096), 99);
    }


    #[test]
    fn test_add_entries_1() {
        // Split root page to create two child pages.
        let tree_dir_entry_1 = TreeDirEntry::new(b"d".to_vec(), 45);
        let tree_dir_entry_2 = TreeDirEntry::new(b"s".to_vec(), 75);
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        entries.push(tree_dir_entry_1);
        entries.push(tree_dir_entry_2);
        let mut tree_dir_page = TreeDirPage::new(4096, 3, 567);
        tree_dir_page.add_entries(entries,4096);
        assert_eq!(tree_dir_page.get_entries(), 1);
        assert_eq!(tree_dir_page.get_all_dir_entries(4096).get(0).unwrap().get_key(), b"s".to_vec());


        // Add to the left. Page with "d" splits.
        let tree_dir_entry_3 = TreeDirEntry::new(b"b".to_vec(), 25);
        let tree_dir_entry_4 = TreeDirEntry::new(b"c".to_vec(), 85);
        entries = Vec::new();
        entries.push(tree_dir_entry_3);
        entries.push(tree_dir_entry_4);
        tree_dir_page.add_entries(entries,4096);
        assert_eq!(tree_dir_page.get_entries(), 2);
        assert_eq!(tree_dir_page.get_all_dir_entries(4096).get(0).unwrap().get_key(), b"c".to_vec());
        assert_eq!(tree_dir_page.get_all_dir_entries(4096).get(1).unwrap().get_key(), b"s".to_vec());

        assert_eq!(tree_dir_page.get_page_to_left(), 25);
        assert_eq!(tree_dir_page.get_entries(), 2);
        assert_eq!(tree_dir_page.get_next_page(b"f".to_vec(), 4096), 85);
        assert_eq!(tree_dir_page.get_next_page(b"s".to_vec(), 4096), 75);

        
        let tree_dir_entry_5 = TreeDirEntry::new(b"f".to_vec(), 185);
        entries = Vec::new();
        entries.push(tree_dir_entry_5);
        tree_dir_page.add_entries(entries,4096);
        assert_eq!(tree_dir_page.get_next_page(b"e".to_vec(), 4096), 185);

    }


    #[test]
    fn test_add_entries_2() {
        // Split root page to create two child pages.
        let tree_dir_entry_1 = TreeDirEntry::new(b"d".to_vec(), 45);
        let tree_dir_entry_2 = TreeDirEntry::new(b"p".to_vec(), 75);
        let tree_dir_entry_3 = TreeDirEntry::new(b"t".to_vec(), 175);
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        entries.push(tree_dir_entry_1);
        entries.push(tree_dir_entry_2);
        entries.push(tree_dir_entry_3);
        let mut tree_dir_page = TreeDirPage::new(4096, 3, 567);
        tree_dir_page.add_entries(entries,4096);
        assert_eq!(tree_dir_page.get_entries(), 2);
        assert_eq!(tree_dir_page.get_all_dir_entries(4096).get(0).unwrap().get_key(), b"p".to_vec());
        assert_eq!(tree_dir_page.get_all_dir_entries(4096).get(1).unwrap().get_key(), b"t".to_vec());


        // The page wih p will split. The first key in this page could be q as it may not have
        // the lowest key
        let tree_dir_entry_4 = TreeDirEntry::new(b"q".to_vec(), 245);
        let tree_dir_entry_5 = TreeDirEntry::new(b"r".to_vec(), 275);
        entries = Vec::new();
        entries.push(tree_dir_entry_4);
        entries.push(tree_dir_entry_5);
        tree_dir_page.add_entries(entries,4096);
        assert_eq!(tree_dir_page.get_entries(), 3);
        assert_eq!(tree_dir_page.get_all_dir_entries(4096).get(0).unwrap().get_key(), b"p".to_vec());
        assert_eq!(tree_dir_page.get_all_dir_entries(4096).get(1).unwrap().get_key(), b"r".to_vec());
        assert_eq!(tree_dir_page.get_all_dir_entries(4096).get(2).unwrap().get_key(), b"t".to_vec());

        assert_eq!(tree_dir_page.get_next_page(b"f".to_vec(), 4096), 45);
        assert_eq!(tree_dir_page.get_next_page(b"p".to_vec(), 4096), 245);
        assert_eq!(tree_dir_page.get_next_page(b"q".to_vec(), 4096), 245);
        assert_eq!(tree_dir_page.get_next_page(b"s".to_vec(), 4096), 275);
        assert_eq!(tree_dir_page.get_next_page(b"u".to_vec(), 4096), 175);
    }
}
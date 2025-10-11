use crate::page::Page;
use crate::page::PageTrait;
use crate::TreeDirEntry;
use std::io::Cursor;
use byteorder::{ReadBytesExt, WriteBytesExt};

// Header 28 bytes.
// | Checksum(u32)   | Page No (u32) | VersionHolder (8 bytes)  | Entries (u16) | FreeSpace (u16) | 
// | ParentPage (u32) | LeftLeafPage (u32) |
//
// | TreeDirEntry | TreeDirEntry ...|
//
// | IndexEntry | IndexEntry |
pub struct TreeInternalPage {
    page: Page
}

impl PageTrait for TreeInternalPage {
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

impl TreeInternalPage {
    const HEADER_SIZE: u16 =  28;

pub fn new(page_size: u64, page_number: u32, version: u64) -> Self {
        let mut tree_page_dir =  TreeInternalPage {
            page: Page::new(page_size),
        };
        tree_page_dir.page.set_type(crate::page::PageType::TreeInternal);
        tree_page_dir.page.set_page_number(page_number);
        tree_page_dir.set_version(version);
        assert!(page_size < u16::MAX as u64);
        tree_page_dir.set_free_space(page_size  as u16 - TreeInternalPage::HEADER_SIZE);
        tree_page_dir.set_entries(0);
        tree_page_dir.set_page_to_left(0);
        tree_page_dir
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != crate::page::PageType::TreeInternal {
            panic!("Invalid page type for TreePageDir");
        }

        let tree_page_dir = TreeInternalPage { page };
        tree_page_dir
    }

    pub fn get_page_to_left(&self) -> u32 {
        let index = 24;
        let slice = &self.page.get_bytes()[index..index + 4];
        let array: [u8; 4] = slice.try_into().unwrap();
        u32::from_le_bytes(array)
    }

    pub fn set_page_to_left(&mut self, page_no: u32) -> () {
        let index = 24;
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

    pub fn get_parent_page(&self) -> u32 {
        let index = 20;
        let slice = &self.page.get_bytes()[index..index + 4];
        let array: [u8; 4] = slice.try_into().unwrap();
        u32::from_le_bytes(array)
    }

    pub fn set_parent_page(&mut self, page_no: u32) -> () {
        let index = 20;
        self.page.get_bytes_mut()[index..index + 4].copy_from_slice(&page_no.to_le_bytes());
    }


     pub fn can_fit(&self, size: usize) -> bool {
        let free_space: usize = self.get_free_space() as usize;
        free_space >= size + 2
    }

    pub fn can_fit_entries(&self, entries: &Vec<TreeDirEntry>) -> bool {
        let mut size: usize = 0;
        for entry in entries {
            size = size + entry.get_byte_size() + 2; 
        }
        let free_space: usize = self.get_free_space() as usize;
        free_space >= size
    }



    pub fn add_first_key(&mut self, page_no_left: u32, key: Vec<u8>, page_to_right: u32, page_size: usize) {
        assert!(self.get_entries() == 0);
        let table_dir_entry = TreeDirEntry::new(key, page_to_right);
        assert!(self.can_fit(table_dir_entry.get_byte_size()), "Cannot fit first tree_dir_entry in page");
        self.set_page_to_left(page_no_left);

        // Store page.
        self.add_tree_dir_entry(&table_dir_entry, page_size as u64);
    }

    pub fn add_page_entry(&mut self, page_no_left: u32, key: Vec<u8>, page_to_right: u32, page_size: usize) {
        let key_copy = key[..].to_vec();
        let table_dir_entry = TreeDirEntry::new(key, page_to_right);
        let table_dir_entry_size: usize = table_dir_entry.get_byte_size();
        assert!(self.can_fit(table_dir_entry_size), "Cannot fit tree_dir_entry in page");

        // Store in the page
        self.store_tree_dir_in_page(table_dir_entry, page_size);
        
        // If there is only one entry in the page, then need to set the left page.
        if self.get_entries() == 1 {
            self.set_page_to_left(page_no_left);
        }

        // If this entry is now the first need to set the page to the left.
        let first_entry = self.get_dir_entry_index(0, page_size);
        if first_entry.get_key() == key_copy {
            self.set_page_to_left(page_no_left);
        }
    }


    // Note this does not deal with updating the leftmost entry.
    pub fn store_tree_dir_in_page(&mut self, table_dir_entry: TreeDirEntry, page_size: usize) -> () {
        assert!(self.can_fit(table_dir_entry.get_byte_size()), "Cannot fit tree_dir_entry in page");
        
        // TODO wildly inefficent way to do this.
        let sorted = self.build_sorted_tree_dir_entries(table_dir_entry, page_size);
        // Clear the page and re-add all tree_dir_entries
        self.set_entries(0);
        self.set_free_space(page_size as u16 - TreeInternalPage::HEADER_SIZE); // Reset free space

        for entry in sorted {
            self.add_tree_dir_entry(&entry, page_size as u64);
        }
    }


    pub fn add_tree_dir_entry(&mut self, tree_dir_entry: &TreeDirEntry, page_size: u64) -> () {
        let tree_dir_entry_size: usize = tree_dir_entry.get_byte_size();
        assert!(self.can_fit(tree_dir_entry_size), "Cannot add TreeDirEntry to page, not enough space.");
            
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


    fn build_sorted_tree_dir_entries(&self, tree_dir_entry: TreeDirEntry, page_size: usize) -> Vec<TreeDirEntry> {
        let mut dir_entries = self.get_all_dir_entries(page_size);
        dir_entries.retain(|t| t.get_key() != tree_dir_entry.get_key());
        dir_entries.push(tree_dir_entry);
        dir_entries.sort_by(|b, a| b.get_key().cmp(a.get_key()));
        dir_entries
    }


    // Get all tuples in the DataPage - used for rebuilding the page when adding or updating a tuple.
    pub fn get_all_dir_entries(&self, page_size: usize) -> Vec<TreeDirEntry> {
        let entries = self.get_entries();
        let mut dir_entries = Vec::new();
        for i in 0..entries {
            let dir_entry = self.get_dir_entry_index(i, page_size);
            dir_entries.push(dir_entry);
        }
        dir_entries
    }

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

    pub fn get_dir_left_key(&self, page_size: usize) -> Option<Vec<u8>> {
        if self.get_entries() == 0 {
            return None;
        }
        Some(self.get_dir_entry_index(0, page_size).get_key().to_vec())
    }


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

        let mut left = 0;
        let mut right = entries as u32 - 1;

        while left <= right {
            let mid = left + (right - left) / 2;
            let entry: TreeDirEntry = self.get_dir_entry_index(mid as u16, page_size);
            if entry.get_key() == key {
                return entry.get_page_no()
            } else if entry.get_key().to_vec() < key {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        self.get_dir_entry_index(right as u16, page_size).get_page_no()
    }


    pub fn add_entries(&mut self, mut entries: Vec<TreeDirEntry>, page_size: usize) -> () {
        // This only makes sense if the page has entries
        assert!(self.get_entries() > 0);
        // Entries should not be empty
        assert!(!entries.is_empty());

        if entries.get(0).unwrap().get_key() < self.get_dir_entry_index(0, page_size).get_key() {
            self.set_page_to_left(entries.get(0).unwrap().get_page_no());
            // remove is suboptimal
            entries.remove(0);
        }

        // TODO - wildly sub optimal
        for entry in entries {
            self.store_tree_dir_in_page(entry, page_size);
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_set_entries() {
        let mut page = TreeInternalPage::new(4096, 45, 567);
        
        assert!(0 == page.get_entries());
        page.set_entries(79);
        assert!(79 == page.get_entries());
        
        assert!(4096 - 28 == page.get_free_space());
        page.set_free_space(179);
        assert!(179 == page.get_free_space());
        
        assert!(0 == page.get_parent_page());
        page.set_parent_page(2179);
        assert!(2179 == page.get_parent_page());

        assert!(0 == page.get_page_to_left());
        page.set_page_to_left(32179);
        assert!(32179 == page.get_page_to_left());

    }


     #[test]
    fn test_add_entries() {
        let mut page = TreeInternalPage::new(4096, 45, 567);
        
        let table_dir_entry1 = TreeDirEntry::new(b"mmk".to_vec(), 74);
        page.store_tree_dir_in_page(table_dir_entry1, 4096);

        let table_dir_entry2 = TreeDirEntry::new(b"bob".to_vec(), 78);
        page.store_tree_dir_in_page(table_dir_entry2, 4096);

        let entries = page.get_all_dir_entries(4096);
        assert!(2 == entries.len());
        assert!(b"bob".to_vec() == entries.get(0).unwrap().get_key());
        assert!(b"mmk".to_vec() == entries.get(1).unwrap().get_key());
    }


    #[test]
    fn test_add_entry() {
        let mut page = TreeInternalPage::new(4096, 45, 567);
        
    
        page.add_page_entry(56, b"mmk".to_vec(),78, 4096);
        assert!(page.get_page_to_left() == 56);

        page.add_page_entry(756, b"bob".to_vec(),778, 4096);

        let entries = page.get_all_dir_entries(4096);
        assert!(2 == entries.len());
        assert!(b"bob".to_vec() == entries.get(0).unwrap().get_key());
        assert!(b"mmk".to_vec() == entries.get(1).unwrap().get_key());

        assert!(page.get_page_to_left() == 756);
    }


    #[test]
    fn test_get_entry() {
        let mut page = TreeInternalPage::new(4096, 45, 567);
        
    
        page.add_page_entry(56, b"mmk".to_vec(),78, 4096);
        page.add_page_entry(756, b"bob".to_vec(),778, 4096);
        page.add_page_entry(85, b"tom".to_vec(),844, 4096);

        let entries = page.get_all_dir_entries(4096);
        assert!(3 == entries.len());
        assert!(b"bob".to_vec() == entries.get(0).unwrap().get_key());
        assert!(b"mmk".to_vec() == entries.get(1).unwrap().get_key());
        assert!(b"tom".to_vec() == entries.get(2).unwrap().get_key());

        assert!(page.get_next_page(b"bob".to_vec(), 4096) == 778);
        assert!(page.get_next_page(b"a".to_vec(), 4096) == 756);
        assert!(page.get_next_page(b"zztop".to_vec(), 4096) == 844);

        assert!(page.get_next_page(b"cat".to_vec(), 4096) == 778);
        assert!(page.get_next_page(b"ppp".to_vec(), 4096) == 78);     

        assert!(page.get_next_page(b"mmk".to_vec(), 4096) == 78);
        assert!(page.get_next_page(b"tom".to_vec(), 4096) == 844);
        

        assert!(page.get_page_to_left() == 756);
    }
}
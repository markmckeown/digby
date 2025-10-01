use crate::page::{Page, PageTrait, PageType};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use crate::tuple::Tuple;
use crate::tuple::TupleTrait;

// TreeLeafPage structure
//
// Header is 20 bytes:
// | Checksum(u32) | Page No (u32) | VersionHolder(8 bytes) | Entries(u16) | Free_Space(u16) | 
//
// TreeLeafPage body is of the format:
//
// | Header | Tuple | Tuple  | ... Free Space ... | Index to Tuple | Index to Type | End Of Page |
// |--------|-------|--------|--------------------|----------------|---------------|-------------|
// Tuples grow down the Page, while the Tuple Index grows up the page - with the free space in between.
//
pub struct TreeLeafPage {
    page: Page
}

impl PageTrait for TreeLeafPage {
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

impl TreeLeafPage {
    // Create a new DataPage with given page size and page number.
    // This is used when creating a page to add to the DB.
    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut page = Page::new(page_size);
        page.set_type(PageType::TreeLeaf);
        page.set_page_number(page_number);      
        let mut data_page = TreeLeafPage { page };
        data_page.set_entries(0);
        data_page.set_free_space((page_size - 20) as u16); // 20 bytes for header
        data_page
    }

    // Create a DataPage from some bytes, ie read from disk.
     pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    // Create a DataPage from a Page - read bytes from disk,
    // determine it is a DataPage, and wrap it.
    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::TreeLeaf 
        && page.get_type() != PageType::TableDir 
        && page.get_type() != PageType::TreeRootSingle {
            panic!("Page type is not TreeLeaf or TableDir or TreeRootSingle");
        }
        TreeLeafPage { page }
    }

    pub fn make_table_dir_page(&mut self) {
        self.page.set_type(PageType::TableDir)
    }

    pub fn make_tree_root_single_page(&mut self) {
        self.page.set_type(PageType::TreeRootSingle)
    }

    fn get_entries(&self) -> u16 {
        let mut cursor = Cursor::new(&self.page.get_bytes()[..]);
        cursor.set_position(16);
        cursor.read_u16::<byteorder::LittleEndian>().unwrap()
    }

    fn set_entries(&mut self, entries: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(16);
        cursor.write_u16::<byteorder::LittleEndian>(entries).expect("Failed to write entries");
    }

    fn get_free_space(&self) -> u16 {
        let mut cursor = Cursor::new(&self.page.get_bytes()[..]);
        cursor.set_position(18);
        cursor.read_u16::<byteorder::LittleEndian>().unwrap()
    }

    fn set_free_space(&mut self, free_space: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(18);
        cursor.write_u16::<byteorder::LittleEndian>(free_space).expect("Failed to write free space");
    }

    pub fn can_fit(&self, size: usize) -> bool {
        let free_space: usize = self.get_free_space() as usize;
        free_space >= size + 2
    }

    // Add a tuple to the DataPage. 
    // Will crash if not enough space.
    // This is low level API. Use store_tuple to add or update a tuple.
    fn add_tuple(&mut self, tuple: &Tuple, page_size: u64) -> () {
        let tuple_size: usize = tuple.get_byte_size();
        assert!(self.can_fit(tuple_size), "Cannot add Tuple to page, not enough space.");
            
        let current_entries = self.get_entries();
        let current_entries_size: usize = current_entries as usize * 2; // Each entry has 2 bytes for index
        let free_space = self.get_free_space();


        let tuple_offset : usize = (page_size as usize) - (free_space as usize + current_entries_size);
        let page_bytes = self.page.get_bytes_mut();
        page_bytes[tuple_offset..tuple_offset + tuple_size as usize].copy_from_slice(tuple.get_serialized());

        let mut cursor = Cursor::new(&mut page_bytes[page_size as usize - (current_entries_size + 2 as usize)..]);
        cursor.write_u16::<byteorder::LittleEndian>(tuple_offset as u16).expect("Failed to write tuple offset");
        self.set_entries(current_entries + 1);
        self.set_free_space(free_space - (tuple_size as u16 + 2));
    }

    // Get tuple at index, used as part of binary search.
    // Crashes if index is out of bounds.
    fn get_tuple_index(&self, index: u16, page_size: usize) -> Tuple {
        let entries = self.get_entries();

        assert!(index < entries);

        let current_entries_size: usize = entries as usize * 2; // Each entry has 2 bytes for index
        let mut cursor = Cursor::new(&self.page.get_bytes()[page_size - current_entries_size..]);
        cursor.set_position((index as u64) * 2);
        let tuple_offset = cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;

        let mut tuple_cursor = Cursor::new(&self.page.get_bytes()[tuple_offset..]);
        let key_len = tuple_cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;
        let value_len = tuple_cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;
        let tuple_size = key_len + value_len + 8 + 2 + 2; // key + value + version + key_len + value_len

        Tuple::from_bytes(self.page.get_bytes()[tuple_offset..tuple_offset + tuple_size].to_vec())
    }

    // Store a tuple in the DataPage. If a tuple with the same key exists, it is replaced.
    // Tuples are kept in sorted order by key.
    // Get all tuples in page, remove any with same key, add new tuple, sort, 
    // clear page and re-add all tuples.
    // If tuple does not fit then crash.
    pub fn store_tuple(&mut self, new_tuple: Tuple, page_size: usize) -> () {
        let tuple_size: usize = new_tuple.get_byte_size();
        assert!(self.can_fit(tuple_size), "Cannot fit tuple in page");
    

        let sorted_tuples = self.build_sorted_tuples(new_tuple, page_size);
        // Clear the page and re-add all tuples
        self.set_entries(0);
        self.set_free_space((page_size - 20) as u16); // Reset free space

        for tuple in sorted_tuples {
            self.add_tuple(&tuple, page_size as u64);
        }
    }

    // Part of store_tuple - get all tuples, remove any with same key as new_tuple,
    // add new_tuple, sort and return.
    fn build_sorted_tuples(&self, new_tuple: Tuple, page_size: usize) -> Vec<Tuple> {
        let mut tuples = self.get_all_tuples(page_size);
        // Remove any existing tuple with the same key
        tuples.retain(|t| t.get_key() != new_tuple.get_key());
        tuples.push(new_tuple);
        tuples.sort_by(|b, a| a.get_key().cmp(b.get_key()));
        tuples
    }


    // Get all tuples in the DataPage - used for rebuilding the page when adding or updating a tuple.
    pub fn get_all_tuples(&self, page_size: usize) -> Vec<Tuple> {
        let entries = self.get_entries();
        let mut tuples = Vec::new();
        for i in 0..entries {
            let tuple = self.get_tuple_index(i, page_size);
            tuples.push(tuple);
        }
        tuples
    }

    // Get a tuple by key using binary search. Returns None if not found.
    pub fn get_tuple(&self, key: Vec<u8>, page_size: usize) -> Option<Tuple> {
        let entries = self.get_entries();
        let mut left = 0;
        let mut right = entries as i32 - 1;

        while left <= right {
            let mid = left + (right - left) / 2;
            let tuple: Tuple = self.get_tuple_index(mid as u16, page_size);
            if tuple.get_key() == key {
                return Some(tuple);
            } else if tuple.get_key().to_vec() < key {
                left = mid + 1;
            } else {
                    right = mid - 1;
                }
            
        }
        None
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_page() {
        let mut data_page = TreeLeafPage::new(4096, 1);
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let version = 1;

        let tuple = Tuple::new(key, value, version);
        data_page.add_tuple(&tuple, 4096);
        assert_eq!(data_page.get_entries(), 1);
        let retrieved_tuple = data_page.get_tuple_index(0, 4096);
        assert_eq!(retrieved_tuple.get_key(), b"key");
        assert_eq!(retrieved_tuple.get_value(), b"value");
        assert_eq!(retrieved_tuple.get_version(), 1);
    }

    #[test]
    fn test_get_tuple() {
        let mut data_page = TreeLeafPage::new(4096, 1);
        

        data_page.store_tuple(Tuple::new(b"a".to_vec(), b"value-a".to_vec(), 1), 4096);
        data_page.store_tuple(Tuple::new(b"b".to_vec(), b"value-b".to_vec(), 2), 4096);
        data_page.store_tuple(Tuple::new(b"c".to_vec(), b"value-c".to_vec(), 3), 4096);
        data_page.store_tuple(Tuple::new(b"d".to_vec(), b"value-d".to_vec(), 4), 4096);
        data_page.store_tuple(Tuple::new(b"e".to_vec(), b"value-e".to_vec(), 5), 4096);

        assert_eq!(data_page.get_entries(), 5);
        let key_to_find = b"a".to_vec();
        let retrieved_tuple = data_page.get_tuple(key_to_find, 4096).unwrap();
        assert_eq!(retrieved_tuple.get_key(), b"a");
        assert_eq!(retrieved_tuple.get_value(), b"value-a");
        assert_eq!(retrieved_tuple.get_version(), 1);

        let missing_key = b"missing".to_vec();
        assert!(data_page.get_tuple(missing_key, 4096).is_none());
    }
}
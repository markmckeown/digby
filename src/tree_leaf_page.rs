use crate::page::{Page, PageTrait, PageType};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use crate::tuple::Tuple;

// TreeLeafPage structure
//
// Header is 20 bytes:
// | Checksum(u32) | Page No (u32) | Version (u64) | Type(u8) | Entries(u8) | Free_Space(u16) | 
//
// TreeLeafPage body is of the format:
//
// | Header | Tuple | Tuple  | ... Free Space ... | Index to Tuple | Index to Type | End Of Page |
// |--------|-------|--------|--------------------|----------------|---------------|-------------|
// Tuples grow down the Page, while the Tuple Index grows up the page - with the free space in between.
//
// Note we can only have up to 255 entries in a TreeLeafPage, as Entries is a u8. A tuple is at least
// 16 bytes - 4 bytes key length, 4 bytes value length, 8 bytes version.
// 16 * 255 = 4080 + 12 bytes header + 510 bytes index = 4602 bytes - so we will have less than 255
// tuples in a 4KB page as there is not enough space for 255 tuples and their indexes.
// We do not need to check entries for overflow as we check if there is enough space in the page before adding a tuple.
pub struct TreeLeafPage {
    page: Page
}

impl PageTrait for TreeLeafPage {
    fn get_bytes(&self) -> &[u8] {
        self.page.get_bytes()
    }

    fn get_page_number(&mut self) -> u32 {
        self.page.get_page_number()
    }

    fn get_page(&mut self) -> &mut Page {
        &mut self.page       
    }

    fn get_version(&mut self) -> u64 {
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
        data_page.set_free_space((page_size - 20) as u16); // 12 bytes for header
        data_page
    }

    // Create a DataPage from some bytes, ie read from disk.
     pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    // Create a DataPage from a Page - read bytes from disk,
    // determine it is a DataPage, and wrap it.
    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::TreeLeaf {
            panic!("Page type is not Data");
        }
        TreeLeafPage { page }
    }

    fn get_entries(&mut self) -> u8 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(17);
        cursor.read_u8().unwrap()
    }

    fn set_entries(&mut self, entries: u8) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(17);
        cursor.write_u8(entries).expect("Failed to write entries");
    }

    fn get_free_space(&mut self) -> u16 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(18);
        cursor.read_u16::<byteorder::LittleEndian>().unwrap()
    }

    fn set_free_space(&mut self, free_space: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(18);
        cursor.write_u16::<byteorder::LittleEndian>(free_space).expect("Failed to write free space");
    }

    fn can_fit(&mut self, size: usize) -> bool {
        let free_space: usize = self.get_free_space() as usize;
        free_space >= size + 2
    }

    // Add a tuple to the DataPage. Returns an error if there is not enough space.
    // This is low level API. Use store_tuple to add or update a tuple.
    fn add_tuple(&mut self, tuple: &Tuple, page_size: u64) -> Result<(), String> {
        let tuple_size: usize = tuple.get_size();
        if !self.can_fit(tuple_size) {
            return Err("Not enough space in DataPage".to_string());
        }

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
        
        Ok(())
    }

    // Get tuple at index, used as part of binary search.
    // Crashes if index is out of bounds.
    fn get_tuple_index(&mut self, index: u8, page_size: usize) -> Tuple {
        let entries = self.get_entries();

        assert!(index < entries);

        let current_entries_size: usize = entries as usize * 2; // Each entry has 2 bytes for index
        let mut cursor = Cursor::new(&self.page.get_bytes()[page_size - current_entries_size..]);
        cursor.set_position((index as u64) * 2);
        let tuple_offset = cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;

        let mut tuple_cursor = Cursor::new(&self.page.get_bytes()[tuple_offset..]);
        let key_len = tuple_cursor.read_u32::<byteorder::LittleEndian>().unwrap() as usize;
        let value_len = tuple_cursor.read_u32::<byteorder::LittleEndian>().unwrap() as usize;
        let tuple_size = key_len + value_len + 8 + 4 + 4 + 1; // key + value + version + key_len + value_len + overflow

        Tuple::from_bytes(self.page.get_bytes()[tuple_offset..tuple_offset + tuple_size].to_vec())
    }

    // Store a tuple in the DataPage. If a tuple with the same key exists, it is replaced.
    // Tuples are kept in sorted order by key.
    // Get all tuples in page, remove any with same key, add new tuple, sort, 
    // clear page and re-add all tuples.
    pub fn store_tuple(&mut self, new_tuple: Tuple, page_size: usize) -> Result<(), String> {
        let tuple_size: usize = new_tuple.get_size();
        if !self.can_fit(tuple_size) {
            return Err("Not enough space in DataPage".to_string());
        }

        let sorted_tuples = self.build_sorted_tuples(new_tuple, page_size);
        // Clear the page and re-add all tuples
        self.set_entries(0);
        self.set_free_space((page_size - 20) as u16); // Reset free space

        for tuple in sorted_tuples {
            self.add_tuple(&tuple, page_size as u64)?;
        }
        
        Ok(())
    }



    // Part of store_tuple - get all tuples, remove any with same key as new_tuple,
    // add new_tuple, sort and return.
    fn build_sorted_tuples(&mut self, new_tuple: Tuple, page_size: usize) -> Vec<Tuple> {
        let mut tuples = self.get_all_tuples(page_size);
        // Remove any existing tuple with the same key
        tuples.retain(|t| t.get_key() != new_tuple.get_key());
        tuples.push(new_tuple);
        tuples.sort_by(|a, b| a.get_key().cmp(b.get_key()));
        tuples
    }


    // Get all tuples in the DataPage - used for rebuilding the page when adding or updating a tuple.
    fn get_all_tuples(&mut self, page_size: usize) -> Vec<Tuple> {
        let entries = self.get_entries();
        let mut tuples = Vec::new();
        for i in 0..entries {
            let tuple = self.get_tuple_index(i, page_size);
            tuples.push(tuple);
        }
        tuples
    }

    // Get a tuple by key using binary search. Returns None if not found.
    pub fn get_tuple(&mut self, key: Vec<u8>, page_size: usize) -> Option<Tuple> {
        let entries = self.get_entries();
        let mut left = 0;
        let mut right = entries as i32 - 1;

        while left <= right {
            let mid = left + (right - left) / 2;
            let tuple: Tuple = self.get_tuple_index(mid as u8, page_size);
            if tuple.get_key() == key {
                return Some(tuple);
            } else if tuple.get_key().to_vec() > key {
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
        assert!(data_page.add_tuple(&tuple, 4096).is_ok());
        assert_eq!(data_page.get_entries(), 1);
        let retrieved_tuple = data_page.get_tuple_index(0, 4096);
        assert_eq!(retrieved_tuple.get_key(), b"key");
        assert_eq!(retrieved_tuple.get_value(), b"value");
        assert_eq!(retrieved_tuple.get_version(), 1);
    }

    #[test]
    fn test_get_tuple() {
        let mut data_page = TreeLeafPage::new(4096, 1);
        

        assert!(data_page.store_tuple(Tuple::new(b"a".to_vec(), b"value-a".to_vec(), 1), 4096).is_ok());
        assert!(data_page.store_tuple(Tuple::new(b"b".to_vec(), b"value-b".to_vec(), 2), 4096).is_ok());
        assert!(data_page.store_tuple(Tuple::new(b"c".to_vec(), b"value-c".to_vec(), 3), 4096).is_ok());
        assert!(data_page.store_tuple(Tuple::new(b"d".to_vec(), b"value-d".to_vec(), 4), 4096).is_ok());
        assert!(data_page.store_tuple(Tuple::new(b"e".to_vec(), b"value-e".to_vec(), 5), 4096).is_ok());

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
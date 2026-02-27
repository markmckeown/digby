use crate::block_layer::PageConfig;
use crate::page::{Page, PageTrait, PageType};
use crate::tuple::Tuple;
use crate::tuple::TupleTrait;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

// TreeLeafPage structure
//
// Header is 20 bytes:
// | Page No (8 bytes) | VersionHolder(8 bytes) | Entries(u16) | Free_Space(u16) |
//
// TreeLeafPage body is of the format:
//
// | Header | Tuple | Tuple  | ... Free Space ... | Index to Tuple | Index to Type | End Of Page |
// |--------|-------|--------|--------------------|----------------|---------------|-------------|
// Tuples grow down the Page, while the Tuple Index grows up the page - with the free space in between.
//
pub struct TreeLeafPage {
    page: Page,
}

impl PageTrait for TreeLeafPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> u64 {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: u64) -> () {
        self.page.set_page_number(page_no)
    }

    fn get_page(&mut self) -> &mut Page {
        &mut self.page
    }

    fn get_version(&self) -> u64 {
        self.page.get_version()
    }

    fn set_version(&mut self, version: u64) -> () {
        self.page.set_version(version);
    }
}

impl TreeLeafPage {
    const HEADER_SIZE: usize = 20;

    pub fn create_new(page_config: &PageConfig, page_number: u64) -> Self {
        TreeLeafPage::new(page_config.block_size, page_config.page_size, page_number)
    }

    // Create a new DataPage with given page size and page number.
    // This is used when creating a page to add to the DB.
    fn new(block_size: usize, page_size: usize, page_number: u64) -> Self {
        let mut page = Page::new(block_size, page_size);
        page.set_type(PageType::TreeLeaf);
        page.set_page_number(page_number);
        let mut data_page = TreeLeafPage { page };
        data_page.set_entries(0);
        data_page.set_free_space((page_size - TreeLeafPage::HEADER_SIZE) as u16);
        data_page
    }

    // Create a TreeLeafPage from a Page - read bytes from disk,
    // determine it is a TreeLeafPage, and wrap it.
    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::TreeLeaf {
            panic!("Page type is not TreeLeaf");
        }
        TreeLeafPage { page }
    }

    pub fn is_empty(&self) -> bool {
        return self.get_entries() == 0;
    }

    fn get_entries(&self) -> u16 {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(16);
        cursor.read_u16::<byteorder::LittleEndian>().unwrap()
    }

    fn set_entries(&mut self, entries: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(16);
        cursor
            .write_u16::<byteorder::LittleEndian>(entries)
            .expect("Failed to write entries");
    }

    fn get_free_space(&self) -> u16 {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(18);
        cursor.read_u16::<byteorder::LittleEndian>().unwrap()
    }

    fn set_free_space(&mut self, free_space: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(18);
        cursor
            .write_u16::<byteorder::LittleEndian>(free_space)
            .expect("Failed to write free space");
    }

    pub fn can_fit(&self, size: usize) -> bool {
        let free_space: usize = self.get_free_space() as usize;
        free_space >= size + 2
    }

    // Add a tuple to the DataPage.
    // Will crash if not enough space.
    // This is low level API. Use store_tuple to add or update a tuple.
    fn add_tuple(&mut self, tuple: &Tuple) -> () {
        let page_size = self.page.page_size;
        let tuple_size: usize = tuple.get_byte_size();
        assert!(
            self.can_fit(tuple_size),
            "Cannot add Tuple to page, not enough space."
        );

        let current_entries = self.get_entries();
        let current_entries_size: usize = current_entries as usize * 2; // Each entry has 2 bytes for index
        let free_space = self.get_free_space();

        let tuple_offset: usize =
            (page_size as usize) - (free_space as usize + current_entries_size);
        let page_bytes = self.page.get_page_bytes_mut();
        page_bytes[tuple_offset..tuple_offset + tuple_size as usize]
            .copy_from_slice(tuple.get_serialized());

        let mut cursor = Cursor::new(
            &mut page_bytes[page_size as usize - (current_entries_size + 2 as usize)..],
        );
        cursor
            .write_u16::<byteorder::LittleEndian>(tuple_offset as u16)
            .expect("Failed to write tuple offset");
        self.set_entries(current_entries + 1);
        self.set_free_space(free_space - (tuple_size as u16 + 2));
    }

    // Get tuple at index, used as part of binary search.
    // Crashes if index is out of bounds.
    fn get_tuple_index(&self, index: u16) -> Tuple {
        let page_size = self.page.page_size;
        let entries = self.get_entries();

        assert!(index < entries);

        let offset = (index * 2) + 2;
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[page_size - offset as usize..]);
        let tuple_offset = cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;

        let mut tuple_cursor = Cursor::new(&self.page.get_page_bytes()[tuple_offset..]);
        let key_len = tuple_cursor.read_u8().unwrap() as usize;
        let value_len = tuple_cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;
        let tuple_size = key_len + value_len + 8 + 2 + 1; // key + value + version + key_len + value_len

        Tuple::from_bytes(
            self.page.get_page_bytes()[tuple_offset..tuple_offset + tuple_size].to_vec(),
        )
    }

    // Store a tuple in the DataPage. If a tuple with the same key exists, it is replaced.
    // Tuples are kept in sorted order by key.
    // Get all tuples in page, remove any with same key, add new tuple, sort,
    // clear page and re-add all tuples.
    // If tuple does not fit then crash.
    pub fn store_tuple(&mut self, new_tuple: Tuple) -> () {
        let tuple_size: usize = new_tuple.get_byte_size();
        let page_size = self.page.page_size;
        assert!(self.can_fit(tuple_size), "Cannot fit tuple in page");

        let sorted_tuples = self.build_sorted_tuples(new_tuple);
        // Clear the page and re-add all tuples
        self.set_entries(0);
        self.set_free_space((page_size - 20) as u16); // Reset free space

        for tuple in sorted_tuples {
            self.add_tuple(&tuple);
        }
    }

    pub fn add_sorted_tuples(&mut self, sorted_tuples: &mut Vec<Tuple>) {
        for tuple in sorted_tuples {
            self.add_tuple(&tuple);
        }
    }

    // Part of store_tuple - get all tuples, remove any with same key as new_tuple,
    // add new_tuple, sort and return.
    fn build_sorted_tuples(&self, new_tuple: Tuple) -> Vec<Tuple> {
        let mut tuples = self.get_all_tuples();
        // Remove any existing tuple with the same key
        tuples.retain(|t| t.get_key() != new_tuple.get_key());
        tuples.push(new_tuple);
        tuples.sort_by(|b, a| b.get_key().cmp(a.get_key()));
        tuples
    }

    pub fn get_right_half_tuples(&mut self) -> Vec<Tuple> {
        let entries = self.get_entries();
        let start = (entries + 1) / 2;
        let mut tuples = Vec::new();
        let mut free_space = self.get_free_space();
        for i in start..entries {
            let tuple = self.get_tuple_index(i);
            free_space += tuple.get_byte_size() as u16 + 2;
            tuples.push(tuple);
        }
        self.set_free_space(free_space);
        self.set_entries(start);
        tuples
    }

    // Get all tuples in the DataPage - used for rebuilding the page when adding or updating a tuple.
    pub fn get_all_tuples(&self) -> Vec<Tuple> {
        let entries = self.get_entries();
        let mut tuples = Vec::new();
        for i in 0..entries {
            let tuple = self.get_tuple_index(i);
            tuples.push(tuple);
        }
        tuples
    }

    // Get a tuple by key using binary search. Returns None if not found.
    pub fn get_tuple(&self, key: &Vec<u8>) -> Option<Tuple> {
        let entries = self.get_entries();
        let mut left = 0;
        let mut right = entries as i32 - 1;

        while left <= right {
            let mid = left + (right - left) / 2;
            let tuple: Tuple = self.get_tuple_index(mid as u16);
            if tuple.get_key() == key {
                return Some(tuple);
            } else if tuple.get_key().to_vec() < *key {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        None
    }

    pub fn get_left_key(&self) -> Option<Vec<u8>> {
        if self.get_entries() == 0 {
            return None;
        }
        Some(self.get_tuple_index(0).get_key().to_vec())
    }

    pub fn delete_key(&mut self, key: &Vec<u8>) -> Option<Tuple> {
        let tuple = self.get_tuple(key);
        if tuple.is_none() {
            return None;
        }
        let page_size = self.page.page_size;
        let mut tuples = self.get_all_tuples();
        // Remove any existing tuple with the same key
        tuples.retain(|t| t.get_key() != key);
        self.set_entries(0);
        self.set_free_space((page_size - 20) as u16); // Reset free space

        // Could probably do this with a memmove
        for tuple in tuples {
            self.add_tuple(&tuple);
        }
        return tuple;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_page() {
        let mut data_page = TreeLeafPage::new(4096, 4096, 1);
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let version = 1;

        let tuple = Tuple::new(&key, &value, version);
        data_page.add_tuple(&tuple);
        assert_eq!(data_page.get_entries(), 1);
        let retrieved_tuple = data_page.get_tuple_index(0);
        assert_eq!(retrieved_tuple.get_key(), b"key");
        assert_eq!(retrieved_tuple.get_value(), b"value");
        assert_eq!(retrieved_tuple.get_version(), 1);
    }

    #[test]
    fn test_get_tuple() {
        let mut data_page = TreeLeafPage::new(4096, 4096, 1);

        data_page.store_tuple(Tuple::new(
            b"a".to_vec().as_ref(),
            b"value-a".to_vec().as_ref(),
            1,
        ));
        data_page.store_tuple(Tuple::new(
            b"b".to_vec().as_ref(),
            b"value-b".to_vec().as_ref(),
            2,
        ));
        data_page.store_tuple(Tuple::new(
            b"c".to_vec().as_ref(),
            b"value-c".to_vec().as_ref(),
            3,
        ));
        data_page.store_tuple(Tuple::new(
            b"d".to_vec().as_ref(),
            b"value-d".to_vec().as_ref(),
            4,
        ));
        data_page.store_tuple(Tuple::new(
            b"e".to_vec().as_ref(),
            b"value-e".to_vec().as_ref(),
            5,
        ));

        assert_eq!(data_page.get_entries(), 5);
        let key_to_find = b"a".to_vec();
        let retrieved_tuple = data_page.get_tuple(&key_to_find).unwrap();
        assert_eq!(retrieved_tuple.get_key(), b"a");
        assert_eq!(retrieved_tuple.get_value(), b"value-a");
        assert_eq!(retrieved_tuple.get_version(), 1);

        let key_to_find = b"b".to_vec();
        let retrieved_tuple = data_page.get_tuple(&key_to_find).unwrap();
        assert_eq!(retrieved_tuple.get_key(), b"b");
        assert_eq!(retrieved_tuple.get_value(), b"value-b");
        assert_eq!(retrieved_tuple.get_version(), 2);

        let key_to_find = b"c".to_vec();
        let retrieved_tuple = data_page.get_tuple(&key_to_find).unwrap();
        assert_eq!(retrieved_tuple.get_key(), b"c");
        assert_eq!(retrieved_tuple.get_value(), b"value-c");
        assert_eq!(retrieved_tuple.get_version(), 3);

        let key_to_find = b"d".to_vec();
        let retrieved_tuple = data_page.get_tuple(&key_to_find).unwrap();
        assert_eq!(retrieved_tuple.get_key(), b"d");
        assert_eq!(retrieved_tuple.get_value(), b"value-d");
        assert_eq!(retrieved_tuple.get_version(), 4);

        let key_to_find = b"e".to_vec();
        let retrieved_tuple = data_page.get_tuple(&key_to_find).unwrap();
        assert_eq!(retrieved_tuple.get_key(), b"e");
        assert_eq!(retrieved_tuple.get_value(), b"value-e");
        assert_eq!(retrieved_tuple.get_version(), 5);

        let missing_key = b"missing".to_vec();
        assert!(data_page.get_tuple(&missing_key).is_none());
    }

    #[test]
    fn test_get_right_half() {
        let mut data_page = TreeLeafPage::new(4096, 4096, 1);

        data_page.store_tuple(Tuple::new(
            b"a".to_vec().as_ref(),
            b"value-a".to_vec().as_ref(),
            1,
        ));
        let tuples: Vec<Tuple> = data_page.get_right_half_tuples();
        assert!(tuples.is_empty());
        assert!(data_page.get_entries() == 1);
        data_page.get_tuple(&b"a".to_vec()).unwrap();

        data_page.store_tuple(Tuple::new(
            b"b".to_vec().as_ref(),
            b"value-b".to_vec().as_ref(),
            2,
        ));
        data_page.get_tuple(&b"a".to_vec()).unwrap();
        let tuples: Vec<Tuple> = data_page.get_right_half_tuples();
        assert!(tuples.len() == 1);
        assert!(tuples.last().unwrap().get_key() == b"b".to_vec());
        assert!(data_page.get_entries() == 1);
        data_page.get_tuple(&b"a".to_vec()).unwrap();

        data_page.store_tuple(Tuple::new(
            b"b".to_vec().as_ref(),
            b"value-b".to_vec().as_ref(),
            2,
        ));
        assert!(data_page.get_entries() == 2);
        data_page.get_tuple(&b"a".to_vec()).unwrap();
        data_page.get_tuple(&b"b".to_vec()).unwrap();
        data_page.store_tuple(Tuple::new(
            b"c".to_vec().as_ref(),
            b"value-c".to_vec().as_ref(),
            3,
        ));
        assert!(data_page.get_entries() == 3);
        let tuples: Vec<Tuple> = data_page.get_right_half_tuples();
        assert!(tuples.len() == 1);
        assert!(tuples.get(0).unwrap().get_key() == b"c".to_vec());
        assert!(data_page.get_entries() == 2);
        data_page.get_tuple(&b"a".to_vec()).unwrap();
        data_page.get_tuple(&b"b".to_vec()).unwrap();
    }

    #[test]
    fn test_delete_tuple() {
        let mut data_page = TreeLeafPage::new(4096, 4092, 1);

        assert!(data_page.get_left_key().is_none());

        data_page.store_tuple(Tuple::new(
            b"a".to_vec().as_ref(),
            b"value-a".to_vec().as_ref(),
            1,
        ));
        data_page.store_tuple(Tuple::new(
            b"b".to_vec().as_ref(),
            b"value-b".to_vec().as_ref(),
            2,
        ));
        data_page.store_tuple(Tuple::new(
            b"c".to_vec().as_ref(),
            b"value-c".to_vec().as_ref(),
            3,
        ));
        data_page.store_tuple(Tuple::new(
            b"d".to_vec().as_ref(),
            b"value-d".to_vec().as_ref(),
            4,
        ));
        data_page.store_tuple(Tuple::new(
            b"e".to_vec().as_ref(),
            b"value-e".to_vec().as_ref(),
            5,
        ));

        data_page.get_tuple(&b"a".to_vec()).unwrap();
        assert_eq!(b"a".to_vec(), data_page.get_left_key().unwrap());
        data_page.get_tuple(&b"b".to_vec()).unwrap();
        data_page.get_tuple(&b"c".to_vec()).unwrap();
        data_page.get_tuple(&b"d".to_vec()).unwrap();
        data_page.get_tuple(&b"e".to_vec()).unwrap();

        data_page.delete_key(&b"c".to_vec());
        assert!(data_page.get_tuple(&b"c".to_vec()).is_none());
        data_page.get_tuple(&b"a".to_vec()).unwrap();
        data_page.get_tuple(&b"e".to_vec()).unwrap();

        data_page.delete_key(&b"a".to_vec());
        data_page.get_tuple(&b"b".to_vec()).unwrap();
        data_page.get_tuple(&b"b".to_vec()).unwrap();
        data_page.get_tuple(&b"d".to_vec()).unwrap();
        data_page.get_tuple(&b"e".to_vec()).unwrap();

        data_page.delete_key(&b"e".to_vec());
        assert!(data_page.get_tuple(&b"e".to_vec()).is_none());

        data_page.delete_key(&b"b".to_vec());
        data_page.delete_key(&b"d".to_vec());
        assert!(data_page.get_tuple(&b"b".to_vec()).is_none());
        assert!(data_page.get_tuple(&b"d".to_vec()).is_none());
    }
}

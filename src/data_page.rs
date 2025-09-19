use crate::page::{Page, PageTrait, PageType};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use crate::tuple::Tuple;

// DataPage structure
//
// Header is 12 bytes:
// | Checksum(u32) | Page No (u32)| Type(u8) | Entries (u8) | Free_Space (u16) | 
//
// DataPage body is of the format:
//
// | Header | Tuple | Tuple  | ... Free Space ... | Index to Tuple | Index to Type | End Of Page |
// |--------|-------|--------|--------------------|----------------|---------------|-------------|
// Tuples grow down the Page, while the Tuple Index grows up the page - with the free space in between.
//
// Note we can only have up to 255 entries in a DataPage, as Entries is a u8. A tuple is at least
// 16 bytes - 4 bytes key length, 4 bytes value length, 8 bytes version.
// 16 * 255 = 4080 + 12 bytes header + 510 bytes index = 4602 bytes - so we will have less than 255
// tuples in a 4KB page as there is not enough space for 255 tuples and their indexes.
// We do not need to check entries for overflow as we check if there is enough space in the page before adding a tuple.
pub struct DataPage {
    page: Page
}

impl PageTrait for DataPage {
    fn get_bytes(&self) -> &[u8] {
        self.page.get_bytes()
    }

    fn get_page_number(&mut self) -> u32 {
        self.page.get_page_number()
    }

    fn get_page(&mut self) -> &mut Page {
        &mut self.page       
    }
}

impl DataPage {
    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut page = Page::new(page_size);
        page.set_type(PageType::Data);
        page.set_page_number(page_number);      
        let mut data_page = DataPage { page };
        data_page.set_entries(0);
        data_page.set_free_space((page_size - 12) as u16); // 12 bytes for header
        data_page
    }

     pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::Data {
            panic!("Page type is not Data");
        }
        DataPage { page }
    }

    pub fn get_entries(&mut self) -> u8 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(9);
        cursor.read_u8().unwrap()
    }

    pub fn set_entries(&mut self, entries: u8) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(9);
        cursor.write_u8(entries).expect("Failed to write entries");
    }

    pub fn get_free_space(&mut self) -> u16 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(10);
        cursor.read_u16::<byteorder::LittleEndian>().unwrap()
    }

    pub fn set_free_space(&mut self, free_space: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(10);
        cursor.write_u16::<byteorder::LittleEndian>(free_space).expect("Failed to write free space");
    }

    pub fn can_fit(&mut self, size: usize) -> bool {
        let free_space: usize = self.get_free_space() as usize;
        free_space >= size + 2
    }

    pub fn add_tuple_base(&mut self, tuple: &Tuple, page_size: u64) -> Result<(), String> {
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

    pub fn get_tuple_index(&mut self, index: u8, page_size: usize) -> Option<Tuple> {
        let entries = self.get_entries();
        if index >= entries {
            return None;
        }

        let current_entries_size: usize = entries as usize * 2; // Each entry has 2 bytes for index
        let mut cursor = Cursor::new(&self.page.get_bytes()[page_size - current_entries_size..]);
        cursor.set_position((index as u64) * 2);
        let tuple_offset = cursor.read_u16::<byteorder::LittleEndian>().unwrap() as usize;

        let mut tuple_cursor = Cursor::new(&self.page.get_bytes()[tuple_offset..]);
        let key_len = tuple_cursor.read_u32::<byteorder::LittleEndian>().unwrap() as usize;
        let value_len = tuple_cursor.read_u32::<byteorder::LittleEndian>().unwrap() as usize;
        let tuple_size = key_len + value_len + 8 + 4 + 4; // key + value + version + key_len + value_len
        

        Some(Tuple::from_bytes(self.page.get_bytes()[tuple_offset..tuple_offset + tuple_size].to_vec()))
    }


    pub fn get_all_tuples(&mut self, page_size: usize) -> Vec<Tuple> {
        let entries = self.get_entries();
        let mut tuples = Vec::new();
        for i in 0..entries {
            if let Some(tuple) = self.get_tuple_index(i, page_size) {
                tuples.push(tuple);
            }
        }
        tuples
    }

    pub fn get_tuple(&mut self, key: Vec<u8>, page_size: usize) -> Option<Tuple> {
        let entries = self.get_entries();
        for i in 0..entries {
            if let Some(tuple) = self.get_tuple_index(i, page_size) {
                if tuple.get_key() == key {
                    return Some(tuple);
                }
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
        let mut data_page = DataPage::new(4096, 1);
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let version = 1;

        let tuple = Tuple::new(key, value, version);
        assert!(data_page.add_tuple_base(&tuple, 4096).is_ok());
        assert_eq!(data_page.get_entries(), 1);
        let retrieved_tuple = data_page.get_tuple_index(0, 4096).unwrap();
        assert_eq!(retrieved_tuple.get_key(), b"key");
        assert_eq!(retrieved_tuple.get_value(), b"value");
        assert_eq!(retrieved_tuple.get_version(), 1);
    }

    #[test]
    fn test_get_tuple() {
        let mut data_page = DataPage::new(4096, 1);
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let version = 1;

        let tuple = Tuple::new(key, value, version);
        assert!(data_page.add_tuple_base(&Tuple::new(b"key2".to_vec(), b"value2".to_vec(), 2), 4096).is_ok());
        assert!(data_page.add_tuple_base(&tuple, 4096).is_ok());
        assert_eq!(data_page.get_entries(), 2);
        let key_to_find = b"key".to_vec();
        let retrieved_tuple = data_page.get_tuple(key_to_find, 4096).unwrap();
        assert_eq!(retrieved_tuple.get_key(), b"key");
        assert_eq!(retrieved_tuple.get_value(), b"value");
        assert_eq!(retrieved_tuple.get_version(), 1);

        let missing_key = b"missing".to_vec();
        assert!(data_page.get_tuple(missing_key, 4096).is_none());
    }
}
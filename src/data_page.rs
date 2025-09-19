use crate::page::{Page, PageTrait, PageType};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use crate::tuple::Tuple;

// Checksum(u32) | Page No (u32)| Type(u8) | Entries (u8) | Free_Space (u16) | Data(4084 bytes)
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

    pub fn add_tuple(&mut self, tuple: &Tuple, page_size: u64) -> Result<(), String> {
        let tuple_size: usize = tuple.get_size();
        if !self.can_fit(tuple_size) {
            return Err("Not enough space in DataPage".to_string());
        }

        let current_entries = self.get_entries();
        let current_entries_size: usize = current_entries as usize * 2; // Each entry has 2 bytes of metadata
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
        assert!(data_page.add_tuple(&tuple, 4096).is_ok());
    }
}
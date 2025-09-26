use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use crate::page::Page;
use crate::page::PageTrait;


// | Checksum(u32) | Page No (u32) | Version (u64) | Type(u8) |  Entries (u16) | Reserved(1 byte) |
// | NextPage(u32) | PreviousPage (u32) | Free Page Id (u32) | Free Page Id (u32) ....|
pub struct FreeDirPage {
    page: Page
}

impl PageTrait for FreeDirPage {
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

impl FreeDirPage {
    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut free_page_dir = FreeDirPage {
            page: Page::new(page_size),
        };
        free_page_dir.page.set_type(crate::page::PageType::FreeDir);
        free_page_dir.page.set_page_number(page_number);
        free_page_dir
    }
    
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);  
    }

    pub fn from_page(mut page: Page) -> Self {
        let page_type = page.get_type();
        if page_type != crate::page::PageType::FreeDir {
            panic!("Invalid page type for FreePageDir");
        }

        FreeDirPage { page }
    }

    pub fn get_entries(&mut self) -> u16 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(17);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_entries(&mut self, entries: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(17);
        cursor.write_u16::<LittleEndian>(entries).expect("Failed to write entries");
    }

    pub fn get_next(&mut self) -> u32 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    pub fn set_next(&mut self, entries: u32) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.write_u32::<LittleEndian>(entries).expect("Failed to write next page");
    }

    pub fn get_previous(&mut self) -> u32 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(24);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    pub fn set_previous(&mut self, entries: u32) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(24);
        cursor.write_u32::<LittleEndian>(entries).expect("Failed to write previous page");
    }

    pub fn is_full(&mut self) -> bool {
        let capacity = self.page.get_bytes().len() - 28;
        (capacity - (4 * self.get_entries() as usize)) <= 0
    }

    pub fn has_free_pages(&mut self) -> bool {
        self.get_entries() > 0
    }

    pub fn get_free_page(&mut self) -> u32 {
        assert!(self.has_free_pages());
        let entries = self.get_entries() - 1;
        self.set_entries(entries);
        let offset = 24 + (4 * entries as u64);
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(offset as u64);
        cursor.read_u32::<LittleEndian>().unwrap()
    }
    
    pub fn add_free_page(&mut self, free_page_number: u32) -> () {
        assert!(!self.is_full());
        let entries = self.get_entries();
        let offset = 24 + (4 * self.get_entries() as u64);
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(offset);
        cursor.write_u32::<LittleEndian>(free_page_number).expect("Failed to write free page");
        self.set_entries(entries + 1);
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adding_entries() {
        let mut free_page_dir = FreeDirPage::new(4096, 34);
        assert!(!free_page_dir.has_free_pages());
        free_page_dir.add_free_page(73);
        free_page_dir.add_free_page(103);
        assert!(free_page_dir.has_free_pages());
        assert!(103 == free_page_dir.get_free_page());
        assert!(73 == free_page_dir.get_free_page());
        assert!(!free_page_dir.has_free_pages());
    }

     #[test]
    fn test_fill_free_page_dir() {
        let mut free_page_dir = FreeDirPage::new(4096, 34);
        let mut count = 0;
        for number in 1..=1020 {
            if !free_page_dir.is_full() {
                count = count + 1;
                free_page_dir.add_free_page(number);
            }
        }
        assert!(free_page_dir.is_full());
        assert!(count == 1017);
        assert!(1017 == free_page_dir.get_free_page());
        assert!(!free_page_dir.is_full());
    }

}
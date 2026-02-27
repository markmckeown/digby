use crate::block_layer::PageConfig;
use crate::page::Page;
use crate::page::PageTrait;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::u16;

// Header size 34 bytes
// | Page No (u64) | VersionHolder (8 bytes) | NextPage(u64) | PreviousPage (u64) | Entries u16 |
// | Free Page Id (u64) | Free Page Id (u64) ....|
pub struct FreeDirPage {
    page: Page,
}

impl PageTrait for FreeDirPage {
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

impl FreeDirPage {
    const HEADER_SIZE: usize = 34;
    pub fn create_new(page_config: &PageConfig, page_number: u64, version: u64) -> Self {
        FreeDirPage::new(
            page_config.block_size,
            page_config.page_size,
            page_number,
            version,
        )
    }

    fn new(block_size: usize, page_size: usize, page_number: u64, version: u64) -> Self {
        let mut free_page_dir = FreeDirPage {
            page: Page::new(block_size, page_size),
        };
        free_page_dir.page.set_type(crate::page::PageType::FreeDir);
        free_page_dir.page.set_page_number(page_number);
        free_page_dir.page.set_version(version);
        free_page_dir
    }

    pub fn from_page(page: Page) -> Self {
        let page_type = page.get_type();
        if page_type != crate::page::PageType::FreeDir {
            panic!("Invalid page type for FreePageDir");
        }

        FreeDirPage { page }
    }

    pub fn get_entries(&self) -> u16 {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(32);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_entries(&mut self, entries: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(32);
        cursor
            .write_u16::<LittleEndian>(entries)
            .expect("Failed to write entries");
    }

    pub fn get_next(&self) -> u64 {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(16);
        cursor.read_u64::<LittleEndian>().unwrap()
    }

    pub fn set_next(&mut self, entries: u64) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(16);
        cursor
            .write_u64::<LittleEndian>(entries)
            .expect("Failed to write next page");
    }

    pub fn get_previous(&self) -> u64 {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(24);
        cursor.read_u64::<LittleEndian>().unwrap()
    }

    pub fn set_previous(&mut self, entries: u64) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(24);
        cursor
            .write_u64::<LittleEndian>(entries)
            .expect("Failed to write previous page");
    }

    fn is_full_for(&self, number_of_pages: usize) -> bool {
        let capacity = self.page.get_page_bytes().len() - FreeDirPage::HEADER_SIZE;
        (capacity - (8 * self.get_entries() as usize)) < 8 * number_of_pages
    }

    pub fn is_full(&self) -> bool {
        self.is_full_for(1)
    }

    pub fn has_free_pages(&self) -> bool {
        self.get_entries() > 0
    }

    pub fn get_free_page(&mut self) -> u64 {
        assert!(self.has_free_pages());
        let entries = self.get_entries() - 1;
        self.set_entries(entries);
        let offset = FreeDirPage::HEADER_SIZE + (8 * entries as usize);
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(offset as u64);
        cursor.read_u64::<LittleEndian>().unwrap()
    }

    pub fn add_free_page(&mut self, free_page_number: u64) -> () {
        assert!(!self.is_full());
        let entries = self.get_entries();
        let offset = FreeDirPage::HEADER_SIZE as u64 + (8 * self.get_entries() as u64);
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(offset);
        cursor
            .write_u64::<LittleEndian>(free_page_number)
            .expect("Failed to write free page");
        self.set_entries(entries + 1);
    }

    pub fn add_free_pages(&mut self, free_pages: &Vec<u64>) -> () {
        assert!(!self.is_full_for(free_pages.len()));
        assert!(free_pages.len() < u16::MAX as usize);
        let entries = self.get_entries();
        let mut offset = FreeDirPage::HEADER_SIZE as u64 + (8 * self.get_entries() as u64);
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(offset);
        for free_page in &free_pages[..] {
            cursor
                .write_u64::<LittleEndian>(*free_page)
                .expect("Failed to write free page");
            offset = offset + 8;
            cursor.set_position(offset);
        }
        self.set_entries(entries + free_pages.len() as u16);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adding_entries() {
        let mut free_page_dir = FreeDirPage::new(4096, 4096, 34, 4564);
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
        let mut free_page_dir = FreeDirPage::new(4096, 4096, 34, 657);
        let mut count = 0;
        for number in 1..=1020 {
            if !free_page_dir.is_full() {
                count = count + 1;
                free_page_dir.add_free_page(number);
            }
        }
        assert!(free_page_dir.is_full());
        assert_eq!(count, 507);
        assert_eq!(507, free_page_dir.get_free_page());
        assert!(!free_page_dir.is_full());
    }
}

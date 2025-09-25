use crate::page::PageType;
use crate::page::Page;
use crate::page::PageTrait;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

// | Checksum(u32) | Page No (u32) | Version (u64) | Type(u8) | Reserved(3 bytes) | 
// | FreePageDir(u32) |
pub struct HeadPage {
    page: Page
}

impl PageTrait for HeadPage {
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

impl HeadPage {
    pub fn new(page_size: u64, page_number: u32, version: u64) -> Self {
        let mut head_page = HeadPage {
            page: Page::new(page_size),
        };
        head_page.page.set_type(PageType::Head);
        head_page.page.set_page_number(page_number);
        head_page.set_version(version);
        head_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::Head {
            panic!("Invalid page type for HeadPage");
        }

        let head_page = HeadPage { page };
        head_page
    }

    pub fn get_free_page_dir(&mut self) -> u32 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    pub fn set_free_page_dir(&mut self, entries: u32) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.write_u32::<LittleEndian>(entries).expect("Failed to write free page dir page");
    }

    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_head_page() {
        let mut head_page = HeadPage::new(4096, 0, 1);
        assert_eq!(head_page.get_version(), 1);
        head_page.set_version(2);
        assert_eq!(head_page.get_version(), 2);
    }
}
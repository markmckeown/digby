use crate::page::PageType;
use crate::page::Page;
use crate::page::PageTrait;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

// | Checksum(u32) | Page No (u32) | Version (u64) | Type(u8) | Reserved(3 bytes) | 
// | GlobalTreeRootPage (u32) | FreeDirPage(u32) | TableDirPage(u32) |
pub struct DbMasterPage {
    page: Page
}

impl PageTrait for DbMasterPage {
    fn get_bytes(&self) -> &[u8] {
        self.page.get_bytes()
    }

    fn get_page_number(& self) -> u32 {
        self.page.get_page_number()
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

impl DbMasterPage {
    pub fn new(page_size: u64, page_number: u32, version: u64) -> Self {
        let mut head_page = DbMasterPage {
            page: Page::new(page_size),
        };
        head_page.page.set_type(PageType::DbMaster);
        head_page.page.set_page_number(page_number);
        head_page.set_version(version);
        head_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::DbMaster {
            panic!("Invalid page type for HeadPage");
        }

        let head_page = DbMasterPage { page };
        head_page
    }

    pub fn get_global_tree_root_page_no(&mut self) -> u32 {
        self.get_u32_at_offset(20)
    }

    pub fn set_global_tree_root_page_no(&mut self, page_no: u32) {
        self.set_u32_at_offset(20, page_no);
    }

    pub fn get_free_page_dir_page_no(&mut self) -> u32 {
        self.get_u32_at_offset(24)
    }

    pub fn set_free_page_dir_page_no(&mut self, page_no: u32) {
        self.set_u32_at_offset(24, page_no);
    }

    pub fn get_table_dir_page_no(&mut self) -> u32 {
        self.get_u32_at_offset(28)
    }

    pub fn set_table_dir_page_no(&mut self, page_no: u32) {
        self.set_u32_at_offset(28, page_no);
    }

    fn set_u32_at_offset(&mut self, offset: u64, value: u32) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(offset);
        cursor.write_u32::<LittleEndian>(value).expect("Failed to write table dir page number");
    }

    fn get_u32_at_offset(&mut self, offset: u64) -> u32 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(offset);
        cursor.read_u32::<LittleEndian>().unwrap()
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_head_page() {
        let mut head_page = DbMasterPage::new(4096, 0, 1);
        assert_eq!(head_page.get_version(), 1);
        head_page.set_version(2);
        assert_eq!(head_page.get_version(), 2);
    }
}
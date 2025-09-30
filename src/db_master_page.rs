use crate::page::PageType;
use crate::page::Page;
use crate::page::PageTrait;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

// | Checksum(u32) | Page No (u32) | VersionHolder (8 bytes) | Pad (4 bytes) | 
// Allow for more TableDirPages in Future
// | GlobalTreeRootPage (u32) | TableDirPage(u32) | Pad (4 bytes) | Pad (4 bytes) | Pad (4 bytes) | FreePageDir (u32) |
// Could have more FreePageDir in future.
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

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::DbMaster {
            panic!("Invalid page type for HeadPage");
        }

        let head_page = DbMasterPage { page };
        head_page
    }

    const GLOBAL_TREE_OFFSET: u64 = 20;
    pub fn get_global_tree_root_page_no(&self) -> u32 {
        self.get_u32_at_offset(DbMasterPage::GLOBAL_TREE_OFFSET)
    }

    pub fn set_global_tree_root_page_no(&mut self, page_no: u32) {
        self.set_u32_at_offset(DbMasterPage::GLOBAL_TREE_OFFSET, page_no);
    }

    const FREE_PAGE_DIR_OFFSET: u64 = 36;
    pub fn get_free_page_dir_page_no(&self) -> u32 {
        self.get_u32_at_offset(DbMasterPage::FREE_PAGE_DIR_OFFSET)
    }

    pub fn set_free_page_dir_page_no(&mut self, page_no: u32) {
        self.set_u32_at_offset(DbMasterPage::FREE_PAGE_DIR_OFFSET, page_no);
    }

    const TABLE_DIR_PAGE: u64 = 24;
    pub fn get_table_dir_page_no(&self) -> u32 {
        self.get_u32_at_offset(DbMasterPage::TABLE_DIR_PAGE)
    }

    pub fn set_table_dir_page_no(&mut self, page_no: u32) {
        self.set_u32_at_offset(DbMasterPage::TABLE_DIR_PAGE, page_no);
    }


    fn set_u32_at_offset(&mut self, offset: u64, value: u32) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(offset);
        cursor.write_u32::<LittleEndian>(value).expect("Failed to write table dir page number");
    }

    fn get_u32_at_offset(&self, offset: u64) -> u32 {
        let mut cursor = Cursor::new(&self.page.get_bytes()[..]);
        cursor.set_position(offset);
        cursor.read_u32::<LittleEndian>().unwrap()
    }
    
    pub fn flip_page_number(&mut self) -> () {
        let page_number = self.get_page_number();
        let mut new_page_number = 0;
        if page_number == 0 {
            new_page_number = 1;
        }
        self.page.set_page_number(new_page_number);
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_head_page() {
        let mut master_page = DbMasterPage::new(4096, 0, 1);
        assert_eq!(master_page.get_version(), 1);
        master_page.set_version(2);
        assert_eq!(master_page.get_version(), 2);
        assert!(0 == master_page.get_free_page_dir_page_no());
        assert!(0 == master_page.get_global_tree_root_page_no());
        assert!(0 == master_page.get_table_dir_page_no());
        master_page.set_free_page_dir_page_no(67);
        master_page.set_global_tree_root_page_no(87);
        master_page.set_table_dir_page_no(34);
        assert!(67 == master_page.get_free_page_dir_page_no());
        assert!(87 == master_page.get_global_tree_root_page_no());
        assert!(34 == master_page.get_table_dir_page_no());
    }
}
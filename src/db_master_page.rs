use crate::db_config::DbConfig;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page::PageType;
use crate::page_no::PageNo;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

// | Page No (8 bytes) | VersionHolder (8 bytes) | GlobalTreeRootPage (8 bytes) |
// | TableDirPage(8 bytes) | FreePageDir (8 bytes) |
// Could have more FreePageDir in future.
pub struct DbMasterPage {
    page: Page,
}

impl PageTrait for DbMasterPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> PageNo {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: PageNo) {
        assert!(
            page_no.get_blk_offset() == 1 || page_no.get_blk_offset() == 2,
            "DbMasterPage must have page number 1 or 2"
        );
        self.page.set_page_number(page_no)
    }

    fn get_page(&mut self) -> &mut Page {
        &mut self.page
    }

    fn get_version(&self) -> u64 {
        self.page.get_version()
    }

    fn set_version(&mut self, version: u64) {
        self.page.set_version(version);
    }
}

impl DbMasterPage {
    pub fn create_new(page_config: &DbConfig, page_number: PageNo, version: u64) -> Self {
        assert!(
            page_number.get_blk_offset() == 1 || page_number.get_blk_offset() == 2,
            "DbMasterPage must have page number 1 or 2"
        );
        DbMasterPage::new(
            page_config.block_size,
            page_config.page_size,
            page_number,
            version,
        )
    }

    fn new(block_size: usize, page_size: usize, page_number: PageNo, version: u64) -> Self {
        let mut head_page = DbMasterPage {
            page: Page::new(block_size, page_size),
        };
        head_page.page.set_type(PageType::DbMaster);
        head_page.page.set_page_number(page_number);
        head_page.set_version(version);
        head_page
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::DbMaster {
            panic!("Invalid page type for DbMasterPage");
        }

        DbMasterPage { page }
    }

    const GLOBAL_TREE_OFFSET: u64 = 16;
    pub fn get_global_tree_root_page_no(&self) -> u64 {
        self.get_u64_at_offset(DbMasterPage::GLOBAL_TREE_OFFSET)
    }

    pub fn set_global_tree_root_page_no(&mut self, page_no: u64) {
        self.set_u64_at_offset(DbMasterPage::GLOBAL_TREE_OFFSET, page_no);
    }

    const FREE_PAGE_DIR_OFFSET: u64 = 32;
    pub fn get_free_page_dir_page_no(&self) -> u64 {
        self.get_u64_at_offset(DbMasterPage::FREE_PAGE_DIR_OFFSET)
    }

    pub fn set_free_page_dir_page_no(&mut self, page_no: u64) {
        self.set_u64_at_offset(DbMasterPage::FREE_PAGE_DIR_OFFSET, page_no);
    }

    const TABLE_DIR_PAGE: u64 = 24;
    pub fn get_table_dir_page_no(&self) -> u64 {
        self.get_u64_at_offset(DbMasterPage::TABLE_DIR_PAGE)
    }

    pub fn set_table_dir_page_no(&mut self, page_no: u64) {
        self.set_u64_at_offset(DbMasterPage::TABLE_DIR_PAGE, page_no);
    }

    fn set_u64_at_offset(&mut self, offset: u64, value: u64) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(offset);
        cursor
            .write_u64::<LittleEndian>(value)
            .expect("Failed to write table dir page number");
    }

    fn get_u64_at_offset(&self, offset: u64) -> u64 {
        let mut cursor = Cursor::new(self.page.get_page_bytes());
        cursor.set_position(offset);
        cursor.read_u64::<LittleEndian>().unwrap()
    }

    pub fn flip_page_number(&mut self) {
        let page_number = self.get_page_number();
        let new_page_number: u64 = if page_number.get_blk_offset() == 1 {
            2
        } else {
            1
        };
        self.page.set_page_number(PageNo::from_u64(new_page_number));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_head_page() {
        let mut master_page = DbMasterPage::new(4096, 4092, PageNo::from_u64(0), 1);
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

    #[test]
    fn test_create_new() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
        };
        let mut master_page = DbMasterPage::create_new(&page_config, PageNo::from_u64(1), 5);
        assert_eq!(master_page.get_page_number().get_blk_offset(), 1);
        assert_eq!(master_page.get_version(), 5);
        assert_eq!(master_page.page.get_type(), PageType::DbMaster);
        assert_eq!(master_page.get_page_bytes().len(), 4092);
        master_page.set_page_number(PageNo::from_u64(1));
        assert_eq!(master_page.get_page_number().get_blk_offset(), 1);
        master_page.set_page_number(PageNo::from_u64(2));
        assert_eq!(master_page.get_page_number().get_blk_offset(), 2);
    }

    #[test]
    #[should_panic(expected = "DbMasterPage must have page number 1 or 2")]
    fn test_bad_page_no() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
        };
        let _master_page = DbMasterPage::create_new(&page_config, PageNo::from_u64(4), 5);
    }

    #[test]
    fn test_from_page_valid() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::DbMaster);
        page.set_page_number(PageNo::from_u64(2));

        let master_page = DbMasterPage::from_page(page);
        assert_eq!(master_page.get_page_number().get_blk_offset(), 2);
    }

    #[test]
    #[should_panic(expected = "DbMasterPage must have page number 1 or 2")]
    fn test_set_invalid_page_no() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::DbMaster);
        page.set_page_number(PageNo::from_u64(2));

        let mut master_page = DbMasterPage::from_page(page);
        master_page.set_page_number(PageNo::from_u64(3));
    }

    #[test]
    #[should_panic(expected = "Invalid page type for DbMasterPage")]
    fn test_from_page_invalid_type() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::LeafPage);
        let _ = DbMasterPage::from_page(page);
    }

    #[test]
    fn test_flip_page_number() {
        let mut master_page = DbMasterPage::new(4096, 4092, PageNo::from_u64(1), 1);
        master_page.flip_page_number();
        assert_eq!(master_page.get_page_number().get_blk_offset(), 2);

        master_page.flip_page_number();
        assert_eq!(master_page.get_page_number().get_blk_offset(), 1);
    }
}

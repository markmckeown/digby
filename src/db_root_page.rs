use crate::block_layer::PageConfig;
use crate::block_sanity::BlockSanity;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page::PageType;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

// | Page No (8 bytes) | Version/Type (8 bytes) |
// | Magic Number(u32) | DbVersionMajor (u16) | DbVersionMinor (u16) |
// | Sanity (u8) | Compression (u8) |
pub struct DbRootPage {
    page: Page,
}

impl PageTrait for DbRootPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> u64 {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: u64) {
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

impl DbRootPage {
    const MAGIC_NUMBER: u32 = 26061973;
    const VERSION_MAJOR: u16 = 0;
    const VERSION_MINOR: u16 = 1;

    pub fn create_new(page_config: &PageConfig) -> Self {
        DbRootPage::new(page_config.block_size, page_config.page_size)
    }

    fn new(block_size: usize, page_size: usize) -> Self {
        let mut db_root_page = DbRootPage {
            page: Page::new(block_size, page_size),
        };
        db_root_page.page.set_type(PageType::DbRoot);
        db_root_page.page.set_page_number(0);
        db_root_page.set_magic_number();
        db_root_page.set_db_major_version();
        db_root_page.set_db_minor_version();
        db_root_page
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::DbRoot {
            panic!("Invalid page type for RootPage");
        }
        if page.get_page_number() != 0 {
            panic!("Invalid page number for RootPage");
        }
        let head_page = DbRootPage { page };
        if head_page.get_magic_number() != Self::MAGIC_NUMBER {
            panic!("Invalid magic number for RootPage");
        }

        head_page
    }

    pub fn get_magic_number(&self) -> u32 {
        let mut cursor = Cursor::new(self.page.get_page_bytes());
        cursor.set_position(16);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    pub fn set_magic_number(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(16);
        cursor
            .write_u32::<LittleEndian>(Self::MAGIC_NUMBER)
            .expect("Failed to write magic number");
    }

    pub fn get_db_major_version(&self) -> u16 {
        let mut cursor = Cursor::new(self.page.get_page_bytes());
        cursor.set_position(20);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_db_major_version(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(20);
        cursor
            .write_u16::<LittleEndian>(Self::VERSION_MAJOR)
            .expect("Failed to write major version number");
    }

    pub fn get_db_minor_version(&self) -> u16 {
        let mut cursor = Cursor::new(self.page.get_page_bytes());
        cursor.set_position(22);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_db_minor_version(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(22);
        cursor
            .write_u16::<LittleEndian>(Self::VERSION_MINOR)
            .expect("Failed to write minor version number");
    }

    pub fn get_sanity_type(&self) -> BlockSanity {
        let mut cursor = Cursor::new(self.page.get_page_bytes());
        cursor.set_position(24);
        BlockSanity::try_from(cursor.read_u8().unwrap()).unwrap()
    }

    pub fn set_sanity_type(&mut self, sanity_type: BlockSanity) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(24);
        cursor
            .write_u8(u8::from(sanity_type))
            .expect("Failed to write sanity type");
    }

    pub fn get_compression_type(&self) -> u8 {
        let mut cursor = Cursor::new(self.page.get_page_bytes());
        cursor.set_position(25);
        cursor.read_u8().unwrap()
    }

    pub fn set_compression_type(&mut self, compression_type: u8) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(25);
        cursor
            .write_u8(compression_type)
            .expect("Failed to write compression type");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_new() {
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let root_page = DbRootPage::create_new(&page_config);

        assert_eq!(root_page.get_page_number(), 0);
        assert_eq!(root_page.get_magic_number(), DbRootPage::MAGIC_NUMBER);
        assert_eq!(root_page.get_db_major_version(), DbRootPage::VERSION_MAJOR);
        assert_eq!(root_page.get_db_minor_version(), DbRootPage::VERSION_MINOR);
        assert_eq!(root_page.page.get_type(), PageType::DbRoot);
    }

    #[test]
    fn test_from_page_valid() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::DbRoot);
        page.set_page_number(0);

        // Manually write the magic number to the page buffer so validation passes
        let mut cursor = Cursor::new(page.get_page_bytes_mut());
        cursor.set_position(16);
        cursor.write_u32::<LittleEndian>(DbRootPage::MAGIC_NUMBER).unwrap();

        let root_page = DbRootPage::from_page(page);
        assert_eq!(root_page.get_magic_number(), DbRootPage::MAGIC_NUMBER);
    }

    #[test]
    #[should_panic(expected = "Invalid page type for RootPage")]
    fn test_from_page_invalid_type() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::LeafPage);
        let _ = DbRootPage::from_page(page);
    }

    #[test]
    #[should_panic(expected = "Invalid page number for RootPage")]
    fn test_from_page_invalid_page_number() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::DbRoot);
        page.set_page_number(1);
        let _ = DbRootPage::from_page(page);
    }

    #[test]
    #[should_panic(expected = "Invalid magic number for RootPage")]
    fn test_from_page_invalid_magic_number() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::DbRoot);
        page.set_page_number(0);
        // Magic number is 0 by default, so validation will panic
        let _ = DbRootPage::from_page(page);
    }

    #[test]
    fn test_setters_and_getters() {
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let mut root_page = DbRootPage::create_new(&page_config);

        root_page.set_sanity_type(BlockSanity::Aes128Gcm);
        assert_eq!(root_page.get_sanity_type(), BlockSanity::Aes128Gcm);

        root_page.set_sanity_type(BlockSanity::XxH32Checksum);
        assert_eq!(root_page.get_sanity_type(), BlockSanity::XxH32Checksum);

        root_page.set_compression_type(1);
        assert_eq!(root_page.get_compression_type(), 1);

        root_page.set_version(100);
        assert_eq!(root_page.get_version(), 100);
    }
}

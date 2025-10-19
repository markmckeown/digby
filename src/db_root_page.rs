use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use crate::block_sanity::BlockSanity;
use crate::block_layer::PageConfig;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page::PageType;

// | Page No (u32) | Version/Type (8 bytes) |
// | Magic Number(u32) | DbVersionMajor (u16) | DbVersionMinor (u16) |
// | Sanity (u8) | Compression (u8) | 
pub struct DbRootPage {
    page: Page
}

impl PageTrait for DbRootPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(& self) -> u32 {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self,  page_no: u32) -> () {
        self.page.set_page_number(page_no)
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
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(12);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    pub fn set_magic_number(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(12);
        cursor.write_u32::<LittleEndian>(Self::MAGIC_NUMBER).expect("Failed to write magic number");
    }

    pub fn get_db_major_version(&self) -> u16 {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(16);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_db_major_version(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(16);
        cursor.write_u16::<LittleEndian>(Self::VERSION_MAJOR).expect("Failed to write major version number");
    }

    pub fn get_db_minor_version(&self) -> u16 {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(18);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_db_minor_version(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(18);
        cursor.write_u16::<LittleEndian>(Self::VERSION_MINOR).expect("Failed to write minor version number");
    }

    pub fn get_sanity_type(&self) -> BlockSanity {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(20);
        BlockSanity::try_from(cursor.read_u8().unwrap()).unwrap()
    }

    pub fn set_sanity_type(&mut self, sanity_type: BlockSanity) -> () {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.write_u8(u8::from(sanity_type)).expect("Failed to write minor version number");
    }

    pub fn get_compression_type(&self) -> u8 {
        let mut cursor = Cursor::new(&self.page.get_page_bytes()[..]);
        cursor.set_position(21);
        cursor.read_u8().unwrap()
    }

    pub fn set_compression_type(&mut self, sanity_type: u8) -> () {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(21);
        cursor.write_u8(sanity_type).expect("Failed to write minor version number");
    }
}   
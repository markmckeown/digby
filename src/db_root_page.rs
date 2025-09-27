use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page::PageType;

// | Checksum(u32) | Page No (u32) | Version (u64) | Type(u8) | Reserved(3 bytes) | Data(4084 bytes)
// | Magic Number(u32) | DbVersionMajor (u16) | DbVersionMinor (u16) |
pub struct DbRootPage {
    page: Page
}

impl PageTrait for DbRootPage {
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

impl DbRootPage {
    const MAGIC_NUMBER: u32 = 26061973;
    const VERSION_MAJOR: u16 = 0;
    const VERSION_MINOR: u16 = 1;

    pub fn new(page_size: u64) -> Self {
        let mut db_root_page = DbRootPage {
            page: Page::new(page_size),
        };
        db_root_page.page.set_type(PageType::DbRoot);
        db_root_page.page.set_page_number(0);
        db_root_page.set_magic_number();
        db_root_page.set_db_major_version();
        db_root_page.set_db_minor_version();
        db_root_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::DbRoot {
            panic!("Invalid page type for RootPage");
        }
        if page.get_page_number() != 0 {
            panic!("Invalid page number for RootPage");
        }
        let mut head_page = DbRootPage { page };
        if head_page.get_magic_number() != Self::MAGIC_NUMBER {
            panic!("Invalid magic number for RootPage");
        }

        head_page
    }

    pub fn get_magic_number(&mut self) -> u32 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    pub fn set_magic_number(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.write_u32::<LittleEndian>(Self::MAGIC_NUMBER).expect("Failed to write magic number");
    }

    pub fn get_db_major_version(&mut self) -> u16 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(24);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_db_major_version(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(24);
        cursor.write_u16::<LittleEndian>(Self::VERSION_MAJOR).expect("Failed to write major version number");
    }

    pub fn get_db_minor_version(&mut self) -> u16 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(26);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_db_minor_version(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(26);
        cursor.write_u16::<LittleEndian>(Self::VERSION_MINOR).expect("Failed to write minor version number");
    }
}   
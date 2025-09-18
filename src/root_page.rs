use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page::PageType;


pub struct RootPage {
    page: Page
}

impl PageTrait for RootPage {
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

impl RootPage {
    const MAGIC_NUMBER: u32 = 26061973;

    pub fn new(page_size: u64) -> Self {
        let mut head_page = RootPage {
            page: Page::new(page_size),
        };
        head_page.page.set_type(PageType::Root);
        head_page.page.set_page_number(0);
        head_page.set_magic_number();
        head_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::Root {
            panic!("Invalid page type for HeadPage");
        }
        if page.get_page_number() != 0 {
            panic!("Invalid page number for HeadPage");
        }
        let mut head_page = RootPage { page };
        if head_page.get_magic_number() != Self::MAGIC_NUMBER {
            panic!("Invalid magic number for HeadPage");
        }

        head_page
    }

    pub fn get_magic_number(&mut self) -> u32 {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(12);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    pub fn set_magic_number(&mut self) {
        let mut cursor = Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(12);
        cursor.write_u32::<LittleEndian>(Self::MAGIC_NUMBER).expect("Failed to write magic number");
    }
}   
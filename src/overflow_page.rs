use crate::page::Page;
use crate::page::PageTrait;
use byteorder::{ReadBytesExt, WriteBytesExt};


// From Page Header
// | Checksum(u32) | Page No (u32) | Version (u64) | Type(u8) | UsedSize (u16)  | Reserved(1 bytes) |
//
// | Previous Overflow Page (u32) | Next Overflow Page (u32) | Data (...) |
//
// If previous or next overflow page is 0, it means there is no previous or next overflow page.
pub struct OverflowPage {
    page: Page
}

impl PageTrait for OverflowPage {
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

impl OverflowPage {
    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut overflow_page = OverflowPage {
            page: Page::new(page_size),
        };
        overflow_page.page.set_type(crate::page::PageType::Overflow);
        overflow_page.page.set_page_number(page_number);
        overflow_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != crate::page::PageType::Overflow {
            panic!("Invalid page type for OverflowPage");
        }

        let overflow_page = OverflowPage { page };
        overflow_page
    }

    pub fn get_previous_page(&mut self) -> u32 {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.read_u32::<byteorder::LittleEndian>().unwrap()
    }

    pub fn set_previous_page(&mut self, page_number: u32) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.write_u32::<byteorder::LittleEndian>(page_number as u32).expect("Failed to write previous overflow page number");
    }

    pub fn get_next_page(&mut self) -> u32 {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(24);
        cursor.read_u32::<byteorder::LittleEndian>().unwrap()
    }

    pub fn set_next_page(&mut self, page_number: u32) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(24);
        cursor.write_u32::<byteorder::LittleEndian>(page_number as u32).expect("Failed to write next overflow page number");
    }

    pub fn get_used_size(&self) -> u16 {
        let slice = &self.page.get_bytes()[17..19];
        let bytes: [u8; 2] = slice.try_into().unwrap();
        return u16::from_le_bytes(bytes)
    }

    pub fn set_used_size(&mut self, used_size: u16) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(17);
        cursor.write_u16::<byteorder::LittleEndian>(used_size as u16).expect("Failed to write used size");
    }

    pub fn get_data(&mut self) -> Vec<u8> {
        self.page.get_bytes()[28..(28 + self.get_used_size() as usize)].to_vec()
    }

    pub fn get_data_capacity(&self) -> usize {
        self.page.get_bytes().len() - 28
    }

    pub fn get_free_space(&self) -> usize {
        self.get_data_capacity() - self.get_used_size() as usize
    }
}
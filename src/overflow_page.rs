use crate::page::Page;
use crate::page::PageTrait;
use byteorder::{ReadBytesExt, WriteBytesExt};


// From Page Header - size 26
// | Checksum(u32) | Page No (u32) | VersionHolder (8 bytes) | Previous Overflow Page (u32) | Next Overflow Page (u32) | SizeUsed (u16) |
//
// |  OverflowTuple.... |
//
// If previous or next overflow page is 0, it means there is no previous or next overflow page.
pub struct OverflowPage {
    page: Page
}

impl PageTrait for OverflowPage {
    fn get_bytes(&self) -> &[u8] {
        self.page.get_bytes()
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

impl OverflowPage {
    const HEADER_SIZE: usize = 26;

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

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != crate::page::PageType::Overflow {
            panic!("Invalid page type for OverflowPage");
        }

        let overflow_page = OverflowPage { page };
        overflow_page
    }

    pub fn get_previous_page(&mut self) -> u32 {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(16);
        cursor.read_u32::<byteorder::LittleEndian>().unwrap()
    }

    pub fn set_previous_page(&mut self, page_number: u32) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(16);
        cursor.write_u32::<byteorder::LittleEndian>(page_number as u32).expect("Failed to write previous overflow page number");
    }

    pub fn get_next_page(&mut self) -> u32 {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.read_u32::<byteorder::LittleEndian>().unwrap()
    }

    pub fn set_next_page(&mut self, page_number: u32) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.write_u32::<byteorder::LittleEndian>(page_number as u32).expect("Failed to write next overflow page number");
    }

    pub fn get_used_size(&self) -> u16 {
        let slice = &self.page.get_bytes()[24..26];
        let bytes: [u8; 2] = slice.try_into().unwrap();
        return u16::from_le_bytes(bytes)
    }

    pub fn set_used_size(&mut self, used_size: u16) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(24);
        cursor.write_u16::<byteorder::LittleEndian>(used_size as u16).expect("Failed to write used size");
    }

    pub fn get_data(&mut self) -> Vec<u8> {
        self.page.get_bytes()[26..(26 + self.get_used_size() as usize)].to_vec()
    }

    pub fn get_data_capacity(&self) -> usize {
        self.page.get_bytes().len() - OverflowPage::HEADER_SIZE
    }

    pub fn get_free_space(&self) -> usize {
        self.get_data_capacity() - self.get_used_size() as usize
    }
}
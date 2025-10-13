use crate::page::Page;
use crate::page::PageTrait;
use byteorder::{ReadBytesExt, WriteBytesExt};


// From Page Header - size 22
// | Checksum(u32) | Page No (u32) | VersionHolder (8 bytes) | Next Overflow Page (u32) | SizeUsed (u16) |
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
    const HEADER_SIZE: usize = 22;

    pub fn new(page_size: u64, page_number: u32, version: u64) -> Self {
        let mut overflow_page = OverflowPage {
            page: Page::new(page_size),
        };
        overflow_page.page.set_type(crate::page::PageType::Overflow);
        overflow_page.page.set_page_number(page_number);
        overflow_page.set_version(version);
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

    pub fn get_next_page(&mut self) -> u32 {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(16);
        cursor.read_u32::<byteorder::LittleEndian>().unwrap()
    }

    pub fn set_next_page(&mut self, page_number: u32) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(16);
        cursor.write_u32::<byteorder::LittleEndian>(page_number as u32).expect("Failed to write next overflow page number");
    }

    pub fn get_used_size(&self) -> u16 {
        let slice = &self.page.get_bytes()[20..22];
        let bytes: [u8; 2] = slice.try_into().unwrap();
        return u16::from_le_bytes(bytes)
    }

    pub fn set_used_size(&mut self, used_size: u16) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_bytes_mut()[..]);
        cursor.set_position(20);
        cursor.write_u16::<byteorder::LittleEndian>(used_size as u16).expect("Failed to write used size");
    }

    pub fn get_free_space(&self, page_size: usize) -> usize {
        page_size - self.get_used_size() as usize
    }

    pub fn add_bytes(&mut self, bytes: &[u8], size: usize) {
        self.get_page().get_bytes_mut()[OverflowPage::HEADER_SIZE .. OverflowPage::HEADER_SIZE + size].copy_from_slice(bytes);
        self.set_used_size(size as u16);
    }

    pub fn get_tuple_bytes(&mut self) -> Vec<u8> {
        let size = self.get_used_size();
        let bytes = 
        self.get_bytes()[OverflowPage::HEADER_SIZE .. OverflowPage::HEADER_SIZE + size as usize].to_vec();
        return bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adding_bytes() {
        let page_size = 4096;
        let mut page = OverflowPage::new(page_size, 0, 0);
        let buffer = b"This is a big buffer".to_vec();

        page.add_bytes(buffer[0..4].as_ref(), 4);
        let out = page.get_tuple_bytes();
        assert_eq!(out.len(), 4);
        assert_eq!(buffer[0..4], *out);
    }
}
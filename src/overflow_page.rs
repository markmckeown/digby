use crate::db_config::DbConfig;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page_no::PageNo;
use byteorder::{ReadBytesExt, WriteBytesExt};

// From Page Header - size 26
// | Page No (8bytes) | VersionHolder (8 bytes) | Next Overflow Page (8 bytes) | SizeUsed (u16) |
//
// |  OverflowTuple.... |
//
// If previous or next overflow page is 0, it means there is no previous or next overflow page.
pub struct OverflowPage {
    page: Page,
}

impl PageTrait for OverflowPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> PageNo {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: PageNo) {
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

impl OverflowPage {
    const HEADER_SIZE: usize = 26;

    pub fn create_new(page_config: &DbConfig, page_number: PageNo, version: u64) -> Self {
        OverflowPage::new(
            page_config.block_size,
            page_config.page_size,
            page_number,
            version,
        )
    }

    fn new(block_size: usize, page_size: usize, page_number: PageNo, version: u64) -> Self {
        let mut overflow_page = OverflowPage {
            page: Page::new(block_size, page_size),
        };
        overflow_page.page.set_type(crate::page::PageType::Overflow);
        overflow_page.page.set_page_number(page_number);
        overflow_page.set_version(version);
        overflow_page
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != crate::page::PageType::Overflow {
            panic!("Invalid page type for OverflowPage");
        }

        OverflowPage { page }
    }

    pub fn get_next_page(&self) -> u64 {
        let mut cursor = std::io::Cursor::new(self.page.get_page_bytes());
        cursor.set_position(16);
        cursor.read_u64::<byteorder::LittleEndian>().unwrap()
    }

    pub fn set_next_page(&mut self, page_number: u64) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(16);
        cursor
            .write_u64::<byteorder::LittleEndian>(page_number)
            .expect("Failed to write next overflow page number");
    }

    pub fn get_used_size(&self) -> u16 {
        let slice = &self.page.get_page_bytes()[24..26];
        let bytes: [u8; 2] = slice.try_into().unwrap();
        u16::from_le_bytes(bytes)
    }

    pub fn set_used_size(&mut self, used_size: u16) {
        let mut cursor = std::io::Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(24);
        cursor
            .write_u16::<byteorder::LittleEndian>(used_size)
            .expect("Failed to write used size");
    }

    pub fn get_free_space(&self) -> usize {
        self.page.page_size - (self.get_used_size() as usize + OverflowPage::HEADER_SIZE)
    }

    pub fn add_bytes(&mut self, bytes: &[u8], size: usize) {
        self.get_page().get_page_bytes_mut()
            [OverflowPage::HEADER_SIZE..OverflowPage::HEADER_SIZE + size]
            .copy_from_slice(bytes);
        self.set_used_size(size as u16);
    }

    pub fn get_tuple_bytes(&self) -> Vec<u8> {
        let size = self.get_used_size();

        self.get_page_bytes()[OverflowPage::HEADER_SIZE..OverflowPage::HEADER_SIZE + size as usize]
            .to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adding_bytes() {
        let page_size = 4096;
        let mut page = OverflowPage::new(page_size, page_size, PageNo::from_u64(334), 34);
        let buffer = b"This is a big buffer".to_vec();

        page.add_bytes(buffer[0..4].as_ref(), 4);
        let out = page.get_tuple_bytes();
        assert_eq!(out.len(), 4);
        assert_eq!(buffer[0..4], *out);
        assert_eq!(page.get_version(), 34);
        assert_eq!(page.get_next_page(), 0);
        assert_eq!(page.get_used_size(), 4);
        assert_eq!(page.get_page_number().to_u64(), 334);
        page.set_page_number(PageNo::from_u64(457));
        assert_eq!(page.get_page_number().to_u64(), 457);
    }

    #[should_panic(expected = "Invalid page type for OverflowPage")]
    #[test]
    fn test_invalid_page_type() {
        let mut page = Page::new(4096, 4092);
        page.set_type(crate::page::PageType::DbMaster);
        OverflowPage::from_page(page);
    }

    #[test]
    fn test_create_new() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
            compressor_type: crate::compressor::CompressorType::None,
        };
        let overflow_page = OverflowPage::create_new(&page_config, PageNo::from_u64(334), 34);
        assert_eq!(overflow_page.get_page_number().to_u64(), 334);
        assert_eq!(overflow_page.get_version(), 34);
        assert_eq!(
            overflow_page.page.get_type(),
            crate::page::PageType::Overflow
        );
        assert_eq!(overflow_page.get_page_bytes().len(), 4092);
    }
}

use crate::db_config::DbConfig;
use crate::page::PageType;
use crate::page_no::PageNo;

pub struct Slab {
    page_no: PageNo,
    slab_bytes: Vec<u8>,
    pages: u16,
}

impl Slab {
    pub fn create_new(page_meta: &DbConfig, page_capacity: u64) -> Self {
        let buffer_size = page_meta.block_size * page_capacity as usize;
        Slab {
            page_no: PageNo::new(PageType::Null, 0, 0),
            slab_bytes: vec![0u8; buffer_size],
            pages: 0,
        }
    }

    pub fn get_page_number(&self) -> PageNo {
        self.page_no
    }

    pub fn set_page_number(&mut self, page_no: PageNo) {
        // TODO Check Type & block count.
        self.page_no = page_no;
    }

    pub fn get_slab_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.slab_bytes[..]
    }

    pub fn get_slab_bytes(&self) -> &[u8] {
        &self.slab_bytes
    }

    pub fn get_pages_used(&self) -> u16 {
        self.pages
    }

    pub fn set_pages_used(&mut self, count: u16) {
        self.pages = count;
    }
}

use crate::page::Page;
use crate::page::PageTrait;


// | Checksum(u32) | Page No (u32) | Version (u64) | Type(u8) | Reserved(3 bytes) | 
pub struct TreePageDir {
    page: Page
}

impl PageTrait for TreePageDir {
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

impl TreePageDir {
    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut tree_page_dir =  TreePageDir {
            page: Page::new(page_size),
        };
        tree_page_dir.page.set_type(crate::page::PageType::TreeDir);
        tree_page_dir.page.set_page_number(page_number);
        tree_page_dir
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != crate::page::PageType::TreeDir {
            panic!("Invalid page type for TreePageDir");
        }

        let tree_page_dir = TreePageDir { page };
        tree_page_dir
    }
}
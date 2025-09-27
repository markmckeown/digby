use crate::page::Page;
use crate::page::PageTrait;

pub struct TreeRootPage {
    page: Page
}   

impl PageTrait for TreeRootPage {
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

impl TreeRootPage {
    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut tree_root_page = TreeRootPage {
            page: Page::new(page_size),
        };
        tree_root_page.page.set_type(crate::page::PageType::TreeRoot);
        tree_root_page.page.set_page_number(page_number);
        tree_root_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != crate::page::PageType::TreeRoot {
            panic!("Invalid page type for FreePage");
        }

        TreeRootPage { page }
    }
}
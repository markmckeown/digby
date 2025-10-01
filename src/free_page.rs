use crate::page::Page;
use crate::page::PageTrait;


pub struct FreePage {
    page: Page
}   

impl PageTrait for FreePage {
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

impl FreePage {
    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut free_page = FreePage {
            page: Page::new(page_size),
        };
        free_page.page.set_type(crate::page::PageType::Free);
        free_page.page.set_page_number(page_number);
        free_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != crate::page::PageType::Free {
            panic!("Invalid page type for FreePage");
        }

        let free_page = FreePage { page };
        free_page
    }

    pub fn copy_page_body(&mut self, from: impl PageTrait, page_size: u64) {
        self.page.copy_page_body(from, page_size);
    }
}
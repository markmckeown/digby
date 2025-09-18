use std::i128;

use crate::page;
use crate::page::Page;
use crate::page::PageTrait;

pub struct HeadPage {
    page: Page
}

pub impl PageTrait for HeadPage {
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

pub impl HeadPage {
    

    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut head_page = HeadPage {
            page: Page::new(page_size),
        };
        head_page.page.set_type(PageType::Head);
        head_page.page.set_page_number(page_number);
        head_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::Head {
            panic!("Invalid page type for HeadPage");
        }

        let mut head_page = HeadPage { page };
        head_page
    }


}
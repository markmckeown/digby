use crate::page::{Page, PageTrait, PageType};


pub struct DataPage {
    page: Page
}

impl PageTrait for DataPage {
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

impl DataPage {
    pub fn new(page_size: u64, page_number: u32) -> Self {
        let mut page = Page::new(page_size);
        page.set_type(PageType::Data);
        page.set_page_number(page_number);          
        DataPage { page }   
    }

     pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::Data {
            panic!("Page type is not Data");
        }
        DataPage { page }
    }
}
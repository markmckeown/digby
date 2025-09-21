use crate::page::Page;
use crate::page::PageTrait;

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
}

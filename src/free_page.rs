use crate::block_layer::PageConfig;
use crate::page::Page;
use crate::page::PageTrait;

pub struct FreePage {
    page: Page,
}

impl PageTrait for FreePage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> u64 {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: u64) {
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



impl FreePage {
    pub fn create_new(page_config: &PageConfig, page_number: u64) -> Self {
        FreePage::new(page_config.block_size, page_config.page_size, page_number)
    }

    fn new(block_size: usize, page_size: usize, page_number: u64) -> Self {
        let mut free_page = FreePage {
            page: Page::new(block_size, page_size),
        };
        free_page.page.set_type(crate::page::PageType::Free);
        free_page.page.set_page_number(page_number);
        free_page
    }

    pub fn from_page(page: Page) -> Self {
        if page.get_type() != crate::page::PageType::Free {
            panic!("Invalid page type for FreePage");
        }

        FreePage { page }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageType;

    #[test]
    fn test_create_new() {
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let free_page = FreePage::create_new(&page_config, 42);
        
        assert_eq!(free_page.get_page_number(), 42);
        // We can access `page` through the trait method
        // but we know it's a FreePage type by successfully creating it
    }

    #[test]
    fn test_from_page_valid() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::Free);
        page.set_page_number(100);

        let free_page = FreePage::from_page(page);
        assert_eq!(free_page.get_page_number(), 100);
    }

    #[test]
    #[should_panic(expected = "Invalid page type for FreePage")]
    fn test_from_page_invalid() {
        let mut page = Page::new(4096, 4092);
        page.set_type(PageType::LeafPage); // Invalid type for FreePage

        let _free_page = FreePage::from_page(page);
    }

    #[test]
    fn test_page_trait_methods() {
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let mut free_page = FreePage::create_new(&page_config, 1);

        free_page.set_version(5);
        assert_eq!(free_page.get_version(), 5);

        assert_eq!(free_page.get_page_number(), 1);
        free_page.set_page_number(42);
        assert_eq!(free_page.get_page_number(), 42);

        let bytes = free_page.get_page_bytes();
        assert_eq!(bytes.len(), 4092);

        let page = free_page.get_page();
        assert_eq!(page.get_page_number(), 42);
    }
}
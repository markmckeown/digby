use crate::db_config::DbConfig;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page_no::PageNo;

pub struct FreePage {
    page: Page,
}

impl PageTrait for FreePage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> PageNo {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: PageNo) {
        assert!(page_no.get_pg_type() == crate::page::PageType::Free);
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
    pub fn create_new(page_config: &DbConfig, pg_no: PageNo) -> Self {
        FreePage::new(page_config.block_size, page_config.page_size, pg_no)
    }

    fn new(block_size: usize, page_size: usize, pg_no: PageNo) -> Self {
        assert!(pg_no.get_pg_type() == crate::page::PageType::Free);
        let mut free_page = FreePage {
            page: Page::new(block_size, page_size),
        };
        free_page.page.set_page_number(pg_no);
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
        let db_config = DbConfig::builder()
            .block_size(4096)
            .block_sanity_size(4)
            .build();
        let free_page = FreePage::create_new(&db_config, PageNo::new(PageType::Free, 0, 42));

        assert_eq!(free_page.get_page_number().get_blk_offset(), 42);
        // We can access `page` through the trait method
        // but we know it's a FreePage type by successfully creating it
    }

    #[test]
    #[should_panic(expected = "Invalid page type for FreePage")]
    fn test_from_page_invalid() {
        let page = Page::new(4096, 4092);
        let _free_page = FreePage::from_page(page);
    }

    #[test]
    fn test_page_trait_methods() {
        let db_config = DbConfig::builder()
            .block_size(4096)
            .block_sanity_size(4)
            .build();
        let mut free_page = FreePage::create_new(&db_config, PageNo::new(PageType::Free, 0, 10));

        free_page.set_version(5);
        assert_eq!(free_page.get_version(), 5);

        assert_eq!(free_page.get_page_number().get_blk_offset(), 10);
        free_page.set_page_number(PageNo::new(PageType::Free, 0, 42));
        assert_eq!(free_page.get_page_number().get_blk_offset(), 42);

        let bytes = free_page.get_page_bytes();
        assert_eq!(bytes.len(), 4092);

        let page = free_page.get_page();
        assert_eq!(page.get_page_number(), PageNo::new(PageType::Free, 0, 42));
    }
}

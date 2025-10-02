use crate::free_dir_page::FreeDirPage;
use crate::page::Page; 
use crate::page::PageTrait;
use crate::page_cache::PageCache;

pub struct FreePageTracker {
    free_dir_page: FreeDirPage,
    free_dir_page_no: u32,
    returned_pages: Vec<u32>,
    new_version: u64,
}

impl FreePageTracker {
    pub fn new(page: Page, new_version: u64) -> Self {
        let page_no = page.get_page_number();
        let free_dir_page = FreeDirPage::from_page(page);
        assert!(free_dir_page.get_version() < new_version);
        FreePageTracker{
            free_dir_page: free_dir_page,
            free_dir_page_no: page_no,
            returned_pages:  Vec::new(),
            new_version: new_version,
        }
    }

    pub fn get_free_page_no(&mut self, page_cache: &mut PageCache) -> u32 {
        if !self.free_dir_page.has_free_pages() {
            let mut new_free_pages: Vec<u32> = page_cache.create_new_pages(16);
            let new_free_page = new_free_pages.pop().unwrap();
            self.free_dir_page.add_free_pages(&new_free_pages);
            return new_free_page;
        }

        self.free_dir_page.get_free_page()
    }

    pub fn return_free_page_no(&mut self, page_no: u32) -> () {
        self.returned_pages.push(page_no);
    }

    pub fn get_free_dir_page(&mut self) ->  &mut FreeDirPage {
        assert!(self.free_dir_page.has_free_pages());
        let next_free_page_no = self.free_dir_page.get_free_page();

        self.free_dir_page.set_page_number(next_free_page_no);
        self.returned_pages.push(self.free_dir_page_no);
        self.free_dir_page.set_version(self.new_version);
        
        for page_no in &self.returned_pages { 
            assert!(!self.free_dir_page.is_full());
            self.free_dir_page.add_free_page(*page_no);
        }

        &mut self.free_dir_page
    }

}
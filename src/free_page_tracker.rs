use crate::free_dir_page::FreeDirPage;
use crate::page::Page; 
use crate::page::PageTrait;
use crate::page_cache::PageCache;

pub struct FreePageTracker {
    free_dir_page: FreeDirPage,
    free_dir_page_no: u32,
    returned_pages: Vec<u32>,
    new_version: u64,
    page_size: usize,
}

impl FreePageTracker {
    pub fn new(page: Page, new_version: u64, page_size: usize) -> Self {
        let page_no = page.get_page_number();
        let free_dir_page = FreeDirPage::from_page(page);
        assert!(free_dir_page.get_version() < new_version);
        FreePageTracker{
            free_dir_page: free_dir_page,
            free_dir_page_no: page_no,
            returned_pages:  Vec::new(),
            new_version: new_version,
            page_size: page_size,
        }
    }

    pub fn get_free_page_no(&mut self, page_cache: &mut PageCache) -> u32 {
        FreePageTracker::get_free_page_internal(&mut self.free_dir_page, page_cache)
    }

    fn get_free_page_internal(free_dir_page: &mut FreeDirPage, page_cache: &mut PageCache) -> u32 {
        if !free_dir_page.has_free_pages() {
            let mut new_free_pages: Vec<u32> = page_cache.create_new_pages(16);
            let new_free_page = new_free_pages.pop().unwrap();
            free_dir_page.add_free_pages(&new_free_pages);
            return new_free_page;
        }

        free_dir_page.get_free_page()
    }


    pub fn return_free_page_no(&mut self, page_no: u32) -> () {
        self.returned_pages.push(page_no);
    }

    pub fn get_free_dir_page(&mut self, page_cache: &mut PageCache) ->  Vec<FreeDirPage> {
        let next_free_page_no = FreePageTracker::get_free_page_internal(&mut self.free_dir_page, page_cache);

        self.free_dir_page.set_page_number(next_free_page_no);
        self.returned_pages.push(self.free_dir_page_no);
        self.free_dir_page.set_version(self.new_version);
        
        while let Some(page_no) = self.returned_pages.pop() {
            if self.free_dir_page.is_full() {
                break;
            }
            self.free_dir_page.add_free_page(page_no);
        }
    
        let first_page = FreeDirPage::from_bytes(self.free_dir_page.get_bytes().to_vec());
        let mut pages: Vec<FreeDirPage> = Vec::new();
        pages.push(first_page);
        while !self.returned_pages.is_empty() {
            let last = pages.last_mut().unwrap();
            // We know last must have entries
            let next_free_page_no = FreePageTracker::get_free_page_internal(last, page_cache);
            let mut next_free_dir_page = FreeDirPage::new(self.page_size as u64, next_free_page_no, self.new_version);
            next_free_dir_page.set_previous(last.get_page_number());
            last.set_next(next_free_dir_page.get_page_number());
            while let Some(page_no) = self.returned_pages.pop()  {
                if next_free_dir_page.is_full() {
                    break;
                }
                next_free_dir_page.add_free_page(page_no);
            }
            pages.push(next_free_dir_page);
        }

        pages
    }

}
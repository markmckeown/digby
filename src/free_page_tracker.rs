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

   
    pub fn get_free_page(&mut self, page_cache: &mut PageCache) -> u32 {
        // If the self.free_dir_page has free pages then use one of them.
        if self.free_dir_page.has_free_pages() {
            return self.free_dir_page.get_free_page();
        }
        
        // The self.free_dir_page has no free pages then check if it has
        // a link to another free_page_dir. 
        let next_free_dir_page_no = self.free_dir_page.get_next();
        if next_free_dir_page_no != 0 {
            // There is another free_dir_page, replace the self.free_dir_page
            // with next free_dir_page and put the previous into the list
            // pf returned pages.
            self.returned_pages.push(self.free_dir_page_no);
            // Update internal state.
            self.free_dir_page = FreeDirPage::from_page(page_cache.get_page(next_free_dir_page_no));
            self.free_dir_page_no = next_free_dir_page_no;
            // Now recursively call get_free_page
            return self.get_free_page(page_cache);
        }
        
        // The current free_dir_page has no free pages, it has no links
        // to other free_dir_pages - so have the page_cache generate
        // new free pages.
        let mut new_free_pages: Vec<u32> = page_cache.create_new_pages(16);
        let new_free_page = new_free_pages.pop().unwrap();
        self.free_dir_page.add_free_pages(&new_free_pages);
        return new_free_page;
    }


    pub fn return_free_page_no(&mut self, page_no: u32) -> () {
        self.returned_pages.push(page_no);
    }

    pub fn get_free_dir_pages(&mut self, page_cache: &mut PageCache) ->  Vec<FreeDirPage> {
        let next_free_page_no = self.get_free_page(page_cache);

        self.free_dir_page.set_page_number(next_free_page_no);
        self.returned_pages.push(self.free_dir_page_no);
        self.free_dir_page.set_version(self.new_version);
        
        while let Some(page_no) = self.returned_pages.pop() {
            if self.free_dir_page.is_full() {
                break;
            }
            self.free_dir_page.add_free_page(page_no);
        }
    
        // If we filled the free page directory page then we need to add more pages to hold
        // all the free pages. We build up a linked list of these.
        // Take a copy of the directory page - might be some more sane way to do this, but the tracker
        // will still have the old page.
        // We push full pages back and add the latest to the front.
        let first_page = FreeDirPage::from_bytes(self.free_dir_page.get_bytes().to_vec());
        let mut pages: Vec<FreeDirPage> = Vec::new();
        pages.push(first_page);
        while !self.returned_pages.is_empty() {
            let last = pages.last_mut().unwrap();
            // We create a new free page for the new free_page_dir page we need - we do not want to use a returned page
            // as that could cause corruption. Returned pages are still in use until the commit is complete.
            let next_free_page_no = *page_cache.create_new_pages(1).get(0).unwrap();
            let mut next_free_dir_page = FreeDirPage::new(self.page_size as u64, next_free_page_no, self.new_version);
            next_free_dir_page.set_next(last.get_page_number());
            last.set_previous(next_free_dir_page.get_page_number());
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
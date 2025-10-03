use crate::free_dir_page::FreeDirPage;
use crate::page::Page; 
use crate::page::PageTrait;
use crate::page_cache::PageCache;


// Track free pages for a commit. This will provide free page numbers
// for locations to write back pages. It will also record page numbers
// that are no longer being used that can be stored as free pages 
// when the commit completes.
//
// This object is created by giving it a free_dir_page that will have
// zero or more free_page_numbers. It may also link to other free_dir_page
// with more free pages. This is a linked list that this object needs to manage.
//
// During a commit this object will be given page numbers that are no longer
// needed. but they should NOT be reused within the commit or stuff will get
// corrupted.
// 
pub struct FreePageTracker {
    free_dir_page_list: Vec<FreeDirPage>,
    returned_pages: Vec<u32>,
    new_version: u64,
    page_size: usize,
}

impl FreePageTracker {
    // Provded with the free_dir_page, this could be linked to other
    // free_dir_page. We store it in a list, when the commit is
    // ready to complete this object has to provide any free_dir_pages
    // that need to be written back.
    //
    // After we provide the free_dir_pages the list is empty - this object
    // has completed its role and should not be used again. We test this
    // in methods by asserting the list is not empty.
    pub fn new(page: Page, new_version: u64, page_size: usize) -> Self {
        let free_dir_page = FreeDirPage::from_page(page);
        assert!(free_dir_page.get_version() < new_version);
        let mut list = Vec::new();
        list.push(free_dir_page);
        FreePageTracker{
            free_dir_page_list: list,
            returned_pages:  Vec::new(),
            new_version: new_version,
            page_size: page_size,
        }
    }

    // The commit wants a free page number it can assign to a page it wants
    // to write back. If there are no free pages in the system then this
    // object will have to ask the PageCache to create more free pages - this
    // is why the PageCache is provide as a parameter.
    pub fn get_free_page(&mut self, page_cache: &mut PageCache) -> u32 {
        assert!(!self.free_dir_page_list.is_empty());

        let last = self.free_dir_page_list.last_mut().unwrap();

        // If the last has free pages then use one of them.
        if last.has_free_pages() {
            return last.get_free_page();
        }
        
        // The last has no free pages then check if it has
        // a link to another free_page_dir. 
        let next_free_dir_page_no = last.get_next();
        if next_free_dir_page_no != 0 {
            // There is another free_dir_page, replace entry in the list with 
            // with next free_dir_page and put last into the list
            // of returned pages.
            self.returned_pages.push(last.get_page_number());
            self.free_dir_page_list.pop(); // The last page is now out of scope and no longer used.
            self.free_dir_page_list.push(FreeDirPage::from_page(page_cache.get_page(next_free_dir_page_no)));
            // Now recursively call get_free_page - the new page will have free page numbers
            // so it is gurantueed to work.
            return self.get_free_page(page_cache);
        }
        
        // The current free_dir_page has no free pages, it has no links
        // to other free_dir_pages - so have the page_cache generate
        // new free pages.
        let mut new_free_pages: Vec<u32> = page_cache.create_new_pages(16);
        // Reverse the free pages or we add at end of file first.
        new_free_pages.reverse();
        // Grab a free page number to return to the commit before adding to free_dir_page
        let new_free_page = new_free_pages.pop().unwrap();
        last.add_free_pages(&new_free_pages);
        return new_free_page;
    }


    pub fn get_return_pages(&self) -> Vec<u32> {
        self.returned_pages.clone()
    }

    // Commit no long needs this page no. It should be recycled for the next
    // commit and should not be used in this commit.
    pub fn return_free_page_no(&mut self, page_no: u32) -> () {
        assert!(!self.free_dir_page_list.is_empty());
        self.returned_pages.push(page_no);
    }

    // The commit wants to write back the free_dir_page - no more free page no will be
    // required. So we add the returned pages into the free_dir_page. If there are more
    // free page numbers that will fit into the free_dir_page then we need to create
    // new free pages to store them. We do not want to reuse those free page numbers
    // in this commit. 
    // Any new free_dir_pages created will be linked together.
    pub fn get_free_dir_pages(&mut self, page_cache: &mut PageCache) ->  Vec<FreeDirPage> {
        assert!(self.free_dir_page_list.len() == 1);

        let next_free_page_no = self.get_free_page(page_cache);
        let mut last = self.free_dir_page_list.last_mut().unwrap();
        // Get a free_page_no for last to be written to.
        self.returned_pages.push(last.get_page_number());
        last.set_page_number(next_free_page_no);
        last.set_version(self.new_version);
        
        // Add all the returned page numbers to the free_dir_page last.
        while let Some(page_no) = self.returned_pages.pop() {
            if last.is_full() {
                break;
            }
            last.add_free_page(page_no);
        }
    
        // If there are still free page numbers to be added then need to create
        // new free_dir_pages to add them to and link them to existing free_dir_pages.
        while !self.returned_pages.is_empty() {
            // We create a new free page for the new free_page_dir page we need - we do not want to use a returned page no
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
            self.free_dir_page_list.push(next_free_dir_page);
            last = self.free_dir_page_list.last_mut().unwrap();
        }

        let mut pages: Vec<FreeDirPage> = Vec::new();
        // Move all the free_dir_pages into the new Vec, the vec in the object is now empty
        // and any attempt to use it will cause a panic
        pages.append(&mut self.free_dir_page_list);
        return pages
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_remove_pages() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_file).expect("Failed to open or create DB file");

        let version = 0;
        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, crate::Db::PAGE_SIZE);
        let block_layer: crate::BlockLayer = crate::BlockLayer::new(file_layer, crate::Db::PAGE_SIZE);
        let mut page_cache: PageCache = PageCache::new(block_layer, crate::Db::PAGE_SIZE);

        let free_dir_page_no = *page_cache.create_new_pages(1).get(0).unwrap();
        let mut free_dir_page = FreeDirPage::new(crate::Db::PAGE_SIZE, free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let mut free_page_tracker = FreePageTracker::new(
            page_cache.get_page(free_dir_page_no), version + 1, crate::Db::PAGE_SIZE as usize);

        let new_free_page = free_page_tracker.get_free_page(&mut page_cache);
        assert!(new_free_page == 1);
        assert!(page_cache.get_total_page_count() == 17);

        for number in 16u32..=5000 {
            free_page_tracker.return_free_page_no(number);
        }
        assert!(page_cache.get_total_page_count() == 17);
        let mut pages = free_page_tracker.get_free_dir_pages(&mut page_cache);
        assert!(pages.len() == 5);
        assert!(page_cache.get_total_page_count() == 17 + 4);

        let free_page_dir_no = pages.last().unwrap().get_page_number();
        while !pages.is_empty() {
            page_cache.put_page(pages.pop().unwrap().get_page());
        }

        free_page_tracker = FreePageTracker::new(
            page_cache.get_page(free_page_dir_no), version + 2, crate::Db::PAGE_SIZE as usize);

        // Thre are five pages of free page numbers - going to use 2100 of them
        for _number in 1u32..=2100 {
            free_page_tracker.get_free_page(&mut page_cache);
        }
        assert!(page_cache.get_total_page_count() == 17 + 4);
        // Two of the free_page_dir are no longer needed.
        assert!(free_page_tracker.get_return_pages().len() == 2);
        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }
}
use crate::block_layer::{PageContainerLayer, PageConfig};
use crate::page::Page;
use crate::page::PageTrait;
use crate::page_no::PageNo;
use std::collections::HashMap;
use std::collections::VecDeque;

pub struct PageCache {
    block_layer: PageContainerLayer,
    page_map: HashMap<PageNo, Page>,
    deque: VecDeque<PageNo>,
    cache_size_limit: usize,
}

impl PageCache {
    pub fn new(block_layer: PageContainerLayer) -> Self {
        PageCache {
            block_layer,
            page_map: HashMap::new(),
            deque: VecDeque::new(),
            cache_size_limit: 1024usize,
        }
    }

    pub fn get_page_config(&self) -> &PageConfig {
        self.block_layer.get_page_config()
    }

    // Generate free pages on disk that can be written back to. Returns
    // a list of page numbers.
    pub fn generate_free_pages(&mut self, no_new_pages: u64, block_cnt_exp: u8) -> Vec<PageNo> {
        self.block_layer.generate_free_pages(no_new_pages, block_cnt_exp)
    }

    // This returns a newly created page at the block layer. So each
    // client would get their own copy of the page. In future there
    // maybe two versions of this method, one that returns an
    // immutable refernce to a page that is shared, and a version
    // that returns a copy of the page.
    pub fn get_page(&mut self, page_number: PageNo) -> Page {
        match self.page_map.get(&page_number) {
            Some(page) => {
                let mut page_copy = Page::create_new(self.get_page_config());
                page_copy
                    .get_block_bytes_mut()
                    .copy_from_slice(page.get_block_bytes());
                page_copy
            }
            None => {
                let page = self.block_layer.read_page(page_number);
                let mut page_for_cache = Page::create_new(self.get_page_config());
                page_for_cache
                    .get_block_bytes_mut()
                    .copy_from_slice(page.get_block_bytes());
                self.add_page_to_cache(page_number, page_for_cache);
                page
            }
        }
    }

    pub fn get_page_ref(&mut self, page_number: PageNo) -> &Page {
        if self.page_map.contains_key(&page_number) {
            return self.page_map.get(&page_number).unwrap();
        }

        let new_page = self.block_layer.read_page(page_number);
        self.add_page_to_cache(page_number, new_page);
        self.page_map.get(&page_number).unwrap()
    }

    fn add_page_to_cache(&mut self, page_no: PageNo, page: Page) {
        if self.page_map.insert(page_no, page).is_none() {
            // Added new page. Add to the dequeue and if it overflows
            // delete an entry.
            self.deque.push_back(page_no);
            if self.deque.len() > self.cache_size_limit {
                let page_to_delete = self.deque.pop_front().unwrap();
                self.page_map.remove(&page_to_delete);
            }
        }
    }

    pub fn put_page(&mut self, page: &mut Page) {
        let page_no = page.get_page_number();
        // Take a copy of the page before the block_layer processes it,
        // the block layer might encrypt it.
        // TODO - block_layer.write_page should return the page to us to avoid need to copy.
        let mut page_for_cache = Page::create_new(self.get_page_config());
        page_for_cache
            .get_block_bytes_mut()
            .copy_from_slice(page.get_block_bytes());
        self.block_layer.write_page(page, page_no);
        self.add_page_to_cache(page_no, page_for_cache);
    }

    pub fn get_total_page_count(&self) -> u64 {
        self.block_layer.get_total_page_count()
    }

    pub fn sync_data(&mut self) {
        self.block_layer.sync_data()
    }

    pub fn sync_all(&mut self) {
        self.block_layer.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        file_layer::FileLayer,
        page::{self, PageTrait},
    };
    use tempfile::tempfile;
    const PAGE_SIZE: usize = 4096;

    #[test]
    fn test_page_cache_read_write() {
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, PAGE_SIZE);
        let block_layer = PageContainerLayer::new(file_layer, PAGE_SIZE);
        let mut page_cache = PageCache::new(block_layer);
        let page_number = 0;

        // Write a page to the cache
        let mut page = Page::create_new(page_cache.get_page_config());
        page_cache.generate_free_pages(10, 0);
        page.set_page_number(page_number);
        page.set_type(page::PageType::Free);
        page_cache.put_page(&mut page);
        page_cache.sync_all();
        // Read the page back from the cache
        let read_page = page_cache.get_page(PageNo::from_u64(page_number));
        assert_eq!(read_page.get_page_number().to_u64(), page_number);
        assert_eq!(read_page.get_page_bytes(), page.get_page_bytes());
    }
}

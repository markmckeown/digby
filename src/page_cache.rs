use crate::block_layer::{BlockLayer, PageConfig};
use crate::page::Page;

pub struct PageCache {
    block_layer: BlockLayer,
}

// PageCache does not cache any pages - any gets are retrieved from
// disk and any puts go to disk.
// In the future when it does hold a cache of pages then need to think
// about mutability. Any client doing a look up can get a  immutable
// reference to a page, any client looking to make changes can get a
// copy of the page.
impl PageCache {
    pub fn new(block_layer: BlockLayer) -> Self {
        PageCache { block_layer }
    }

    pub fn get_page_config(&self) -> &PageConfig {
        self.block_layer.get_page_config()
    }

    // Generate free pages on disk that can be written back to. Returns
    // a list of page numbers.
    pub fn generate_free_pages(&mut self, no_new_pages: u64) -> Vec<u64> {
        self.block_layer.generate_free_pages(no_new_pages)
    }

    // This returns a newly created page at the block layer. So each
    // client would get their own copy of the page. In future there
    // maybe two versions of this method, one that returns an
    // immutable refernce to a page that is shared, and a version
    // that returns a copy of the page.
    pub fn get_page(&mut self, page_number: u64) -> Page {
        self.block_layer.read_page(page_number)
    }

    pub fn put_page(&mut self, page: &mut Page) {
        self.block_layer.write_page(page);
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
        let file_layer = FileLayer::new(temp_file, PAGE_SIZE as usize);
        let block_layer = BlockLayer::new(file_layer, PAGE_SIZE as usize);
        let mut page_cache = PageCache::new(block_layer);
        let page_number = 0;

        // Write a page to the cache
        let mut page = Page::create_new(page_cache.get_page_config());
        page_cache.generate_free_pages(10);
        page.set_page_number(page_number);
        page.set_type(page::PageType::Free);
        page_cache.put_page(&mut page);
        page_cache.sync_all();
        // Read the page back from the cache
        let read_page = page_cache.get_page(page_number);
        assert_eq!(read_page.get_page_number(), page_number);
        assert_eq!(read_page.get_page_bytes(), page.get_page_bytes());
    }
}

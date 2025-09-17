use crate::block_layer::BlockLayer;
use crate::page::Page;


pub struct PageCache {
    block_layer: BlockLayer,
    page_size: u64,
}

impl PageCache {
    pub fn new(block_layer: BlockLayer, page_size: u64) -> Self {
        PageCache { block_layer, page_size }
    }

    pub fn read_page(&mut self, page_number: u32) -> Page {
        self.block_layer.get_page(page_number, self.page_size)
    }

    pub fn write_page(&mut self, page: &mut Page) -> () {
        self.block_layer.put_page(page);
    }

    pub fn sync_data(&mut self) -> () {
        self.block_layer.sync_data()
    }

    pub fn sync_all(&mut self) -> () {
        self.block_layer.sync_all()
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::{file_layer::FileLayer, page::PageTrait};
    use tempfile::tempfile;
    const PAGE_SIZE: u64 = 4096;
    

    #[test]
    fn test_page_cache_read_write() {
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file);
        let block_layer = BlockLayer::new(file_layer);
        let mut page_cache = PageCache::new(block_layer, PAGE_SIZE);
        let page_number = 0;

        // Write a page to the cache
        let mut page = Page::new(PAGE_SIZE);
        page.set_page_number(page_number);
        page_cache.write_page(&mut page);
        page_cache.sync_all();
        // Read the page back from the cache
        let mut read_page = page_cache.read_page(page_number);
        assert_eq!(read_page.get_page_number(), page_number);
        assert_eq!(read_page.get_bytes(), page.get_bytes());
    }
    
}

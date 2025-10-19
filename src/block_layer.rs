use crate::file_layer::FileLayer;
use crate::page::Page; 
use crate::page::PageTrait;
use crate::block_sanity::BlockSanity;



#[derive(Copy, Clone)]
pub struct PageConfig {
    pub block_size: usize,
    pub page_size: usize,
}

pub struct BlockLayer {
    file_layer: FileLayer,
    page_config: PageConfig,
    block_sanity: BlockSanity,
    key: Vec<u8>,
}

impl BlockLayer {
    pub fn new(file_layer: FileLayer, block_size: usize) -> Self {
        BlockLayer { 
            file_layer, 
            block_sanity: BlockSanity::XxH32Checksum,
            page_config: PageConfig { 
                block_size: block_size, 
                page_size:  block_size - BlockSanity::get_bytes_used(BlockSanity::XxH32Checksum)
            },
            key: Vec::new(),
        }
    }

    pub fn new_with_key(file_layer: FileLayer, block_size: usize, key: Vec<u8>) -> Self {
        let mut enc_key = vec![0u8; 16];
        // Note we only use the first 16 bytes of the key for AES-128-GCM
        if key.len() >= 16 {
            enc_key.copy_from_slice(&key[0 .. 16]);
        } else {
            // If the key is less than 16 bytes, pad with zeros
            enc_key[0 .. key.len()].copy_from_slice(&key[..]);
        }
        BlockLayer { 
            file_layer, 
            block_sanity: BlockSanity::Aes128Gcm,
            page_config: PageConfig { 
                block_size: block_size, 
                page_size:  block_size - BlockSanity::get_bytes_used(BlockSanity::Aes128Gcm)
            },
            key: enc_key,
        }
    }


    pub fn get_page_config(&self) -> &PageConfig {
        return &self.page_config
    }

    pub fn read_page(&mut self, page_number: u32) -> Page {
        let mut page = Page::create_new(&self.page_config);
        self.file_layer.read_page_from_disk(&mut page, page_number).expect("Failed to read page");
        self.check_sanity(&mut page);
        page
    }

    pub fn get_total_page_count(&self) -> u32 {
        self.file_layer.get_page_count()
    }

    pub fn write_page(&mut self, page: &mut Page) -> () {
        let page_number = page.get_page_number();
        assert!(page_number < self.file_layer.get_page_count(), "Writing page outside the file.");

        self.set_sanity(page);
        self.file_layer.write_page_to_disk(page, page_number).expect("Failed to write page");
    }

    pub fn generate_free_pages(&mut self, no_new_pages: u32) -> Vec<u32> {
        let existing_page_count = self.file_layer.get_page_count();
        let mut created_page_nos: Vec<u32> = Vec::new();
        for new_page_no in existing_page_count..existing_page_count + no_new_pages {
            let mut page = Page::create_new(&self.page_config);
            page.set_page_number(new_page_no);
            page.set_type(crate::page::PageType::Free);
            self.set_sanity(&mut page);
            created_page_nos.push(new_page_no);
            self.file_layer.append_new_page(&mut page, new_page_no);
        }
        // Sync the file and file metadata.
        self.file_layer.sync_all();
        created_page_nos
    }

    fn set_sanity(&self, page: &mut Page) -> () {
        self.block_sanity.set_block_sanity(page, &self.key);
    }

    fn check_sanity(&self, page: &mut Page) -> () {
        self.block_sanity.check_block_sanity(page, &self.key);
    }

    pub fn sync_data(&mut self) -> () {
        self.file_layer.sync_data();
        ()
    }

    pub fn sync_all(&mut self) -> () {
        self.file_layer.sync_all();
        ()
    }
}   

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_layer::FileLayer;
    use crate::page::{Page, PageType};
    use crate::DbMasterPage;
    use tempfile::tempfile; 

    #[test]
    fn test_block_layer_put_get() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        let mut block_layer = BlockLayer::new(file_layer, block_size);
        let page_number = 0;
        block_layer.generate_free_pages(10);
        let mut page = Page::create_new(block_layer.get_page_config());
        page.set_page_number(page_number);
        page.set_type(PageType::Free);
        page.get_page_bytes_mut()[40..44].copy_from_slice(&[1, 2, 3, 4]); // Sample data
        block_layer.write_page(&mut page);
        let retrieved_page = block_layer.read_page(page_number);
        assert_eq!(&retrieved_page.get_page_bytes()[40..44], &[1, 2, 3, 4]);
    }


    #[test]
    fn test_create_new_pages() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        let mut block_layer = BlockLayer::new(file_layer, block_size);
        let mut free_pages = block_layer.generate_free_pages(1);
        assert!(free_pages.len() == 1);
        free_pages = block_layer.generate_free_pages(2);
        assert!(free_pages.len() == 2);
        free_pages = block_layer.generate_free_pages(5);
        assert!(free_pages.len() == 5);
    }

    #[test]
    fn test_create_header_page() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        let mut block_layer = BlockLayer::new(file_layer, block_size);
        let mut page = DbMasterPage::create_new(block_layer.get_page_config(), 0, 0);
        block_layer.generate_free_pages(1);
        block_layer.write_page(page.get_page());
    }


}
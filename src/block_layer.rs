use crate::block_sanity::BlockSanity;
use crate::file_layer::FileLayer;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page_no::PageNo;

// The page container layer sits above the file layer.
// Data is stored to the file as blocks, the blocks
// contain pages.
//
// A page container contains a page and is made up of
// one or more file blocks, along with the page it
// contains either a checksum for the page or the
// encryption information for the page. 
//
// Everything above the page container layer works in pages,
// the file_layer works in blocks and the page_container_layer
// maps between blocks and pages.
//
// The page is stored at the start of the page container, the 
// checksum/encryption information is stored at the end of the page container.
//
// | Page | Checksum/Encryption Bytes |
//
// The amount of bytes used for checkum/encryption
// depends on the BlockSanity used. 4 bytes for a
// xxhash_32 hash of the page bytes, 28 bytes for
// AES-128-GCM encryption of the page. 
//
// The file block size is determined at DB creation time,
// on Linux 4096 bytes can be sent to disk atomically -
// there is recent support for untorn writes that could
// support 16K writes atomically. 
//
// The page container layer is also respnsible for generating
// free pages.
//

// This struct is used to communicate to anything that is
// interested what the block size and page size is.
#[derive(Copy, Clone)]
pub struct PageConfig {
    pub block_size: usize,
    pub page_size: usize,
}

pub struct PageContainerLayer {
    file_layer: FileLayer,
    page_config: PageConfig,
    block_sanity: BlockSanity,
    key: Vec<u8>, // The encryption key if encryption is being used.
}

impl PageContainerLayer {
    pub fn new(file_layer: FileLayer, block_size: usize) -> Self {
        PageContainerLayer {
            file_layer,
            block_sanity: BlockSanity::XxH32Checksum,
            page_config: PageConfig {
                block_size,
                page_size: block_size - BlockSanity::get_bytes_used(BlockSanity::XxH32Checksum),
            },
            key: Vec::new(),
        }
    }

    pub fn new_with_key(file_layer: FileLayer, block_size: usize, key: Vec<u8>) -> Self {
        let mut enc_key = vec![0u8; 16];
        // Note we only use the first 16 bytes of the key for AES-128-GCM
        if key.len() >= 16 {
            enc_key.copy_from_slice(&key[0..16]);
        } else {
            // If the key is less than 16 bytes, pad with zeros
            enc_key[0..key.len()].copy_from_slice(&key[..]);
        }
        PageContainerLayer {
            file_layer,
            block_sanity: BlockSanity::Aes128Gcm,
            page_config: PageConfig {
                block_size,
                page_size: block_size - BlockSanity::get_bytes_used(BlockSanity::Aes128Gcm),
            },
            key: enc_key,
        }
    }

    pub fn get_page_config(&self) -> &PageConfig {
        &self.page_config
    }

    pub fn read_page(&mut self, page_number: u64) -> Page {
        let page_no = PageNo::from_u64(page_number);
        let mut page = Page::create_new(&self.page_config);
        self.file_layer
            .read_page_from_disk(&mut page, &page_no)
            .expect("Failed to read page");
        self.check_sanity(&mut page);
        page
    }

    pub fn get_total_page_count(&self) -> u64 {
        self.file_layer.get_block_count()
    }

    pub fn write_page(&mut self, page: &mut Page, page_number: u64) {
        let page_no = PageNo::from_u64(page_number);
        assert!(
            page_no.get_file_blk_offset() < self.file_layer.get_block_count(),
            "Writing page outside the file."
        );

        self.set_sanity(page);
        self.file_layer
            .write_page_to_disk(page, &page_no)
            .expect("Failed to write page");
    }

    // There has been a request for more free pages during a commit - there are
    // no free pages in the system. This will initialise the pages (possibly not
    // needed and a waste of time) and extend the file with a sync - note, that
    // if the commit does not complete then these pages will be leaked.
    pub fn generate_free_pages(&mut self, no_new_pages: u64, _block_cnt_exp: u8) -> Vec<u64> {
        // Get the file block offset.
        // Create new page_container with required number of blocks.
        // Set page number - block offset & block count.
        // Set page sanity.
        // Append new page.
        // Get new file block offset - repeat
        let existing_page_count = self.file_layer.get_block_count();
        let mut created_page_nos: Vec<u64> = Vec::new();
        for new_page_no in existing_page_count..existing_page_count + no_new_pages {
            let mut page = Page::create_new(&self.page_config);
            page.set_page_number(new_page_no);
            page.set_type(crate::page::PageType::Free);
            self.set_sanity(&mut page);
            created_page_nos.push(new_page_no);
            self.file_layer.append_new_page(&page, &PageNo::from_u64(new_page_no));
        }
        // Sync the file and file metadata.
        self.file_layer.sync_all();
        created_page_nos
    }

    fn set_sanity(&self, page: &mut Page) {
        self.block_sanity.set_block_sanity(page, &self.key);
    }

    fn check_sanity(&self, page: &mut Page) {
        self.block_sanity.check_block_sanity(page, &self.key);
    }

    pub fn sync_data(&mut self) {
        self.file_layer.sync_data();
    }

    pub fn sync_all(&mut self) {
        self.file_layer.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbRootPage;
    use crate::file_layer::FileLayer;
    use crate::page::{Page, PageType};
    use tempfile::tempfile;

    #[test]
    fn test_block_layer_put_get() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        let mut block_layer = PageContainerLayer::new(file_layer, block_size);
        let page_number = 0;
        block_layer.generate_free_pages(10, 0);
        let mut page = Page::create_new(block_layer.get_page_config());
        page.set_page_number(page_number);
        page.set_type(PageType::Free);
        page.get_page_bytes_mut()[40..44].copy_from_slice(&[1, 2, 3, 4]); // Sample data
        block_layer.write_page(&mut page, page_number);
        let retrieved_page = block_layer.read_page(page_number);
        assert_eq!(&retrieved_page.get_page_bytes()[40..44], &[1, 2, 3, 4]);
    }

    #[test]
    fn test_block_layer_put_get_encrypted() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        // Use oversized key to test that only the first 16 bytes are used for AES-128-GCM
        let key = [0u8; 32].to_vec(); // Key for AES-128-GCM
        let mut block_layer = PageContainerLayer::new_with_key(file_layer, block_size, key);
        let page_number = 0;
        block_layer.generate_free_pages(10, 0);
        let mut page = Page::create_new(block_layer.get_page_config());
        page.set_page_number(page_number);
        page.set_type(PageType::Free);
        page.get_page_bytes_mut()[40..44].copy_from_slice(&[1, 2, 3, 4]); // Sample data
        block_layer.write_page(&mut page, page_number);
        let retrieved_page = block_layer.read_page(page_number);
        assert_eq!(&retrieved_page.get_page_bytes()[40..44], &[1, 2, 3, 4]);
    }

    #[test]
    fn test_block_layer_put_get_encrypted_small_key() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        // Use undersized key to test that only the first 16 bytes are used for AES-128-GCM
        let key = [0u8; 8].to_vec(); // Key for AES-128-GCM
        let mut block_layer = PageContainerLayer::new_with_key(file_layer, block_size, key);
        let page_number = 0;
        block_layer.generate_free_pages(10, 0);
        let mut page = Page::create_new(block_layer.get_page_config());
        page.set_page_number(page_number);
        page.set_type(PageType::Free);
        page.get_page_bytes_mut()[40..44].copy_from_slice(&[1, 2, 3, 4]); // Sample data
        block_layer.write_page(&mut page, page_number);
        let retrieved_page = block_layer.read_page(page_number);
        assert_eq!(&retrieved_page.get_page_bytes()[40..44], &[1, 2, 3, 4]);
    }

    #[test]
    #[should_panic(expected = "Writing page outside the file.")]
    fn test_block_out_side_page_range() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        let mut block_layer = PageContainerLayer::new(file_layer, block_size);
        let mut page = Page::create_new(block_layer.get_page_config());
        page.set_page_number(4);
        page.set_type(PageType::Free);
        // This should panic as out of range of file.
        block_layer.write_page(&mut page, 4);
    }

    #[test]
    fn test_create_new_pages() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        let mut block_layer = PageContainerLayer::new(file_layer, block_size);
        let mut free_pages = block_layer.generate_free_pages(1, 0);
        assert!(free_pages.len() == 1);
        free_pages = block_layer.generate_free_pages(2, 0);
        assert!(free_pages.len() == 2);
        free_pages = block_layer.generate_free_pages(5, 0);
        assert!(free_pages.len() == 5);
    }

    #[test]
    fn test_create_root_page() {
        let block_size: usize = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, block_size);
        let mut block_layer = PageContainerLayer::new(file_layer, block_size);
        let mut page = DbRootPage::create_new(block_layer.get_page_config());
        block_layer.generate_free_pages(1, 0);
        block_layer.write_page(page.get_page(), 0);
    }
}

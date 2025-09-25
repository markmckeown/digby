use crate::file_layer::FileLayer; 
use crate::page::Page; 
use crate::page::PageTrait;
use xxhash_rust::xxh32::xxh32;
use byteorder::LittleEndian;
use std::io::Cursor;
use byteorder::{ReadBytesExt, WriteBytesExt};

pub struct BlockLayer {
    file_layer: FileLayer,
    page_size: u64
}

impl BlockLayer {
    pub fn new(file_layer: FileLayer, page_size: u64) -> Self {
        BlockLayer { 
            file_layer, 
            page_size 
        }
    }

    pub fn read_page(&mut self, page_number: u32, page_size: u64) -> Page {
        let mut page = Page::new(page_size);
        self.file_layer.read_page_from_disk(&mut page, page_number).expect("Failed to read page");
        self.verify_checksum(&mut page);
        page
    }

    pub fn write_page(&mut self, page: &mut Page) -> Vec::<u32> {
        let page_number = page.get_page_number();
        let mut new_pages = Vec::<u32>::new();
        if page_number + 1 > self.file_layer.get_page_count() {
           self.create_new_pages(page_number, &mut new_pages);
        }

        self.set_checksum(page);
        self.file_layer.write_page_to_disk(page, page_number).expect("Failed to write page");
        // Remove this page from the free pages if we added free pages and have overwritten it.
        new_pages.retain(|&x| x != page_number);
        new_pages
    }

    pub fn create_new_pages(&mut self, page_number: u32, new_pages:&mut Vec<u32>) {
        let existing_page_count = self.file_layer.get_page_count();
        for new_page_no in existing_page_count..=page_number {
            let mut page = Page::new(self.page_size);
            page.set_page_number(new_page_no);
            page.set_type(crate::page::PageType::Free);
            self.set_checksum(&mut page);
            new_pages.push(new_page_no);
            self.file_layer.append_new_page(&mut page, new_page_no);
        }
        // Sync the file and file metadata.
        self.file_layer.sync();
    }


    fn set_checksum(&mut self, page: &mut Page) {
        let checksum = self.generate_checksum(page);
        let mut cursor = Cursor::new(page.get_bytes_mut());
        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum as u32).expect("Failed to write checksum");
    }   

    fn generate_checksum(&self, page: &Page) -> u32 {
        xxh32(&page.get_bytes()[4..], 0)
    }


    fn get_checksum(&self, page: &Page) -> u32 {
        let mut cursor = std::io::Cursor::new(page.get_bytes());
        cursor.set_position(0);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    fn verify_checksum(&self, page: &mut Page) -> () {
        let stored_checksum = self.get_checksum(page);
        let calculated_checksum = self.generate_checksum(page);
        if stored_checksum != calculated_checksum {
            panic!("Checksum mismatch: stored {}, calculated {} for page {}", stored_checksum, calculated_checksum, page.get_page_number());
        }
    }

    pub fn sync_data(&mut self) -> () {
        self.file_layer.sync_data();
        ()
    }

    pub fn sync_all(&mut self) -> () {
        self.file_layer.sync();
        ()
    }
}   

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_layer::FileLayer;
    use crate::page::Page;
    use crate::HeadPage;
    use tempfile::tempfile; 

    #[test]
    fn test_block_layer_put_get() {
        let page_size = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, page_size);
        let mut block_layer = BlockLayer::new(file_layer, page_size);
        let page_number = 0;
        let mut page = Page::new(page_size);
        page.set_page_number(page_number);
        page.get_bytes_mut()[40..44].copy_from_slice(&[1, 2, 3, 4]); // Sample data
        block_layer.write_page(&mut page);
        let retrieved_page = block_layer.read_page(page_number, page_size as u64);
        assert_eq!(&retrieved_page.get_bytes()[40..44], &[1, 2, 3, 4]);
    }


    #[test]
    fn test_create_new_pages() {
        let page_size = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, page_size);
        let mut block_layer = BlockLayer::new(file_layer, page_size);
        let page_number = 0;
        let mut free_pages = Vec::<u32>::new();
        block_layer.create_new_pages(page_number, &mut free_pages);
        assert!(free_pages.len() == 1);
        block_layer.create_new_pages(page_number + 1, &mut free_pages);
        assert!(free_pages.len() == 2);
        block_layer.create_new_pages(4, &mut free_pages);
        assert!(free_pages.len() == 5);
    }

    #[test]
    fn test_create_header_page() {
        let page_size = 4096;
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file, page_size);
        let mut block_layer = BlockLayer::new(file_layer, page_size);
        let mut page = HeadPage::new(page_size, 0, 0);
        let free_pages = block_layer.write_page(page.get_page());
        assert!(free_pages.len() == 0);
    }


}
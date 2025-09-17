use crate::file_layer::FileLayer; 
use crate::page::Page; 
use crate::page::PageTrait;
use xxhash_rust::xxh32::xxh32;
use byteorder::LittleEndian;
use std::io::Cursor;
use byteorder::{ReadBytesExt, WriteBytesExt};

pub struct BlockLayer {
    file_layer: FileLayer,
}

impl BlockLayer {
    pub fn new(file_layer: FileLayer) -> Self {
        BlockLayer { file_layer }
    }

    pub fn get_page(&mut self, page_number: u32, page_size: u64) -> Page {
        let mut page = Page::new(page_size);
        self.file_layer.read_page_from_disk(&mut page, page_number).expect("Failed to read page");
        self.verify_checksum(&mut page);
        page
    }

    pub fn put_page(&mut self, page: &mut Page) {
        let page_number = page.get_page_number();
        self.set_checksum(page);
        self.file_layer.write_page_to_disk(page, page_number).expect("Failed to write page");
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
}   

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_layer::FileLayer;
    use crate::page::Page;
    use tempfile::tempfile; 

    #[test]
    fn test_block_layer_put_get() {
        let temp_file = tempfile().expect("Failed to create temp file");
        let file_layer = FileLayer::new(temp_file);
        let mut block_layer = BlockLayer::new(file_layer);
        let page_size = 4096;
        let page_number = 0;
        let mut page = Page::new(page_size);
        page.set_page_number(page_number);
        page.get_bytes_mut()[40..44].copy_from_slice(&[1, 2, 3, 4]); // Sample data
        block_layer.put_page(&mut page);
        let retrieved_page = block_layer.get_page(page_number, page_size as u64);
        assert_eq!(&retrieved_page.get_bytes()[40..44], &[1, 2, 3, 4]);
    }
}
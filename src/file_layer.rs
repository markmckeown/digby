use crate::page::Page;

pub struct FileLayer {
    file: std::fs::File,
    block_size: usize,
    block_count: u32,
}


impl FileLayer {
    pub fn new(file: std::fs::File, block_size: usize) -> Self {
        let metadata = file.metadata().expect("Failed to get metadata for file.");
        let file_size = metadata.len();
        assert!(file_size % block_size as u64 == 0, "File size is not a multiple of page size.");
        let block_count : u32 = (file_size / block_size as u64).try_into().unwrap();
        FileLayer { 
            file,
            block_size,
            block_count
        }
    }

    pub fn get_page_count(&self) -> u32 {
        self.block_count
    }

    pub fn append_new_page(&mut self, page: &Page, page_number: u32) -> () {
        use std::io::{Seek, SeekFrom, Write};
        assert!(page_number == self.block_count, "page_number should match page_count");
        let offset = page_number as u64 * self.block_size as u64;
        self.file.seek(SeekFrom::Start(offset)).expect("Failed to seek for append_new_page");
        self.file.write_all(&page.get_block_bytes()).expect("Failed to write for append_new_page");
        self.block_count = self.block_count + 1;
    }

    pub fn write_page_to_disk(&mut self, page: &Page, page_number: u32) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let offset = page_number as u64 * self.block_size as u64;
        self.file.seek(SeekFrom::Start(offset)).expect("Failed to seek for write_page_to_disk");
        self.file.write_all(&page.get_block_bytes()).expect("Failed to write for write_page_to_disk");
        Ok(())
    }

    pub fn read_page_from_disk(&mut self, page: &mut Page, page_number: u32) -> std::io::Result<()> {
        assert!(page_number < self.block_count);
        use std::io::{Read, Seek, SeekFrom};

        let offset = page_number as u64 * self.block_size as u64;
        self.file.seek(SeekFrom::Start(offset)).expect("Failed to seek for read");
        self.file.read_exact(page.get_block_bytes_mut()).expect("Failed to read");
        Ok(())
    }

    pub fn sync_all(&self)  {
        self.file.sync_all().expect("Failed to sync")
    }

    pub fn sync_data(&self) {
        self.file.sync_data().expect("Failed to sync data")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const BLOCK_SIZE: usize = 4096;
    use tempfile::tempfile;   
    use rand::Rng;
    use rand::distr::Alphanumeric;

    #[test]
    fn test_file_layer_write_and_read() {
        let temp_file = tempfile().expect("Failed to create temp file");
        let mut file_layer = FileLayer::new(temp_file, BLOCK_SIZE);
        let mut page = Page::new(BLOCK_SIZE, BLOCK_SIZE - 4); // Create a new page
        file_layer.append_new_page(&page, 0);
        let test_data: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(BLOCK_SIZE as usize)
            .map(char::from)
            .collect();
        page.get_block_bytes_mut().copy_from_slice(test_data.as_bytes()); // Fill the page with test data

        // Write the page to disk
        file_layer.write_page_to_disk(&mut page, 0).expect("Failed to write page");

        // Read the page back from disk
        let mut read_page = Page::new(BLOCK_SIZE, BLOCK_SIZE);
        file_layer.read_page_from_disk(&mut read_page, 0).expect("Failed to read page");

        // Verify that the read data matches the written data
        assert_eq!(page.get_block_bytes(), read_page.get_block_bytes());
    }
}
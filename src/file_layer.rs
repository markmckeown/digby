use crate::page::Page;
use crate::page_no::PageNo;

pub struct FileLayer {
    file: std::fs::File,
    block_size: usize,
    block_count: u64,
}

impl FileLayer {
    pub fn new(file: std::fs::File, block_size: usize) -> Self {
        let metadata = file.metadata().expect("Failed to get metadata for file.");
        let file_size = metadata.len();
        assert!(
            file_size.is_multiple_of(block_size as u64),
            "File size is not a multiple of block size."
        );
        let block_count: u64 = file_size / block_size as u64;
        FileLayer {
            file,
            block_size,
            block_count,
        }
    }

    pub fn get_block_count(&self) -> u64 {
        self.block_count
    }

    pub fn append_new_page(&mut self, page: &Page, page_no: &PageNo) {
        use std::io::{Seek, SeekFrom, Write};
        
        let block_count = page_no.get_pg_ctr_block_cnt();
        let block_offset = page_no.get_file_blk_offset();
        assert!(
            block_offset == self.block_count,
            "page_number should match page_count"
        );
        let offset = block_offset * self.block_size as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for append_new_page");
        self.file
            .write_all(page.get_block_bytes())
            .expect("Failed to write for append_new_page");
        self.block_count += block_count;
    }

    pub fn write_page_to_disk(&mut self, page: &Page, page_no: &PageNo) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let block_offset = page_no.get_file_blk_offset();
        let offset = block_offset * self.block_size as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for write_page_to_disk");
        self.file
            .write_all(page.get_block_bytes())
            .expect("Failed to write for write_page_to_disk");
        Ok(())
    }

    pub fn read_page_from_disk(
        &mut self,
        page: &mut Page,
        page_no: &PageNo,
    ) -> std::io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};

        let block_offset = page_no.get_file_blk_offset();
        assert!(block_offset < self.block_count);

        let offset = block_offset * self.block_size as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for read");
        self.file
            .read_exact(page.get_block_bytes_mut())
            .expect("Failed to read");
        Ok(())
    }

    pub fn sync_all(&self) {
        self.file.sync_all().expect("Failed to sync")
    }

    pub fn sync_data(&self) {
        self.file.sync_data().expect("Failed to sync data")
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::FileExt;

    use super::*;
    const BLOCK_SIZE: usize = 4096;
    use rand::Rng;
    use rand::distr::Alphanumeric;
    use tempfile::tempfile;

    #[test]
    fn test_file_layer_write_and_read() {
        let temp_file = tempfile().expect("Failed to create temp file");
        let mut file_layer = FileLayer::new(temp_file, BLOCK_SIZE);
        let mut page = Page::new(BLOCK_SIZE, BLOCK_SIZE - 4); // Create a new page
        file_layer.append_new_page(&page, &PageNo::from_u64(0));
        let test_data: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(BLOCK_SIZE)
            .map(char::from)
            .collect();
        page.get_block_bytes_mut()
            .copy_from_slice(test_data.as_bytes()); // Fill the page with test data

        // Write the page to disk
        file_layer
            .write_page_to_disk(&page, &PageNo::from_u64(0))
            .expect("Failed to write page");

        // Read the page back from disk
        let mut read_page = Page::new(BLOCK_SIZE, BLOCK_SIZE);
        file_layer
            .read_page_from_disk(&mut read_page, &PageNo::from_u64(0))
            .expect("Failed to read page");

        // Verify that the read data matches the written data
        assert_eq!(page.get_block_bytes(), read_page.get_block_bytes());
    }

    #[test]
    #[should_panic(expected = "page_number should match page_count")]
    fn test_file_layer_write_bad_page_no() {
        let temp_file = tempfile().expect("Failed to create temp file");
        let mut file_layer = FileLayer::new(temp_file, BLOCK_SIZE);
        let mut page = Page::new(BLOCK_SIZE, BLOCK_SIZE - 4); // Create a new page
        file_layer.append_new_page(&page, &PageNo::from_u64(24));
        let test_data: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(BLOCK_SIZE)
            .map(char::from)
            .collect();
        page.get_block_bytes_mut()
            .copy_from_slice(test_data.as_bytes()); // Fill the page with test data

        // Write the page to disk
        file_layer.append_new_page(&page, &PageNo::from_u64(0));
    }

    #[test]
    #[should_panic(expected = "File size is not a multiple of block size.")]
    fn test_file_layer_non_block_size_file() {
        let temp_file = tempfile().expect("Failed to create temp file");
        temp_file
            .write_all_at(b"Hello, Rust!", 0)
            .expect("Failed to write to temp file");
        let _file_layer = FileLayer::new(temp_file, BLOCK_SIZE);
    }
}

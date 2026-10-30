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
        // A crash while extending the file can leave a partial block at the
        // end, it is not referenced by the database so ignore it. Appending
        // a page will overwrite it.
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

    // Remove blocks from the end of the file.
    pub fn truncate(&mut self, block_count: u64) {
        assert!(block_count <= self.block_count);
        self.file
            .set_len(block_count * self.block_size as u64)
            .expect("Failed to truncate file");
        self.file.sync_all().expect("Failed to sync");
        self.block_count = block_count;
    }

    pub fn append_new_page(&mut self, page: &Page, page_no: &PageNo) {
        use std::io::{Seek, SeekFrom, Write};

        let pg_blk_count = page_no.get_blk_cnt();
        let pg_blk_offset = page_no.get_blk_offset();
        assert!(
            pg_blk_offset == self.block_count,
            "page_number should match page_count"
        );
        let offset = pg_blk_offset * self.block_size as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for append_new_page");
        self.file
            .write_all(page.get_pg_ctr_bytes())
            .expect("Failed to write for append_new_page");
        self.block_count += pg_blk_count;
    }

    pub fn write_page_to_disk(&mut self, page: &Page, page_no: &PageNo) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let pg_blk_offset = page_no.get_blk_offset();
        let offset = pg_blk_offset * self.block_size as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for write_page_to_disk");
        self.file
            .write_all(page.get_pg_ctr_bytes())
            .expect("Failed to write for write_page_to_disk");
        Ok(())
    }

    pub fn read_page_from_disk(
        &mut self,
        page_buffer: &mut [u8],
        page_no: &PageNo,
    ) -> std::io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};

        let pg_blk_offset = page_no.get_blk_offset();
        assert!(
            pg_blk_offset < self.block_count,
            "blk offset {}, block count {}",
            pg_blk_offset,
            self.block_count
        );

        let offset = pg_blk_offset * self.block_size as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for read");
        self.file.read_exact(page_buffer).expect("Failed to read");
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
    use super::*;
    use crate::page::PageType;
    use std::os::unix::fs::FileExt;
    const BLOCK_SIZE: usize = 4096;
    use rand::RngExt;
    use rand::distr::Alphanumeric;
    use tempfile::tempfile;

    #[test]
    fn test_file_layer_write_and_read() {
        let temp_file = tempfile().expect("Failed to create temp file");
        let mut file_layer = FileLayer::new(temp_file, BLOCK_SIZE);
        let mut page = Page::new(BLOCK_SIZE, BLOCK_SIZE - 4); // Create a new page
        let page_no = PageNo::new(PageType::Null, 0, 0);
        file_layer.append_new_page(&page, &page_no);
        let test_data: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(BLOCK_SIZE)
            .map(char::from)
            .collect();
        page.get_pg_ctr_bytes_mut()
            .copy_from_slice(test_data.as_bytes()); // Fill the page with test data

        // Write the page to disk
        file_layer
            .write_page_to_disk(&page, &page_no)
            .expect("Failed to write page");

        // Read the page back from disk
        let mut read_page = Page::new(BLOCK_SIZE, BLOCK_SIZE);
        file_layer
            .read_page_from_disk(read_page.get_pg_ctr_bytes_mut(), &page_no)
            .expect("Failed to read page");

        // Verify that the read data matches the written data
        assert_eq!(page.get_pg_ctr_bytes(), read_page.get_pg_ctr_bytes());
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
        page.get_pg_ctr_bytes_mut()
            .copy_from_slice(test_data.as_bytes()); // Fill the page with test data

        // Write the page to disk
        file_layer.append_new_page(&page, &PageNo::from_u64(0));
    }

    #[test]
    fn test_file_layer_non_block_size_file() {
        // A partial block at the end of the file is ignored.
        let temp_file = tempfile().expect("Failed to create temp file");
        temp_file
            .write_all_at(&[1u8; BLOCK_SIZE + 12], 0)
            .expect("Failed to write to temp file");
        let file_layer = FileLayer::new(temp_file, BLOCK_SIZE);
        assert_eq!(file_layer.get_block_count(), 1);
    }

    #[test]
    fn test_file_layer_truncate() {
        let temp_file = tempfile().expect("Failed to create temp file");
        temp_file
            .write_all_at(&[1u8; 3 * BLOCK_SIZE], 0)
            .expect("Failed to write to temp file");
        let mut file_layer = FileLayer::new(temp_file, BLOCK_SIZE);
        file_layer.truncate(1);
        assert_eq!(file_layer.get_block_count(), 1);
        assert_eq!(file_layer.file.metadata().unwrap().len(), BLOCK_SIZE as u64);
    }
}

use crate::page::Page; 


pub struct FileLayer {
    file: std::fs::File,
}


impl FileLayer {
    pub fn new(file: std::fs::File) -> Self {
        FileLayer { file }
    }

    pub fn write_page_to_disk(&mut self, page: &mut Page, page_number: u32, page_size: u64) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let offset = page_number as u64 * page_size;
        self.file.seek(SeekFrom::Start(offset)).expect("Failed to seek for write");
        self.file.write_all(&page.get_bytes_mut()).expect("Failed to write");
        Ok(())
    }

    pub fn read_page_from_disk(&mut self, page: &mut Page, page_number: u32, page_size: u64) -> std::io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};

        let offset = page_number as u64 * page_size;
        self.file.seek(SeekFrom::Start(offset)).expect("Failed to seek for read");
        self.file.read_exact(page.get_bytes_mut()).expect("Failed to read");
        Ok(())
    }

    pub fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_all().expect("Failed to sync");
        Ok(())
    }

    pub fn sync_data(&mut self) -> std::io::Result<()> {
        self.file.sync_data().expect("Failed to sync data");
        Ok(())
    }
}

impl Drop for FileLayer {
    fn drop(&mut self) {
        self.sync().expect("Failed to sync on drop");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const PAGE_SIZE: u64 = 4096;
    use tempfile::tempfile; // Add this line to use the tempfile crate  
    use rand::Rng;
    use rand::distr::Alphanumeric;
    use crate::page::PageTrait;

    #[test]
    fn test_file_layer_write_and_read() {
        let temp_file = tempfile().expect("Failed to create temp file");
        let mut file_layer = FileLayer::new(temp_file);
        let mut page = Page::new(PAGE_SIZE); // Create a new page
        let test_data: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(PAGE_SIZE as usize)
            .map(char::from)
            .collect();
        page.get_bytes_mut().copy_from_slice(test_data.as_bytes()); // Fill the page with test data

        // Write the page to disk
        file_layer.write_page_to_disk(&mut page, 0, PAGE_SIZE).expect("Failed to write page");

        // Read the page back from disk
        let mut read_page = Page::new(PAGE_SIZE);
        file_layer.read_page_from_disk(&mut read_page, 0, PAGE_SIZE).expect("Failed to read page");

        // Verify that the read data matches the written data
        assert_eq!(page.get_bytes(), read_page.get_bytes());
    }
}
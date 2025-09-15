pub struct PageCache {
     file: std::fs::File,   
     page_size: u64, 
}

impl PageCache {
    pub fn new(file: std::fs::File, page_size: u64) -> Self {
        PageCache { file, page_size }
    }

    pub fn read_page(&mut self, page_number: u64, buffer: &mut [u8]) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Read};

        let offset = page_number * self.page_size;
        self.file.seek(SeekFrom::Start(offset)).expect("Failed to seek to DB page");
        self.file.read_exact(buffer).expect("Failed to read DB page");
        Ok(())
    }

    pub fn write_page(&mut self, page_number: u64, page_data: &[u8]) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let offset = page_number * self.page_size;
        self.file.seek(SeekFrom::Start(offset)).expect("Failed to seek to DB page");
        self.file.write_all(page_data).expect("Failed to write DB page");

        Ok(())
    }

    pub fn sync_data(&mut self) -> std::io::Result<()> {
        self.file.sync_data().expect("Failed to sync data DB file to disk");
        Ok(())
    }   

    pub fn sync_all(&mut self) -> std::io::Result<()> {
        self.file.sync_all().expect("Failed to sync all DB file to disk");
        Ok(())
    }
}

impl Drop for PageCache {
    fn drop(&mut self) {
        self.sync_all().expect("Failed to sync all DB file to disk on drop");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    

    #[test]
    fn test_page_cache_read_write() {
        let path = "/tmp/test_page_cache.db";
        let page_size: u64 = 4096;
        let mut file = OpenOptions::new() 
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Should be able to create/open file");      

        let cache = PageCache::new(file, page_size);

    }
    
}

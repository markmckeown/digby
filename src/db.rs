use crate::page_cache::PageCache;
use crate::file_layer::FileLayer;
use crate::block_layer::BlockLayer;
use crate::page::Page;
use crate::root_page::RootPage;
use crate::page::PageTrait;

pub struct Db {
    path: String, 
    page_cache: PageCache,
}


impl Db {
    pub const PAGE_SIZE: u64 = 4096;

    pub fn new(path: &str) -> Self {        
        use std::fs::OpenOptions;
        use std::path::Path;
        
        let mut is_new = false;
        let mut file_size: u64 = 0;

        let db_file: std::fs::File;
        if Path::new(path).exists() {
            db_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path).expect("Failed to open existing DB file");
            file_size = std::fs::metadata(path).unwrap().len();
            if file_size == 0 {
                is_new = true;
            }
        } else {
            db_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path).expect("Failed to open or create DB file");
            is_new = true;
        }

        let file_layer: FileLayer = FileLayer::new(db_file);
        let block_layer: BlockLayer = BlockLayer::new(file_layer);
        let page_cache: PageCache = PageCache::new(block_layer, Self::PAGE_SIZE);

        let mut db = Db {
            path: path.to_string(),
            page_cache: page_cache,
        };

        if is_new {
            db.init_db_file().expect("Failed to initialize DB file");
        } else {
            db.check_db_integrity(file_size).expect("DB integrity check failed");
        }
        db
    }

    pub fn check_db_integrity(&mut self, file_size: u64) -> std::io::Result<()> {
        assert!(file_size % Self::PAGE_SIZE == 0, "Corrupted DB file: size is not multiple of page size");
        let mut _page : Page = self.page_cache.read_page(0);
        Ok(())
    }

    pub fn init_db_file(&mut self) -> std::io::Result<()> {
        let mut head_page: RootPage = RootPage::new(Db::PAGE_SIZE);
        self.page_cache.write_page(&mut head_page.get_page());
        self.page_cache.sync_all();
        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

}

impl Drop for Db {
    fn drop(&mut self) {
        self.page_cache.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::NamedTempFile; 

    #[test]
    fn test_db_creation() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
        let db = Db::new(temp_file.path().to_str().unwrap());
        assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
        }
        {
        let db = Db::new(temp_file.path().to_str().unwrap());
        assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

}
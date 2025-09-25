use crate::head_page::HeadPage;
use crate::page_cache::PageCache;
use crate::file_layer::FileLayer;
use crate::block_layer::BlockLayer;
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

        let db_file: std::fs::File;
        if Path::new(path).exists() {
            db_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path).expect("Failed to open existing DB file");
            if std::fs::metadata(path).unwrap().len() == 0 {
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

        let file_layer: FileLayer = FileLayer::new(db_file, Db::PAGE_SIZE);
        let block_layer: BlockLayer = BlockLayer::new(file_layer, Db::PAGE_SIZE);
        let page_cache: PageCache = PageCache::new(block_layer, Self::PAGE_SIZE);

        let mut db = Db {
            path: path.to_string(),
            page_cache: page_cache,
        };

        if is_new {
            db.init_db_file().expect("Failed to initialize DB file");
        } else {
            db.check_db_integrity().expect("DB integrity check failed");
        }
        db
    }

    pub fn check_db_integrity(&mut self) -> std::io::Result<()> {
        let _root_page = RootPage::from_page(self.page_cache.get_page(0));

        let _head_page1 = HeadPage::from_page(self.page_cache.get_page(1)); 

        let _head_page2 = HeadPage::from_page(self.page_cache.get_page(2)); 

        Ok(())
    }

    pub fn init_db_file(&mut self) -> std::io::Result<()> {
        let mut root_page: RootPage = RootPage::new(Db::PAGE_SIZE);
        let mut free_pages: Vec<u32> = self.page_cache.put_page(&mut root_page.get_page());

        let mut head_page1: HeadPage = HeadPage::new(Db::PAGE_SIZE, 1, 0);
        free_pages.extend(self.page_cache.put_page(&mut head_page1.get_page()));

        let mut head_page2: HeadPage = HeadPage::new(Db::PAGE_SIZE, 2, 0);
        free_pages.extend(self.page_cache.put_page(&mut head_page2.get_page()));
        assert!(free_pages.len() == 0, "There should be no free pages");

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
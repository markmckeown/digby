use crate::{FreeDirPage, FreePage, TableDirPage};
use crate::db_master_page::DbMasterPage;
use crate::page_cache::PageCache;
use crate::file_layer::FileLayer;
use crate::block_layer::BlockLayer;
use crate::db_root_page::DbRootPage;
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
        let _root_page = DbRootPage::from_page(self.page_cache.get_page(0));
        let master_page1 = DbMasterPage::from_page(self.page_cache.get_page(1)); 
        let master_page2 = DbMasterPage::from_page(self.page_cache.get_page(2)); 
        let mut current_master = if master_page1.get_version() > master_page2.get_version() {
             master_page1 
        } else {
             master_page2
        }; 
        let current_version = current_master.get_version();
        let free_dir_page_no = current_master.get_free_page_dir_page_no();
        let free_dir_page = FreeDirPage::from_page(self.page_cache.get_page(free_dir_page_no));
        assert!(free_dir_page.get_version() <= current_version);
        let table_dir_page_no = current_master.get_table_dir_page_no();
        let table_dir_page = TableDirPage::from_page(self.page_cache.get_page(table_dir_page_no));
        assert!(table_dir_page.get_version() <= current_version);

        Ok(())
    }

    pub fn init_db_file(&mut self) -> std::io::Result<()> {
        // Trigger adding 11 free pages and syncing the the file by creating a page
        // at page no 10.
        let mut free_page: FreePage = FreePage::new(Db::PAGE_SIZE, 10);
        let mut free_pages =  self.page_cache.put_page(&mut free_page.get_page());

        // There are 11 free pages but we are going to use the first five of them.
        free_pages.retain(|&x| x != 0);
        free_pages.retain(|&x| x != 1);
        free_pages.retain(|&x| x != 2);
        free_pages.retain(|&x| x != 3);
        free_pages.retain(|&x| x != 4);

        
        // Write the free page directory
        let mut free_dir_page = FreeDirPage::new(Db::PAGE_SIZE, 3, 0);
        for page_number in &free_pages {
            free_dir_page.add_free_page(*page_number);
        }
        assert!(self.page_cache.put_page(&mut free_dir_page.get_page()).is_empty());

        // Write the table directoru page.
        let mut table_dir_page = TableDirPage::new(Db::PAGE_SIZE, 4, 0);
        assert!(self.page_cache.put_page(&mut table_dir_page.get_page()).is_empty());

        // Write first master page
        let mut master_page1: DbMasterPage = DbMasterPage::new(Db::PAGE_SIZE, 1, 0);
        master_page1.set_free_page_dir_page_no(3);
        master_page1.set_table_dir_page_no(4);
        assert!(self.page_cache.put_page(&mut master_page1.get_page()).is_empty());

        // Write second master page.
        let mut master_page2: DbMasterPage = DbMasterPage::new(Db::PAGE_SIZE, 2, 1);
        master_page2.set_free_page_dir_page_no(3);
        master_page2.set_table_dir_page_no(4);
        assert!(self.page_cache.put_page(&mut master_page2.get_page()).is_empty());
        
        // Flush all pages so far, don't sync the file metadata
        self.page_cache.sync_data();

        // Write the root page as last step to make the DB sane.
        let mut db_root_page: DbRootPage = DbRootPage::new(Db::PAGE_SIZE);
        assert!(self.page_cache.put_page(&mut db_root_page.get_page()).is_empty());

        assert!(free_pages.len() == 6, "There should be 6 free pages");

        self.page_cache.sync_data();
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
            let mut db = Db::new(temp_file.path().to_str().unwrap());
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            let _head_page1 = DbMasterPage::from_page(db.page_cache.get_page(1));
            let mut head_page2 = DbMasterPage::from_page(db.page_cache.get_page(2));
            let free_page_dir_page_no = head_page2.get_free_page_dir_page_no();
            let mut free_page_dir_page = FreeDirPage::from_page(db.page_cache.get_page(free_page_dir_page_no));
            assert!(free_page_dir_page.get_entries() == 6);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

}
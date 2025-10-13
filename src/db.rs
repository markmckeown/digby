use crate::free_page_tracker::FreePageTracker;
use crate::{FreeDirPage, StoreTupleProcessor, TableDirPage, TreeLeafPage};
use crate::db_master_page::DbMasterPage;
use crate::page_cache::PageCache;
use crate::file_layer::FileLayer;
use crate::block_layer::BlockLayer;
use crate::db_root_page::DbRootPage;
use crate::page::PageTrait;
use crate::tuple::{Tuple};

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
        let current_master = if master_page1.get_version() > master_page2.get_version() {
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
        // Get some free pages and make space in the file.
        // Will trigger a file sync.
        let mut free_pages: Vec<u32> = self.page_cache.create_new_pages(10);
        assert!(free_pages.len() == 10);
        
        // Write the Global Tree Root Page.
        let mut global_tree_root_page = TreeLeafPage::new(Db::PAGE_SIZE, 5);
        // remove it from the free list
        free_pages.retain(|&x| x != 5);
        self.page_cache.put_page(&mut global_tree_root_page.get_page());

        // Write the table directoru page.
        let mut table_dir_page = TableDirPage::new(Db::PAGE_SIZE, 4, 0);
        // remove from the free page list
        free_pages.retain(|&x| x != 4);
        self.page_cache.put_page(&mut table_dir_page.get_page());

        // Write first master page
        let mut master_page1: DbMasterPage = DbMasterPage::new(Db::PAGE_SIZE, 1, 0);
        // remove from free page list
        free_pages.retain(|&x| x != 1);
        master_page1.set_free_page_dir_page_no(3);
        master_page1.set_table_dir_page_no(4);
        master_page1.set_global_tree_root_page_no(5);
        self.page_cache.put_page(&mut master_page1.get_page());

        // Write second master page.
        let mut master_page2: DbMasterPage = DbMasterPage::new(Db::PAGE_SIZE, 2, 1);
        // remove from free page list
        free_pages.retain(|&x| x != 2);
        master_page2.set_free_page_dir_page_no(3);
        master_page2.set_table_dir_page_no(4);
        master_page2.set_global_tree_root_page_no(5);
        self.page_cache.put_page(&mut master_page2.get_page());
        
        // Now write the free page directory
        let mut free_dir_page = FreeDirPage::new(Db::PAGE_SIZE, 3, 0);
        // The free_dir_page is no longer free, and also the root db page won't be free.
        free_pages.retain(|&x| x != 0);
        free_pages.retain(|&x| x != 3);
        free_dir_page.add_free_pages(&free_pages);
        self.page_cache.put_page(&mut free_dir_page.get_page());

        // Flush all pages so far, don't sync the file metadata
        self.page_cache.sync_data();

        // Write the root page as last step to make the DB sane.
        let mut db_root_page: DbRootPage = DbRootPage::new(Db::PAGE_SIZE);
        self.page_cache.put_page(&mut db_root_page.get_page());

        assert!(free_pages.len() == 4, "There should be 4 free pages");

        self.page_cache.sync_data();
        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn get_master_page(&mut self) -> DbMasterPage {
        let master_page1 = DbMasterPage::from_page(self.page_cache.get_page(1)); 
        let master_page2 = DbMasterPage::from_page(self.page_cache.get_page(2)); 
        let current_master = if master_page1.get_version() > master_page2.get_version() {
             master_page1 
        } else {
             master_page2
        };
        current_master
    }


    pub fn get(&mut self, key: Vec<u8>) -> Option<Tuple> {
        assert!(key.len() < 1024, "Cannot handle big keys yet.");
        let master_page = self.get_master_page();
        let tree_page_no = master_page.get_global_tree_root_page_no();
        let page = self.page_cache.get_page(tree_page_no);

        return StoreTupleProcessor::get_tuple(key, page, &mut self.page_cache, Db::PAGE_SIZE as usize);
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> () {
        // Assert on the things that cannot be handled yet.
        assert!(key.len() < 1024, "Cannot handle big keys yet.");
        assert!(value.len() < 1024, "Cannot handle big values yet.");
        
        // Get the current master page. Note this is a copy of the page 
        let mut master_page = self.get_master_page();

        // Increment the version number
        let old_version = master_page.get_version();
        let new_version = old_version + 1;

        // Create the tuple we want to add. 
        let tuple = Tuple::new(key, value, new_version);

        // Find the free page directory that has the free page numbers. Make sure
        // it has free pages - cannot handle the case it does not yet.
        let free_page_dir_page_no = master_page.get_free_page_dir_page_no();
        let mut free_page_tracker = FreePageTracker::new(
            self.page_cache.get_page(free_page_dir_page_no), 
            new_version, Db::PAGE_SIZE as usize);

        
        // Now get the page number of the root of the global tree. Then get the page,
        // this is a copy of the page. Only handle the case when the root is also 
        // a leaf node ATM.
        let tree_root_page_no = master_page.get_global_tree_root_page_no();

        let page =  self.page_cache.get_page(tree_root_page_no);   

        let new_tree_free_page_no = StoreTupleProcessor::store_tuple(tuple, page, &mut free_page_tracker, 
            &mut self.page_cache, new_version, Db::PAGE_SIZE as usize);

        
        free_page_tracker.return_free_page_no(tree_root_page_no);
        // Write the new free page directory back through the page cache.
        let mut free_dir_pages = free_page_tracker.get_free_dir_pages(&mut self.page_cache);
        assert!(free_dir_pages.len() >= 1);
        let first_free_dir_page = free_dir_pages.last().unwrap().get_page_number();
        while let Some(mut free_dir_page) = free_dir_pages.pop() {
            self.page_cache.put_page(free_dir_page.get_page());
        }


        // Now need to update the master - tell it were the 
        // the globale tree root page is and where the free page
        // directory is now.
        master_page.set_free_page_dir_page_no(first_free_dir_page);
        master_page.set_global_tree_root_page_no(new_tree_free_page_no);
        // update the version
        master_page.set_version(new_version);
        // flip the page number to overrwrite the non-current master
        // page and make it the new current master.
        master_page.flip_page_number();

        // Sync the first two pages before writing the new master page.
        self.page_cache.sync_data();
        // Put the master page.
        self.page_cache.put_page(master_page.get_page());
        // Now sync the master
        self.page_cache.sync_data();          

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
    use crate::tuple::{TupleTrait};

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
            let head_page2 = DbMasterPage::from_page(db.page_cache.get_page(2));
            let free_page_dir_page_no = head_page2.get_free_page_dir_page_no();
            let free_page_dir_page = FreeDirPage::from_page(db.page_cache.get_page(free_page_dir_page_no));
            assert!(free_page_dir_page.get_entries() == 4);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap());
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            db.put(b"the_key".to_vec(), b"the_value".to_vec());
        }
        // The new scope essentially closes the DB - when Files run out of scope then 
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap());
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            let tuple = db.get(b"the_key".to_vec()).unwrap();
            assert!(tuple.get_value() == b"the_value".to_vec());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }



}
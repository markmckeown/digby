use crate::compressor::CompressorType;
use crate::free_page_tracker::FreePageTracker;
use crate::{Compressor, FreeDirPage, OverflowPageHandler, StoreTupleProcessor, TableDirPage, TreeLeafPage, TupleProcessor};
use crate::db_master_page::DbMasterPage;
use crate::page_cache::PageCache;
use crate::file_layer::FileLayer;
use crate::block_layer::{BlockLayer, BlockSanity};
use crate::db_root_page::DbRootPage;
use crate::page::PageTrait;
use crate::overflow_tuple::OverflowTuple;
use crate::tuple::{Overflow, TupleTrait};

pub struct Db {
    path: String, 
    page_cache: PageCache,
    compressor: Compressor,
}


impl Db {
    pub const BLOCK_SIZE: usize = 4096;

    pub fn new(path: &str, key: Option<Vec<u8>>, compressor_type: CompressorType) -> Self {        
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

        let file_layer: FileLayer = FileLayer::new(db_file, Db::BLOCK_SIZE);
        let block_layer: BlockLayer;
        let sanity_type: BlockSanity;
        if key.is_none() {
            block_layer = BlockLayer::new(file_layer, Db::BLOCK_SIZE);
            sanity_type = BlockSanity::XxH32Checksum;
        } else {
            block_layer = BlockLayer::new_with_key(file_layer, Db::BLOCK_SIZE, key.unwrap());
            sanity_type = BlockSanity::AesGcm;
        }
        let page_cache: PageCache = PageCache::new(block_layer);


        let mut db = Db {
            path: path.to_string(),
            page_cache: page_cache,
            compressor: Compressor::new(compressor_type),
        };

        if is_new {
            db.init_db_file(sanity_type).expect("Failed to initialize DB file");
        } else {
            db.check_db_integrity(sanity_type).expect("DB integrity check failed");
        }
        db
    }

    pub fn check_db_integrity(&mut self, sanity_type: BlockSanity) -> std::io::Result<()> {
        let root_page = DbRootPage::from_page(self.page_cache.get_page(0));
        if root_page.get_sanity_type() != sanity_type {
            panic!("Db encryption mis-match, stored type is {:?}, requested type {:?}", root_page.get_sanity_type(), sanity_type);
        }
        let stored_compressor_type = CompressorType::try_from(root_page.get_compression_type()).expect("Unknown compressor");
        if stored_compressor_type != self.compressor.compressor_type {
            panic!("Db compression mis-match, stored type is {:?}, requested type {:?}", root_page.get_compression_type(), 
            self.compressor.compressor_type);
        }
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

    pub fn init_db_file(&mut self, sanity_type: BlockSanity) -> std::io::Result<()> {
        // Get some free pages and make space in the file.
        // Will trigger a file sync.
        let mut free_pages: Vec<u32> = self.page_cache.generate_free_pages(10);
        assert!(free_pages.len() == 10);
        
        // Write the Global Tree Root Page.
        let mut global_tree_root_page = TreeLeafPage::create_new(self.page_cache.get_page_config(), 5);
        // remove it from the free list
        free_pages.retain(|&x| x != 5);
        self.page_cache.put_page(&mut global_tree_root_page.get_page());

        // Write the table directoru page.
        let mut table_dir_page = TableDirPage::create_new(self.page_cache.get_page_config(), 4, 0);
        // remove from the free page list
        free_pages.retain(|&x| x != 4);
        self.page_cache.put_page(&mut table_dir_page.get_page());

        // Write first master page
        let mut master_page1: DbMasterPage = DbMasterPage::create_new(self.page_cache.get_page_config(), 1, 0);
        // remove from free page list
        free_pages.retain(|&x| x != 1);
        master_page1.set_free_page_dir_page_no(3);
        master_page1.set_table_dir_page_no(4);
        master_page1.set_global_tree_root_page_no(5);
        self.page_cache.put_page(&mut master_page1.get_page());

        // Write second master page.
        let mut master_page2: DbMasterPage = DbMasterPage::create_new(self.page_cache.get_page_config(), 2, 1);
        // remove from free page list
        free_pages.retain(|&x| x != 2);
        master_page2.set_free_page_dir_page_no(3);
        master_page2.set_table_dir_page_no(4);
        master_page2.set_global_tree_root_page_no(5);
        self.page_cache.put_page(&mut master_page2.get_page());
        
        // Now write the free page directory
        let mut free_dir_page = FreeDirPage::create_new(self.page_cache.get_page_config(), 3, 0);
        // The free_dir_page is no longer free, and also the root db page won't be free.
        free_pages.retain(|&x| x != 0);
        free_pages.retain(|&x| x != 3);
        free_dir_page.add_free_pages(&free_pages);
        self.page_cache.put_page(&mut free_dir_page.get_page());

        // Flush all pages so far, don't sync the file metadata
        self.page_cache.sync_data();

        // Write the root page as last step to make the DB sane.
        let mut db_root_page: DbRootPage = DbRootPage::create_new(self.page_cache.get_page_config());
        db_root_page.set_sanity_type(sanity_type);
        db_root_page.set_compression_type(self.compressor.compressor_type.clone().into());
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


    pub fn get_tuple_value<T: TupleTrait>(&self, tuple: &T) -> Vec<u8> {
        let overflow = tuple.get_overflow();
        if overflow == Overflow::ValueCompressed || overflow == Overflow::KeyValueCompressed {
            return self.compressor.decompress(tuple.get_value());
        }
        return tuple.get_value().to_vec();
    }

    pub fn get_tuple_key<T: TupleTrait>(&self, tuple: &T) -> Vec<u8> {
        let overflow = tuple.get_overflow();
        if overflow == Overflow::KeyValueCompressed {
            return self.compressor.decompress(tuple.get_key());
        }
        return tuple.get_key().to_vec();
    }


    pub fn get(&mut self, key: &Vec<u8>) -> Option<Vec<u8>> {
        assert!(key.len() < u32::MAX as usize, "Cannot handle keys larger than u32::MAX.");
        let master_page = self.get_master_page();
        let tree_page_no = master_page.get_global_tree_root_page_no();
        // TODO need to check versions.
        let page = self.page_cache.get_page(tree_page_no);
        // If not an oversized key...
        if !TupleProcessor::is_oversized_key(key) {
            if let Some(tuple) = StoreTupleProcessor::get_tuple(key, page, &mut self.page_cache) {
                return Some(self.get_tuple_value(&tuple));
            } else {
                return None;
            }
        }

        // Oversized key - get short version
        // TODO - if the key already exists and points to an overflow page then
        // we will leak the original overflow pages. So need to get the key first
        // and add the overflow pages to the free pages - could do that here
        // or at a lower level.
        let short_key = TupleProcessor::generate_short_key(key);
        // This tuple will have a page number as the value, the page will be an overflow page
        // that forms a linked list of pages that will hold the tuple.
        let tuple =  StoreTupleProcessor::get_tuple(&short_key, page, 
            &mut self.page_cache);
        // Do not have this key.
        if tuple.is_none() {
            return None;
        }
        let overflow_page_no = u32::from_le_bytes(tuple.unwrap().get_value()[0 .. 4].try_into().unwrap());
        let overflow_tuple: OverflowTuple = OverflowPageHandler::get_overflow_tuple(overflow_page_no, &mut self.page_cache);
        // Confirm the key is the same - would require a SHA256 clash to fail
        if *key != self.get_tuple_key(&overflow_tuple) {
            return None;
        }
        return Some(self.get_tuple_value(&overflow_tuple));
    }

    pub fn put(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> () {
        // Assert on the things that cannot be handled yet.
        assert!(key.len() < u32::MAX as usize, "Cannot handle keys larger than u32::MAX.");
        assert!(value.len() < u32::MAX as usize, "Cannot handle values larger than u32::MAX.");
        
        // Get the current master page. Note this is a copy of the page 
        let mut master_page = self.get_master_page();

        // Increment the version number
        let old_version = master_page.get_version();
        let new_version = old_version + 1;

        // Find the free page directory that has the free page numbers. Make sure
        // it has free pages - cannot handle the case it does not yet.
        let free_page_dir_page_no = master_page.get_free_page_dir_page_no();
        let mut free_page_tracker = FreePageTracker::new(
                self.page_cache.get_page(free_page_dir_page_no), 
                new_version, *self.page_cache.get_page_config());

        // Create the tuple we want to add. 
        let tuple = TupleProcessor::generate_tuple(&key, &value, &mut self.page_cache, &mut free_page_tracker, 
            new_version, &self.compressor);  
        
        // Now get the page number of the root of the global tree. Then get the page,
        // this is a copy of the page. Only handle the case when the root is also 
        // a leaf node ATM.
        let tree_root_page_no = master_page.get_global_tree_root_page_no();
        let page =  self.page_cache.get_page(tree_root_page_no);   
        let new_tree_free_page_no = StoreTupleProcessor::store_tuple(tuple, page, &mut free_page_tracker, 
            &mut self.page_cache, new_version);
       
        // Write out the free pages.
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
    use rand::RngCore; 

    #[test]
    fn test_db_creation() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let db = Db::new(temp_file.path().to_str().unwrap(), None, CompressorType::None);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
        }
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(), None, CompressorType::None);
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
        let key = b"the_key".to_vec();
        let value = b"the_value".to_vec();
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(), None, CompressorType::None);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            db.put(&key, &value);
        }
        // The new scope essentially closes the DB - when Files run out of scope then 
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(), None, CompressorType::None);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            let returned_value = db.get(&key).unwrap();
            assert!(returned_value == value);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_large_key_value_compressible() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let key: Vec<u8> = vec![111u8; 8192];
        let value: Vec<u8> = vec![56u8; 18192];
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(), None, CompressorType::LZ4);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            db.put(&key, &value);
        }
        // The new scope essentially closes the DB - when Files run out of scope then 
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(), None, CompressorType::LZ4);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            let returned_value = db.get(&key).unwrap();
            assert!(returned_value == value);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

     #[test]
    fn test_db_store_large_key_value_incompressible() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let mut key: Vec<u8> = vec![0u8; 8192];
        let mut value: Vec<u8> = vec![0u8; 18192];
        let mut rng = rand::rng();
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut value);
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(), None, CompressorType::LZ4);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            db.put(&key, &value);
        }
        // The new scope essentially closes the DB - when Files run out of scope then 
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(), None, CompressorType::LZ4);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            let returned_value = db.get(&key).unwrap();
            assert!(returned_value == value);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

     #[test]
    fn test_db_store_value_with_encryption() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let key = b"the_key".to_vec();
        let value = b"the_value".to_vec();
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(), Some(b"the_key".to_vec()), CompressorType::None);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            db.put(&key, &value);
        }
        // The new scope essentially closes the DB - when Files run out of scope then 
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new(temp_file.path().to_str().unwrap(),Some(b"the_key".to_vec()), CompressorType::None);
            assert_eq!(db.get_path(), temp_file.path().to_str().unwrap());
            let returned_value = db.get(&key).unwrap();
            assert!(returned_value == value);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

}
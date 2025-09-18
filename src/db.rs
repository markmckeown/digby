use crate::page_cache::PageCache;
use crate::file_layer::FileLayer;
use crate::block_layer::BlockLayer;
use crate::page::Page;

pub struct Db {
    path: String, 
    page_cache: PageCache,
}


impl Db {
    pub const PAGE_SIZE: u64 = 4096;
    pub const MAGIC_NUMBER: u32 = 26061973;

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
            db.check_db_integrity().expect("DB integrity check failed");
        }
        db
    }

    pub fn check_db_integrity(&mut self) -> std::io::Result<()> {
        let mut _page : Page = self.page_cache.read_page(0);
        Ok(())
    }

    pub fn init_db_file(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

}


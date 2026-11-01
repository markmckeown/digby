use crate::Compressor;
use crate::TupleProcessor;
use crate::compressor::CompressorType;
use crate::db_config::DbConfig;
use crate::free_page_manager::FreePageManager;
use crate::leaf_page::LeafPage;
use crate::page_cache::PageCache;
use crate::page_no::PageNo;

pub struct FreedPageSlabWriter {
    db_config: DbConfig,
    version: u64,
}

impl FreedPageSlabWriter {
    pub fn new(db_config: DbConfig, version: u64) -> Self {
        FreedPageSlabWriter { db_config, version }
    }

    pub fn write_to_leaf_page(
        &self,
        page_no: PageNo,
        page_cache: &mut PageCache,
        free_pg_mgr: &mut FreePageManager,
        freed_pages: Vec<u8>,
    ) -> LeafPage {
        let compressor = Compressor::new(CompressorType::None);
        let tuple = TupleProcessor::generate_tuple(
            &self.version.to_le_bytes(),
            &freed_pages[..],
            page_cache,
            free_pg_mgr,
            self.version,
            &compressor,
            &self.db_config,
        );
        let mut leaf_page = LeafPage::create_new(&self.db_config, page_no, self.version);
        leaf_page.add_tuple(&tuple);
        leaf_page
    }
}

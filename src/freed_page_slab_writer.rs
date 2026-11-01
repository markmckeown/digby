use crate::Compressor;
use crate::TupleProcessor;
use crate::compressor::CompressorType;
use crate::db_config::DbConfig;
use crate::free_page_manager::FreePageManager;
use crate::freed_page_log_tree_collector::FreedPageLogTreeCollector;
use crate::leaf_page::LeafPage;
use crate::page::PageTrait;
use crate::page::PageType;
use crate::page_cache::PageCache;
use crate::page_no::PageNo;
use crate::slab_ref::SlabRef;

pub struct FreedPageSlabWriter {
    db_config: DbConfig,
    version: u64,
    slab_ref: SlabRef,
}

impl FreedPageSlabWriter {
    pub fn new(db_config: DbConfig, version: u64, slab_ref: SlabRef) -> Self {
        FreedPageSlabWriter {
            db_config,
            version,
            slab_ref,
        }
    }

    pub fn can_write_freed_to_slab(&self) -> bool {
        self.slab_ref.has_free_space()
    }

    pub fn write_freed_to_slab(
        &mut self,
        page_cache: &mut PageCache,
        free_pg_mgr: &mut FreePageManager,
        freed_page_collector: &FreedPageLogTreeCollector,
    ) {
        assert!(self.slab_ref.has_free_space());
        let next_page_no = self.slab_ref.get_next_page_in_slab(PageType::LeafPage);
        self.write_freed_to_page(next_page_no, page_cache, free_pg_mgr, freed_page_collector);
    }

    pub fn write_freed_to_page(
        &self,
        page_no: PageNo,
        page_cache: &mut PageCache,
        free_pg_mgr: &mut FreePageManager,
        freed_page_collector: &FreedPageLogTreeCollector,
    ) {
        let mut leaf_page = self.write_to_leaf_page(
            page_no,
            page_cache,
            free_pg_mgr,
            freed_page_collector.get_pages_buffer(),
        );
        page_cache.put_page_no_cache(leaf_page.get_page());
    }

    fn write_to_leaf_page(
        &self,
        page_no: PageNo,
        page_cache: &mut PageCache,
        free_pg_mgr: &mut FreePageManager,
        freed_pages: Vec<u8>,
    ) -> LeafPage {
        let compressor = Compressor::new(CompressorType::None);
        // TODO - assuming 100 bytes here for book keeping.
        let max_value_size = self.db_config.block_size - 100;
        let tuple = TupleProcessor::generate_tuple_with_value_size_limit(
            &self.version.to_le_bytes(),
            &freed_pages[..],
            page_cache,
            free_pg_mgr,
            self.version,
            &compressor,
            max_value_size,
        );
        let mut leaf_page = LeafPage::create_new(&self.db_config, page_no, self.version);
        leaf_page.add_tuple(&tuple);
        leaf_page
    }
}

use crate::page::PageTrait;
use crate::{BaseFreePageTracker, DbMasterPage, FreePageTracker, PageCache, PageNo};
use std::collections::HashMap;

pub struct FreePageManager {
    base_free_pg_tracker: BaseFreePageTracker,
    free_pg_trackers: HashMap<u8, FreePageTracker>,
    og_free_pg_dir_pg_nos: Vec<PageNo>,
    version: u64,
    flushed: bool,
}

impl FreePageManager {
    pub fn new(db_master_page: &DbMasterPage, page_cache: &mut PageCache, version: u64) -> Self {
        let mut og_free_pg_dir_pg_nos: Vec<PageNo> = Vec::new();
        let base_free_pg_tracker_pg_no = db_master_page.get_free_page_dir_page_no(0);
        let base_free_pg_tracker =
            BaseFreePageTracker::new(page_cache.get_page(base_free_pg_tracker_pg_no), version);

        for i in 0..9 {
            og_free_pg_dir_pg_nos.push(db_master_page.get_free_page_dir_page_no(i));
        }

        FreePageManager {
            base_free_pg_tracker,
            free_pg_trackers: HashMap::new(),
            og_free_pg_dir_pg_nos,
            version,
            flushed: false,
        }
    }

    pub fn get_free_page(&mut self, page_cache: &mut PageCache, blk_cnt_shift: u8) -> PageNo {
        assert!(
            !self.flushed,
            "Cannot get free page after flushing free page trackers."
        );
        if blk_cnt_shift == 0 {
            return self.base_free_pg_tracker.get_free_page(page_cache);
        }
        if !self.free_pg_trackers.contains_key(&blk_cnt_shift) {
            self.create_page_tracker(page_cache, blk_cnt_shift);
        }
        self.free_pg_trackers
            .get_mut(&blk_cnt_shift)
            .unwrap()
            .get_free_page(page_cache, &mut self.base_free_pg_tracker)
    }

    pub fn return_free_page_no(&mut self, page_cache: &mut PageCache, page_no: PageNo) {
        assert!(
            !self.flushed,
            "Cannot return free page after flushing free page trackers."
        );
        let blk_cnt_shift = page_no.get_blk_cnt_shift();
        if blk_cnt_shift == 0 {
            self.base_free_pg_tracker.return_free_page_no(page_no);
            return;
        }

        self.get_free_page_tracker(page_cache, blk_cnt_shift)
            .return_free_page_no(page_no);
    }

    pub fn flush_free_page_trackers(
        &mut self,
        master_page: &mut DbMasterPage,
        page_cache: &mut PageCache,
    ) {
        assert!(
            !self.flushed,
            "Cannot flush free page trackers after they have already been flushed."
        );
        self.flushed = true;
        for (blk_cnt_shift, tracker) in &mut self.free_pg_trackers {
            let mut free_dir_pages =
                tracker.get_free_dir_pages(page_cache, &mut self.base_free_pg_tracker);
            assert!(!free_dir_pages.is_empty());
            let first_free_dir_page = free_dir_pages.last().unwrap().get_page_number();
            while let Some(mut free_dir_page) = free_dir_pages.pop() {
                page_cache.put_page(free_dir_page.get_page());
            }
            master_page.set_free_page_dir_page_no(*blk_cnt_shift, first_free_dir_page);
        }
        // Now flush the base free page tracker
        let mut free_dir_pages = self.base_free_pg_tracker.get_free_dir_pages(page_cache);
        assert!(!free_dir_pages.is_empty());
        let first_free_dir_page = free_dir_pages.last().unwrap().get_page_number();
        while let Some(mut free_dir_page) = free_dir_pages.pop() {
            page_cache.put_page(free_dir_page.get_page());
        }
        master_page.set_free_page_dir_page_no(0, first_free_dir_page);
    }

    fn get_free_page_tracker(
        &mut self,
        page_cache: &mut PageCache,
        blk_cnt_shift: u8,
    ) -> &mut FreePageTracker {
        assert!(blk_cnt_shift != 0);
        if !self.free_pg_trackers.contains_key(&blk_cnt_shift) {
            self.create_page_tracker(page_cache, blk_cnt_shift);
        }
        self.free_pg_trackers.get_mut(&blk_cnt_shift).unwrap()
    }

    fn create_page_tracker(
        &mut self,
        page_cache: &mut PageCache,
        blk_cnt_shift: u8,
    ) -> &mut FreePageTracker {
        assert!(blk_cnt_shift != 0);
        assert!(
            blk_cnt_shift < self.og_free_pg_dir_pg_nos.len() as u8,
            "Block count shift {} is out of bounds for the original free page directory page numbers.",
            blk_cnt_shift
        );
        let page_no = self
            .og_free_pg_dir_pg_nos
            .get(blk_cnt_shift as usize)
            .unwrap();
        let page = page_cache.get_page(*page_no);
        let free_pg_tracker = FreePageTracker::new(page, self.version, blk_cnt_shift);
        self.free_pg_trackers.insert(blk_cnt_shift, free_pg_tracker);
        self.free_pg_trackers.get_mut(&blk_cnt_shift).unwrap()
    }
}

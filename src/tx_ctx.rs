use crate::FreePageManager;
use crate::db_master_page::DbMasterPage;
use crate::page_cache::PageCache;
use crate::page_no::PageNo;

pub struct TxCtx {
    pub master_page: DbMasterPage,
    pub new_version: u64,
    pub global_root_page_no: PageNo,
    pub tree_dir_root_page_no: PageNo,
    pub free_pg_mgr: FreePageManager,
}

impl TxCtx {
    pub fn new(master_page: DbMasterPage, page_cache: &mut PageCache, new_version: u64) -> Self {
        let global_root_page_no = master_page.get_global_tree_root_page_no();
        let tree_dir_root_page_no = master_page.get_table_dir_page_no();
        let free_pg_mgr = FreePageManager::new(&master_page, page_cache, new_version);
        Self {
            master_page,
            new_version,
            global_root_page_no,
            tree_dir_root_page_no,
            free_pg_mgr,
        }
    }
}

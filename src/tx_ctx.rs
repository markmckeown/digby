use crate::FreePageManager;
use crate::db_config::DbConfig;
use crate::db_master_page::DbMasterPage;
use crate::free_page_tracker::FreePageTracker;
use crate::page_no::PageNo;

pub struct TxCtx {
    pub master_page: DbMasterPage,
    pub new_version: u64,
    pub free_page_tracker: FreePageTracker,
    pub global_root_page_no: PageNo,
    pub tree_dir_root_page_no: PageNo,
    pub free_pg_mgr: FreePageManager,
}

impl TxCtx {
    pub fn new(
        master_page: DbMasterPage,
        new_version: u64,
        free_page_tracker: FreePageTracker,
        db_config: DbConfig,
    ) -> Self {
        let global_root_page_no = master_page.get_global_tree_root_page_no();
        let tree_dir_root_page_no = master_page.get_table_dir_page_no();
        let free_pg_mgr = FreePageManager::new(&master_page, new_version, db_config);
        Self {
            master_page,
            new_version,
            free_page_tracker,
            global_root_page_no,
            tree_dir_root_page_no,
            free_pg_mgr,
        }
    }
}

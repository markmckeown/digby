use crate::db_master_page::DbMasterPage;
use crate::free_page_tracker::FreePageTracker;

pub struct DbWriter {
    pub master_page: DbMasterPage,
    pub new_version: u64,
    pub free_page_tracker: FreePageTracker,
    pub global_root_page_no: Option<u64>,
    pub tree_dir_root_page_no: Option<u64>
}

impl DbWriter {
    pub fn new(master_page: DbMasterPage, 
               new_version: u64, 
               free_page_tracker: FreePageTracker) -> Self {
        Self {
            master_page,
            new_version,
            free_page_tracker,
            global_root_page_no: None,
            tree_dir_root_page_no: None
        }
    }

}


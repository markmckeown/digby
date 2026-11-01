use crate::SlabRef;
use crate::db_config::DbConfig;
use crate::page::PageType;
use crate::page_no::PageNo;

pub struct Slab {
    slab_bytes: Vec<u8>,
    slab_ref: SlabRef,
}

impl Slab {
    pub fn create_new(page_meta: &DbConfig, page_no: PageNo) -> Self {
        assert!(page_no.get_pg_type() == PageType::Slab);
        let buffer_size = page_meta.block_size * page_no.get_blk_cnt() as usize;
        Slab {
            slab_bytes: vec![0u8; buffer_size],
            slab_ref: SlabRef::new(page_no),
        }
    }

    pub fn get_slab_page_number(&self) -> PageNo {
        self.slab_ref.get_slab_page_no()
    }

    pub fn get_slab_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.slab_bytes[..]
    }

    pub fn get_slab_bytes(&self) -> &[u8] {
        &self.slab_bytes
    }

    pub fn get_pages_used(&self) -> u16 {
        self.slab_ref.get_pages_used()
    }

    pub fn set_pages_used(&mut self, count: u16) {
        self.slab_ref.set_pages_used(count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Db;
    use crate::page::PageType;
    use crate::slab_sanity::LogSlabSanity;

    #[test]
    fn test_slab_creation() {
        let dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let file_path = dir.path().join("db");
        let db_path = file_path.to_str().unwrap();
        {
            let db_config = DbConfig::builder().block_size(4096).build();
            let mut db = Db::create(db_path, None, None, &db_config).unwrap();
            let mut txn = db.new_transaction();
            let mut slab_page_no = txn.free_pg_mgr.get_free_page(&mut db.page_cache, 4);
            slab_page_no.set_type(PageType::Slab);
            let log_slab_sanity = LogSlabSanity {};
            let _slab = db.page_cache.get_slab(slab_page_no, &log_slab_sanity);
            db.commit(txn);
        }
        {
            let mut _db = Db::open(db_path, None, None).unwrap();
        }
    }
}

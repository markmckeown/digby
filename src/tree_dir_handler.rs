use crate::dir_page::DirPage;
use crate::page::PageTrait;
use crate::page_cache::PageCache;
use crate::{FreePageTracker, TreeDirEntry, db_config};

pub struct TreeDirHandler {}

pub struct DirPageRef {
    pub page: DirPage,
    pub left_key: Option<Vec<u8>>,
}

impl TreeDirHandler {
    pub fn handle_tree_leaf_store(
        db_config: &db_config::DbConfig,
        mut dir_page: DirPage,
        entries: Vec<TreeDirEntry>,
    ) -> Vec<DirPageRef> {
        assert!(!entries.is_empty(), "entries was empty");
        let mut tree_dir_pages: Vec<DirPageRef> = Vec::new();

        if dir_page.store_child_pages(&entries) {
            tree_dir_pages.push(DirPageRef {
                page: dir_page,
                left_key: None,
            });
            return tree_dir_pages;
        }

        let (mut left_dir, mut right_dir, new_left_key) = dir_page.split_page(db_config, 0);

        if entries.first().unwrap().get_key() < new_left_key.as_slice() {
            // Use original page to add entries. Note if the first is less than the left key in the
            // new page then all entries will be.
            assert!(left_dir.store_child_pages(&entries));
        } else {
            // Use the original page.
            assert!(right_dir.store_child_pages(&entries));
        }
        tree_dir_pages.push(DirPageRef {
            page: left_dir,
            left_key: None,
        });
        tree_dir_pages.push(DirPageRef {
            page: right_dir,
            left_key: Some(new_left_key),
        });

        tree_dir_pages
    }

    pub fn map_dir_pages(
        page_refs: &mut Vec<DirPageRef>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        version: u64,
    ) {
        for page_ref in page_refs {
            let old_page_no = page_ref.page.get_page_number();
            if old_page_no.to_u64() != 0 {
                free_page_tracker.return_free_page_no(old_page_no);
            }
            let new_page_no = free_page_tracker.get_free_page(page_cache);
            page_ref.page.set_page_number(new_page_no);
            page_ref.page.set_version(version);
        }
    }

    pub fn handle_tree_dir_store(
        db_config: &db_config::DbConfig,
        mut parent_dir_page: DirPage,
        entries: Vec<TreeDirEntry>,
    ) -> Vec<DirPageRef> {
        assert!(!entries.is_empty(), "dir entries was empty");
        let mut tree_dir_pages: Vec<DirPageRef> = Vec::new();

        if parent_dir_page.store_child_pages(&entries) {
            tree_dir_pages.push(DirPageRef {
                page: parent_dir_page,
                left_key: None,
            });
            return tree_dir_pages;
        }

        // Need to split the parent dir page.
        let (mut left_dir, mut right_dir, new_left_key) = parent_dir_page.split_page(db_config, 0);

        if entries.first().unwrap().get_key() < new_left_key.as_slice() {
            // Add entries to original page.
            assert!(left_dir.store_child_pages(&entries));
        } else {
            assert!(right_dir.store_child_pages(&entries));
        }
        tree_dir_pages.push(DirPageRef {
            page: left_dir,
            left_key: None,
        });
        tree_dir_pages.push(DirPageRef {
            page: right_dir,
            left_key: Some(new_left_key),
        });

        tree_dir_pages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LeafPage;
    use crate::Tuple;
    use crate::db_config::DbConfig;
    use crate::page_no::PageNo;

    #[test]
    fn test_add_1() {
        let page_config = DbConfig::builder()
            .block_size(4096)
            .page_size(4092)
            .block_sanity_size(4)
            .compressor_type(crate::compressor::CompressorType::None)
            .leaf_page_blk_exp(0)
            .dir_page_blk_exp(0)
            .build();
        let mut tree_leaf_page = LeafPage::create_new(&page_config, PageNo::from_u64(0), 0);
        tree_leaf_page.set_page_number(PageNo::from_u64(21));
        let tuple: Tuple = Tuple::new(b"f".to_vec().as_ref(), b"f_value".to_vec().as_ref(), 345);
        tree_leaf_page.add_tuple(&tuple);

        let mut tree_leaf_page1 = LeafPage::create_new(&page_config, PageNo::from_u64(0), 0);
        tree_leaf_page1.set_page_number(PageNo::from_u64(27));
        let tuple1: Tuple = Tuple::new(b"h".to_vec().as_ref(), b"h_value".to_vec().as_ref(), 345);
        tree_leaf_page1.add_tuple(&tuple1);

        let mut leaf_pages: Vec<LeafPage> = vec![tree_leaf_page, tree_leaf_page1];

        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for leaf_page in leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(
                leaf_page.get_left_key().unwrap(),
                leaf_page.get_page_number().to_u64(),
            );
            entries.push(tree_dir_entry);
        }

        let mut tree_dir_page = DirPage::create_new(&page_config, PageNo::from_u64(0), 0);

        let mut new_pages =
            TreeDirHandler::handle_tree_leaf_store(&page_config, tree_dir_page, entries);
        assert_eq!(new_pages.len(), 1);
        tree_dir_page = new_pages.pop().unwrap().page;
        assert_eq!(tree_dir_page.get_page_to_left(), PageNo::from_u64(21));
        assert_eq!(tree_dir_page.get_dir_left_key().unwrap(), b"h".to_vec());

        let tuple3: Tuple = Tuple::new(b"a".to_vec().as_ref(), b"a_value".to_vec().as_ref(), 345);
        tree_leaf_page = LeafPage::create_new(&page_config, PageNo::from_u64(0), 0);
        tree_leaf_page.add_tuple(&tuple3);
        tree_leaf_page.set_page_number(PageNo::from_u64(79));
        leaf_pages = vec![tree_leaf_page];

        entries = Vec::new();
        for leaf_page in leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(
                leaf_page.get_left_key().unwrap(),
                leaf_page.get_page_number().to_u64(),
            );
            entries.push(tree_dir_entry);
        }

        new_pages = TreeDirHandler::handle_tree_leaf_store(&page_config, tree_dir_page, entries);
        assert_eq!(new_pages.len(), 1);
        tree_dir_page = new_pages.pop().unwrap().page;
        assert_eq!(tree_dir_page.get_page_to_left(), PageNo::from_u64(79));
    }
}

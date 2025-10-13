use crate::free_page_tracker::FreePageTracker;
use crate::page_cache::PageCache; 
use crate::tree_dir_handler::TreeDirHandler;
use crate::page::{Page, PageTrait, PageType};
use crate::tuple::Tuple;
use crate::tree_leaf_page::TreeLeafPage;
use crate::leaf_page_handler::LeafPageHandler;
use crate::tree_dir_page::TreeDirPage;
use crate::tree_dir_entry::TreeDirEntry;

pub struct StoreTupleProcessor {

}

impl StoreTupleProcessor{
    pub fn store_tuple(
        tuple: Tuple,
        first: Page,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
        page_size: usize) -> u32 {
        
        if first.get_type() == PageType::TreeLeaf {
            let tree_root_single = TreeLeafPage::from_page(first);
            return StoreTupleProcessor::store_tuple_tree_root_single(tuple, tree_root_single, free_page_tracker, 
                page_cache, new_version, page_size);
        }

       return 0;
    }

    fn store_tuple_tree_root_single(
        tuple: Tuple,
        tree_root_single: TreeLeafPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
        page_size: usize) -> u32 {

        let mut leaf_pages = LeafPageHandler::add_tuple(tree_root_single, tuple, page_size);
        LeafPageHandler::map_pages(&mut leaf_pages, free_page_tracker, page_cache, new_version);
        if leaf_pages.len() == 1 {
            let page_number = leaf_pages.get(0).unwrap().get_page_number();
            page_cache.put_page(leaf_pages.pop().unwrap().get_page());
            return page_number;
        }

        // The root leaf page has split.
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for mut leaf_page in  leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(leaf_page.get_left_key(page_size).unwrap(), leaf_page.get_page_number());
            entries.push(tree_dir_entry);
            page_cache.put_page(leaf_page.get_page());
        }
        let new_tree_dir_page = TreeDirPage::new(page_size as u64, 0, 0);
        let mut dir_pages = TreeDirHandler::handle_tree_leaf_store(new_tree_dir_page, entries, page_size);
        assert!(dir_pages.len() == 1);
        TreeDirHandler::map_pages(&mut dir_pages, free_page_tracker, page_cache, new_version);
        let page_number = dir_pages.get(0).unwrap().page.get_page_number();
        page_cache.put_page(&mut dir_pages.pop().unwrap().page.get_page());
        return page_number;
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_is_leaf() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let version = 23;
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_file).expect("Failed to open or create DB file");
        
        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, crate::Db::PAGE_SIZE);
        let block_layer: crate::BlockLayer = crate::BlockLayer::new(file_layer, crate::Db::PAGE_SIZE);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer, crate::Db::PAGE_SIZE);

        let free_dir_page_no = *page_cache.create_new_pages(1).get(0).unwrap();
        let mut free_dir_page = crate::FreeDirPage::new(crate::Db::PAGE_SIZE, free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());
        
        let root_tree_page_no = *page_cache.create_new_pages(1).get(0).unwrap();
        let mut leaf_page = TreeLeafPage::new(crate::Db::PAGE_SIZE, root_tree_page_no);
        leaf_page.set_version(version);
        page_cache.put_page(leaf_page.get_page());

        let mut free_page_tracker = FreePageTracker::new(
            page_cache.get_page(free_dir_page_no), version + 1, crate::Db::PAGE_SIZE as usize);

        let reloaded_page = page_cache.get_page(root_tree_page_no);

        let tuple = Tuple::new(b"key_1".to_vec(), b"value_1".to_vec(), version + 1);
        let new_root_tree_no = StoreTupleProcessor::store_tuple(tuple, reloaded_page, &mut free_page_tracker, 
            &mut page_cache, version + 1, crate::Db::PAGE_SIZE as usize);
        assert_eq!(new_root_tree_no, 2);
        
        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }
}
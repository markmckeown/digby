use crate::page::PageTrait;
use crate::tree_leaf_page::TreeLeafPage;
use crate::tuple::{Tuple, TupleTrait};
use crate::free_page_tracker::FreePageTracker;
use crate::page_cache::PageCache;

pub struct LeafPageHandler {
}

impl LeafPageHandler {
    // This happens after overflow handling, so we know the tuple will fit in a page.
    pub fn add_tuple(page: TreeLeafPage, 
                    tuple: Tuple,
                    free_page_tracker: &mut FreePageTracker,
                    page_cache: &mut PageCache,
                    version: u64, 
                    page_size: usize) -> Vec<TreeLeafPage> {
        let mut pages: Vec<TreeLeafPage> = Vec::new();
        pages.push(page);
        LeafPageHandler::add_to_page(tuple, &mut pages, page_size);
        LeafPageHandler::map_pages(&mut pages, free_page_tracker, page_cache, version);
        pages
    }

    fn map_pages(pages: &mut Vec<TreeLeafPage>, 
                    free_page_tracker: &mut FreePageTracker, 
                    page_cache: &mut PageCache, 
                    version: u64) -> () {
        for page in pages {
            let old_page_no = page.get_page_number();
            if old_page_no != 0 {
                free_page_tracker.return_free_page_no(old_page_no);
            }
            let new_page_no = free_page_tracker.get_free_page(page_cache);
            page.set_page_number(new_page_no);
            page.set_version(version);
        }
    }

    
    fn add_to_page(tuple: Tuple, 
                new_pages: &mut Vec<TreeLeafPage>,
                page_size: usize) -> () {
        assert!(!new_pages.is_empty());
        let page = new_pages.last_mut().unwrap();
        // Tuple can fit into page, no split needed.
        if page.can_fit(tuple.get_byte_size()) {
            page.store_tuple(tuple, page_size);
            return;
        }

        // Split the page and get the entries to the right
        let mut tuples_to_right = page.get_right_half_tuples(page_size);
        // Create a new page to hold the entries to the right
        let mut new_page = TreeLeafPage::new(page_size as u64, 0);
        if tuples_to_right.is_empty() {
            // Edge case, the page cannot be split as it has only one entry. The new page 
            // is empty so add tuple to it. We can assume it can fit into a page.
            assert!(new_page.can_fit(tuple.get_byte_size()));
            new_page.store_tuple(tuple, page_size);
            new_pages.push(new_page);
            return;
        }

        // We grab the left most key of the entries removed from the first page and
        // add the entries to the page.
        let left_key_for_new_page = &tuples_to_right.get(0).unwrap().get_key().to_vec()[..];
        new_page.add_sorted_tuples(&mut tuples_to_right, page_size);

        // Tuple is to the left of the split entries so try and add to the original page.
        if tuple.get_key() < left_key_for_new_page {
            if page.can_fit(tuple.get_byte_size()) {
                // Tuple fits into the original page now!
                page.store_tuple(tuple, page_size);
                new_pages.push(new_page);
                return;
            } else {
                // Tuple does not fit into the old page, need to split again,
                // Put the new page to the start of the new_pages - the old
                // page is still the last entry adn will be split again when
                // recursively called.
                new_pages.insert(0, new_page);
                return LeafPageHandler::add_to_page(tuple, new_pages, page_size);  
            }
        }

        // The tuple should go into the new page if it can fit.
        if new_page.can_fit(tuple.get_byte_size()) {
            new_page.store_tuple(tuple, page_size);
            new_pages.push(new_page);
            return;
        }
        // Tuple cannot fit into new page, new page is the last in the list
        // and will be split again when function is recursively called.
        new_pages.push(new_page);
        return LeafPageHandler::add_to_page(tuple, new_pages, page_size);
    }
}


#[cfg(test)]
mod tests {
    use crate::free_dir_page::FreeDirPage;

    use super::*;

    #[test]
    fn test_small_tuple_split_page() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_file).expect("Failed to open or create DB file");

        let version = 0;
        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, crate::Db::PAGE_SIZE);
        let block_layer: crate::BlockLayer = crate::BlockLayer::new(file_layer, crate::Db::PAGE_SIZE);
        let mut page_cache: PageCache = PageCache::new(block_layer, crate::Db::PAGE_SIZE);

        let mut free_dir_page_no = *page_cache.create_new_pages(1).get(0).unwrap();
        let mut tree_leaf_page_no = *page_cache.create_new_pages(1).get(0).unwrap();
        
        let mut free_dir_page = FreeDirPage::new(crate::Db::PAGE_SIZE, free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let mut first_tree_leaf_page: TreeLeafPage = TreeLeafPage::new(crate::Db::PAGE_SIZE, tree_leaf_page_no);
        first_tree_leaf_page.set_version(version+1);
        page_cache.put_page(first_tree_leaf_page.get_page());

        
        let mut new_version = version;
        let mut tree_leaf_page: TreeLeafPage;
        let mut tuple: Tuple;
        let mut j: u32 = 0;
        // Each loop is a new commit.
        for i in 1u32..1024 {
            j = i;
            new_version = new_version + 1;
            tuple = Tuple::new(i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec(), new_version);
            tree_leaf_page = TreeLeafPage::from_page(page_cache.get_page(tree_leaf_page_no));
            let mut free_page_tracker = FreePageTracker::new( 
                 page_cache.get_page(free_dir_page_no), new_version, crate::Db::PAGE_SIZE as usize);

            // Fill page, but don't split.
            if !tree_leaf_page.can_fit(tuple.get_byte_size()) {
                break;
            }     
            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple, &mut free_page_tracker,
                &mut page_cache, new_version, crate::Db::PAGE_SIZE as usize);
            assert!(pages.len() == 1);
            tree_leaf_page = pages.pop().unwrap();
            tree_leaf_page_no = tree_leaf_page.get_page_number();
            page_cache.put_page(tree_leaf_page.get_page());
            let mut free_dir_pages = free_page_tracker.get_free_dir_pages(&mut page_cache);  
            assert!(free_dir_pages.len() == 1);  
            free_dir_page = free_dir_pages.pop().unwrap();
            free_dir_page_no = free_dir_page.get_page_number();
            page_cache.put_page(free_dir_page.get_page());
        }

        // We should be using 18 pages - the two created at the start and the free_pages allocated
        // by the free page allocator
        assert_eq!(page_cache.get_total_page_count(), 18);
        // There should be 16 free pages of the 18, 2 used to store page_dir and the tree_leaf
        assert_eq!(free_dir_page.get_entries(), 16);


        // Original page is full now - next input will require a split
        tuple = Tuple::new(j.to_le_bytes().to_vec(), j.to_le_bytes().to_vec(), new_version);
        let mut free_page_tracker = FreePageTracker::new( 
                 page_cache.get_page(free_dir_page_no), new_version, crate::Db::PAGE_SIZE as usize);
        tree_leaf_page = TreeLeafPage::from_page(page_cache.get_page(tree_leaf_page_no));
        let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple, &mut free_page_tracker,
                &mut page_cache, new_version, crate::Db::PAGE_SIZE as usize);
        assert!(pages.len() == 2);
        let tree_leaf_page1 = pages.pop().unwrap();
        assert_eq!(tree_leaf_page1.get_all_tuples(crate::Db::PAGE_SIZE as usize).len(), 93);
        let tree_leaf_page2 = pages.pop().unwrap();
        assert_eq!(tree_leaf_page2.get_all_tuples(crate::Db::PAGE_SIZE as usize).len(), 93);
            
        let mut free_dir_pages = free_page_tracker.get_free_dir_pages(&mut page_cache);  
        assert!(free_dir_pages.len() == 1);  
        free_dir_page = free_dir_pages.pop().unwrap();

        // Used an extra free page.
        assert_eq!(free_dir_page.get_entries(), 15);

        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

     #[test]
    fn test_add_big_pages() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_file).expect("Failed to open or create DB file");

        let version = 0;
        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, crate::Db::PAGE_SIZE);
        let block_layer: crate::BlockLayer = crate::BlockLayer::new(file_layer, crate::Db::PAGE_SIZE);
        let mut page_cache: PageCache = PageCache::new(block_layer, crate::Db::PAGE_SIZE);

        let mut free_dir_page_no = *page_cache.create_new_pages(1).get(0).unwrap();
        let mut tree_leaf_page_no = *page_cache.create_new_pages(1).get(0).unwrap();
        
        let mut free_dir_page = FreeDirPage::new(crate::Db::PAGE_SIZE, free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let mut first_tree_leaf_page: TreeLeafPage = TreeLeafPage::new(crate::Db::PAGE_SIZE, tree_leaf_page_no);
        first_tree_leaf_page.set_version(version+1);
        page_cache.put_page(first_tree_leaf_page.get_page());

        
        let mut new_version = version;
        let mut tree_leaf_page: TreeLeafPage;
        let mut tuple: Tuple;
        

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..2 {
            new_version = new_version + 1;
            let value = vec![0u8; 2048];
            tuple = Tuple::new(i.to_le_bytes().to_vec(), value, new_version);
            tree_leaf_page = TreeLeafPage::from_page(page_cache.get_page(tree_leaf_page_no));
            let mut free_page_tracker = FreePageTracker::new( 
                 page_cache.get_page(free_dir_page_no), new_version, crate::Db::PAGE_SIZE as usize);

            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple, &mut free_page_tracker,
                &mut page_cache, new_version, crate::Db::PAGE_SIZE as usize);
            tree_leaf_page_count = 0;    
            while let Some(mut tree_leaf_page) = pages.pop() {
                tree_leaf_page_count = tree_leaf_page_count + 1;
                tree_leaf_page_no = tree_leaf_page.get_page_number();
                page_cache.put_page(tree_leaf_page.get_page());
            }
            let mut free_dir_pages = free_page_tracker.get_free_dir_pages(&mut page_cache);  
            assert!(free_dir_pages.len() == 1);  
            free_dir_page = free_dir_pages.pop().unwrap();
            free_dir_page_no = free_dir_page.get_page_number();
            page_cache.put_page(free_dir_page.get_page());
        }
        assert_eq!(tree_leaf_page_count, 2);

        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

}

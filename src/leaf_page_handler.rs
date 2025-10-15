use crate::page::PageTrait;
use crate::tree_leaf_page::TreeLeafPage;
use crate::tuple::{Tuple, TupleTrait};
use crate::free_page_tracker::FreePageTracker;
use crate::page_cache::PageCache;

pub struct LeafPageHandler {
}

pub struct UpdateResult {
    pub tree_leaf_pages: Vec<TreeLeafPage>,
    pub deleted_tuple: Option<Tuple>,
}

impl LeafPageHandler {
    // This happens after overflow handling, so we know the tuple will fit in a page.
    pub fn add_tuple(page: TreeLeafPage, 
                    tuple: Tuple,
                    page_size: usize) -> UpdateResult {
        let mut pages: Vec<TreeLeafPage> = Vec::new();
        pages.push(page);
        let deleted_tuple = LeafPageHandler::add_to_page(tuple, &mut pages, page_size);
        UpdateResult {
           tree_leaf_pages: pages,
           deleted_tuple: deleted_tuple,
        }
    }

    pub fn map_pages(pages: &mut Vec<TreeLeafPage>, 
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
                page_size: usize) -> Option<Tuple> {
        assert!(!new_pages.is_empty());
        let page = new_pages.last_mut().unwrap();
        // Need to get the existing tuple in case there are overflow pages to clean up.
        let existing_tuple = page.get_tuple(tuple.get_key().to_vec().as_ref(), page_size);
        
        // Tuple can fit into page, no split needed. If the key is already in the page
        // then it would be delete in the store_tuple
        if page.can_fit(tuple.get_byte_size()) {
            page.store_tuple(tuple, page_size);
            return existing_tuple;
        }

        // Cannot fit, but we may be replacing the key so try deleting it.
        if page.delete_key(&tuple.get_key().to_vec(), page_size) {
            // key was deleted. now check it fits.
            if page.can_fit(tuple.get_byte_size()) {
                page.store_tuple(tuple, page_size);
                return existing_tuple;
            } 
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
            return existing_tuple;
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
                return existing_tuple;
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
            return existing_tuple;
        }
        // Tuple cannot fit into new page, new page is the last in the list
        // and will be split again when function is recursively called.
        new_pages.push(new_page);
        return LeafPageHandler::add_to_page(tuple, new_pages, page_size);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_tuple_split_page() {
    
        let version = 0;
        
        let mut tree_leaf_page: TreeLeafPage = TreeLeafPage::new(crate::Db::PAGE_SIZE, 56);
        tree_leaf_page.set_version(version+1);
        let mut new_version = version;
        let mut tuple: Tuple;
        let mut j: u32 = 0;
        // Each loop is a new commit.
        for i in 1u32..1024 {
            j = i;
            new_version = new_version + 1;
            tuple = Tuple::new(i.to_le_bytes().to_vec().as_ref(), i.to_le_bytes().to_vec().as_ref(), new_version);

            // Fill page, but don't split.
            if !tree_leaf_page.can_fit(tuple.get_byte_size()) {
                break;
            }     
            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple, crate::Db::PAGE_SIZE as usize);
            assert!(pages.tree_leaf_pages.len() == 1);
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap();
        }


        // Original page is full now - next input will require a split
        tuple = Tuple::new(j.to_le_bytes().to_vec().as_ref(), j.to_le_bytes().to_vec().as_ref(), new_version);
        let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple, crate::Db::PAGE_SIZE as usize);
        assert!(pages.tree_leaf_pages.len() == 2);
        let tree_leaf_page1 = pages.tree_leaf_pages.pop().unwrap();
        assert_eq!(tree_leaf_page1.get_all_tuples(crate::Db::PAGE_SIZE as usize).len(), 93);
        let tree_leaf_page2 = pages.tree_leaf_pages.pop().unwrap();
        assert_eq!(tree_leaf_page2.get_all_tuples(crate::Db::PAGE_SIZE as usize).len(), 93);
    }

    // In this test we overwrite the same key twice.
     #[test]
    fn test_over_write_big_tuples() {
        let mut tree_leaf_page: TreeLeafPage = TreeLeafPage::new(crate::Db::PAGE_SIZE, 0);
        tree_leaf_page.set_version(1);
        
        let mut new_version = 2;
        let mut tuple: Tuple;
        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for _i in 0u32..2 {
            new_version = new_version + 1;
            let value = vec![0u8; 2048];
            // Same key used
            tuple = Tuple::new(1u32.to_le_bytes().to_vec().as_ref(), value.as_ref(), new_version);

            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple,  crate::Db::PAGE_SIZE as usize);
            tree_leaf_page_count = pages.tree_leaf_pages.len();
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap();
        }
        assert_eq!(tree_leaf_page_count, 1);
    }


     #[test]
    fn test_add_big_tuples() {
        let version = 0;
        let mut tree_leaf_page: TreeLeafPage = TreeLeafPage::new(crate::Db::PAGE_SIZE, 0);
        tree_leaf_page.set_version(version+1);
        let mut new_version = version;
        let mut tuple: Tuple;
        

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..2 {
            new_version = new_version + 1;
            let value = vec![0u8; 2048];
            tuple = Tuple::new(i.to_le_bytes().to_vec().as_ref(), value.as_ref(), new_version);
            
            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple,  crate::Db::PAGE_SIZE as usize);
            tree_leaf_page_count = pages.tree_leaf_pages.len();    
            tree_leaf_page  = pages.tree_leaf_pages.pop().unwrap();
        }
        assert_eq!(tree_leaf_page_count, 2);
    }



    #[test]
    fn test_add_small_large_large() {
        let version = 0;
        let mut tree_leaf_page: TreeLeafPage = TreeLeafPage::new(crate::Db::PAGE_SIZE, 0);
        tree_leaf_page.set_version(version+1);
    

        let mut new_version = version;
        let mut tuple: Tuple;
        
        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version = new_version + 1;
            let value: Vec<u8>;
            if i == 0 {
                value = vec![0u8; 8];
            } else {
                value = vec![0u8; 2048];
            }
            // Use the same key
            tuple = Tuple::new(i.to_le_bytes().to_vec().as_ref(), value.as_ref(), new_version);
            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple, crate::Db::PAGE_SIZE as usize);
            tree_leaf_page_count = pages.tree_leaf_pages.len();    
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap();
        }
        assert_eq!(tree_leaf_page_count, 3);
    }


    #[test]
    fn test_add_large_small_large() {
        let version = 0;
        let mut tree_leaf_page: TreeLeafPage = TreeLeafPage::new(crate::Db::PAGE_SIZE, 0);
        let mut new_version = version;
        let mut tuple: Tuple;
        
        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version = new_version + 1;
            let value: Vec<u8>;
            if i == 1 {
                value = vec![0u8; 8];
            } else {
                value = vec![0u8; 2048];
            }
            // Use the same key
            tuple = Tuple::new(i.to_le_bytes().to_vec().as_ref(), value.as_ref(), new_version);
            

            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple,  crate::Db::PAGE_SIZE as usize);
            tree_leaf_page_count = pages.tree_leaf_pages.len();    
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap();
        }
        assert_eq!(tree_leaf_page_count, 2);
    }


     #[test]
    fn test_add_small_large_large_reverse() {
        let version = 0;
        let mut tree_leaf_page: TreeLeafPage = TreeLeafPage::new(crate::Db::PAGE_SIZE, 0);
        tree_leaf_page.set_version(version+1);
        
        let mut new_version = version;
        let mut tuple: Tuple;
        
        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version = new_version + 1;
            let j = 3 - i;
            let value: Vec<u8>;
            if j == 3 {
                value = vec![0u8; 8];
            } else {
                value = vec![0u8; 2048];
            }
            // Use the same key
            tuple = Tuple::new(j.to_le_bytes().to_vec().as_ref(), value.as_ref(), new_version);
            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple,  crate::Db::PAGE_SIZE as usize);
            tree_leaf_page_count = pages.tree_leaf_pages.len();    
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap();
            
        }
        assert_eq!(tree_leaf_page_count, 3);
    }


}
